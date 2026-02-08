"""
InfluxDB 1.x reader: OHLCV from stock_data; one canonical symbol per query, optional LRU cache.

Used for: DMS read path (single and batch); symbols normalized via core.symbol before query.

Classes:
    LRUCache        TTL LRU cache (key -> value; evict by access order and expiry).
    InfluxDBReader  Reader for InfluxDB 1.x (measurement stock_data; tags symbol, interval).

LRUCache methods:
    .get(key: tuple) -> Optional[DataFrame]
    .set(key: tuple, value: pd.DataFrame) -> None

InfluxDBReader methods:
    .connect() -> bool
    .disconnect() -> None
    .read_history(symbol: str, start_date: datetime, end_date: datetime, interval: str = "1d") -> Optional[DataFrame]
    .read_batch(symbols: list, start_date: datetime, end_date: datetime, interval: str = "1d") -> Dict[str, Optional[DataFrame]]

InfluxDBReader features:
    - Normalize symbol once before query (no fallback; aligns with writer).
    - Optional LRU cache (cache_size, cache_ttl) for read_history and read_batch.
    - read_batch: one InfluxDB query for all symbols, then split by symbol.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict
from functools import lru_cache
import pandas as pd

try:
    from influxdb import InfluxDBClient
    HAS_INFLUXDB = True
except ImportError:
    HAS_INFLUXDB = False

from .base import Reader

from ...core.symbol import normalize_symbol

logger = logging.getLogger(__name__)


class LRUCache:
    """TTL LRU cache; evicts by access order and expiry."""

    def __init__(self, maxsize: int = 1000, ttl: int = 3600):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[tuple, tuple] = {}  # (key) -> (value, timestamp)
        self._access_order: list = []
    
    def get(self, key: tuple) -> Optional[pd.DataFrame]:
        """Return cached value or None if missing or expired."""
        if key not in self._cache:
            return None
        
        value, timestamp = self._cache[key]
        
        # Check TTL
        if time.time() - timestamp > self.ttl:
            del self._cache[key]
            if key in self._access_order:
                self._access_order.remove(key)
            return None
        
        # Update access order
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
        
        return value
    
    def set(self, key: tuple, value: pd.DataFrame) -> None:
        """Store value; evict oldest if at maxsize."""
        # Remove oldest if cache is full
        if len(self._cache) >= self.maxsize and self._access_order:
            oldest_key = self._access_order.pop(0)
            del self._cache[oldest_key]
        
        self._cache[key] = (value, time.time())
        if key not in self._access_order:
            self._access_order.append(key)


class InfluxDBReader(Reader):
    """InfluxDB 1.x reader; normalizes symbols and optionally caches results."""

    def __init__(self, config: Optional[dict] = None):
        """Init from config (host, port, database, username, password; cache_enabled, cache_size, cache_ttl)."""
        if not HAS_INFLUXDB:
            raise ImportError("Please install influxdb: pip install influxdb")
        
        super().__init__(config)
        
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 8086)
        self.database = self.config.get("database", "stock_data")
        self.username = self.config.get("username", "")
        self.password = self.config.get("password", "")
        
        # Cache configuration
        self.cache_enabled = self.config.get("cache_enabled", True)
        cache_size = self.config.get("cache_size", 1000)
        cache_ttl = self.config.get("cache_ttl", 3600)
        self._cache = LRUCache(maxsize=cache_size, ttl=cache_ttl) if self.cache_enabled else None
        
        self.client: Optional[InfluxDBClient] = None
    
    def connect(self) -> bool:
        """Connect to InfluxDB and create database if not exists."""
        try:
            # Connect without database first (for faster startup)
            self.client = InfluxDBClient(
                host=self.host,
                port=self.port,
                username=self.username if self.username else None,
                password=self.password if self.password else None,
                timeout=5,  # 5 seconds timeout for faster connection
            )
            # Test connection with ping (quick check)
            self.client.ping()
            
            # Create database if not exists
            try:
                databases = self.client.get_list_database()
                db_names = [db['name'] for db in databases]
                if self.database not in db_names:
                    self.client.create_database(self.database)
                    logger.info(f"Created database: {self.database}")
                else:
                    logger.debug(f"Database already exists: {self.database}")
            except Exception as e:
                # If get_list_database fails, try to create anyway
                logger.warning(f"Could not check database existence, attempting to create: {e}")
                try:
                    self.client.create_database(self.database)
                    logger.info(f"Created database: {self.database}")
                except Exception as create_e:
                    # Database might already exist, continue
                    logger.debug(f"Database creation attempt: {create_e}")
            
            # Switch to the database
            self.client.switch_database(self.database)
            self._connected = True
            logger.info(f"Connected to InfluxDB: {self.host}:{self.port}, database: {self.database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            self._connected = False
            return False
    
    def disconnect(self) -> None:
        """Disconnect from InfluxDB."""
        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
        self.client = None
        self._connected = False
        logger.info("Disconnected from InfluxDB")
    
    def read_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> Optional[pd.DataFrame]:
        """Read OHLCV for one symbol; symbol normalized once, single query; optional cache."""
        if not self._connected or not self.client:
            logger.error("Not connected to InfluxDB")
            return None
        
        symbol = normalize_symbol(symbol)
        # Check cache
        cache_key = (symbol, start_date, end_date, interval)
        if self.cache_enabled and self._cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {symbol}")
                return cached.copy()
        
        def _try_read_symbol(symbol_to_try: str) -> Optional[pd.DataFrame]:
            """Run one query for the given symbol; return DataFrame or None."""
            try:
                # Sanitize inputs to prevent injection (InfluxDB uses single quotes)
                safe_symbol = symbol_to_try.replace("'", "''").replace("\\", "\\\\")
                safe_interval = interval.replace("'", "''").replace("\\", "\\\\")
                
                # Build query with sanitized inputs.
                # Use end_exclusive = end_date's calendar day + 1 so that the full end_date day
                # is included (time < end excludes the exact end timestamp; daily bars are at 00:00).
                start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_date_only = end_date.date() if hasattr(end_date, "date") else end_date
                end_exclusive = (end_date_only + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                query = f"""
                SELECT open, high, low, close, volume
                FROM stock_data
                WHERE symbol = '{safe_symbol}'
                AND interval = '{safe_interval}'
                AND time >= '{start_str}'
                AND time < '{end_exclusive}'
                ORDER BY time
                """
                
                result = self.client.query(query)
                
                if not result:
                    return None
                
                # Convert to DataFrame
                points = list(result.get_points())
                if not points:
                    return None
                
                data = []
                for point in points:
                    data.append({
                        "Open": point["open"],
                        "High": point["high"],
                        "Low": point["low"],
                        "Close": point["close"],
                        "Volume": point["volume"],
                    })
                
                df = pd.DataFrame(data)
                df.index = pd.to_datetime([p["time"] for p in points])
                
                logger.debug(f"Read {len(df)} records for {symbol_to_try}")
                return df
            except Exception as e:
                logger.debug(f"Failed to read {symbol_to_try}: {e}")
                return None
        
        try:
            df = _try_read_symbol(symbol)
            if df is None:
                return None
            # Store in cache
            if self.cache_enabled and self._cache:
                self._cache.set(cache_key, df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading data for {symbol}: {e}", exc_info=True)
            return None

    def read_batch(
        self,
        symbols: list,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> Dict[str, pd.DataFrame]:
        """Batch read: one InfluxDB query for all symbols, then split by normalized symbol."""
        if not self._connected or not self.client or not symbols:
            return {}
        req_to_norm = {s: normalize_symbol(s) for s in symbols}
        unique_norm = set(req_to_norm.values())
        symbol_conds = []
        for sym in unique_norm:
            safe = sym.replace("'", "''").replace("\\", "\\\\")
            symbol_conds.append(f"symbol = '{safe}'")
        where_symbols = " OR ".join(symbol_conds)
        safe_interval = interval.replace("'", "''").replace("\\", "\\\\")
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_only = end_date.date() if hasattr(end_date, "date") else end_date
        end_exclusive = (end_date_only + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        query = f"""
        SELECT symbol, open, high, low, close, volume
        FROM stock_data
        WHERE ({where_symbols})
        AND interval = '{safe_interval}'
        AND time >= '{start_str}'
        AND time < '{end_exclusive}'
        ORDER BY time
        """
        try:
            result = self.client.query(query)
            if not result:
                return {sym: None for sym in symbols}
            points = list(result.get_points())
            if not points:
                return {sym: None for sym in symbols}
            by_normalized: Dict[str, list] = {sym: [] for sym in unique_norm}
            for p in points:
                db_sym = p.get("symbol")
                if db_sym in by_normalized:
                    by_normalized[db_sym].append(p)
            out: Dict[str, pd.DataFrame] = {}
            for req_sym in symbols:
                norm = req_to_norm[req_sym]
                plist = by_normalized.get(norm) or []
                if not plist:
                    out[req_sym] = None
                    continue
                data = [
                    {
                        "Open": p["open"],
                        "High": p["high"],
                        "Low": p["low"],
                        "Close": p["close"],
                        "Volume": p["volume"],
                    }
                    for p in plist
                ]
                df = pd.DataFrame(data)
                df.index = pd.to_datetime([p["time"] for p in plist])
                if self.cache_enabled and self._cache:
                    cache_key = (req_sym, start_date, end_date, interval)
                    self._cache.set(cache_key, df)
                out[req_sym] = df
            logger.debug("read_batch: one query returned %d points for %d symbols", len(points), len(symbols))
            return out
        except Exception as e:
            logger.error("Error in read_batch: %s", e, exc_info=True)
            return {sym: None for sym in symbols}
