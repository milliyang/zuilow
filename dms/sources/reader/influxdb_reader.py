"""
InfluxDB 1.x Reader

Read OHLCV from InfluxDB 1.x; optional LRU cache (TTL), symbol fallback (US. prefix / no prefix).

Classes:
    LRUCache        Simple LRU cache with TTL (key -> value, evict by access order and expiry)
    InfluxDBReader  Reader implementation for InfluxDB 1.x

InfluxDBReader:
    Config: host, port, database, username, password; cache_enabled, cache_size, cache_ttl.
    .connect() -> bool                                  Connect and create DB if not exists
    .disconnect()                                       Close connection
    .read_history(symbol, start_date, end_date, interval) -> Optional[DataFrame]  Query stock_data measurement

Measurement/tags: stock_data, symbol, interval; fields: open, high, low, close, volume.
Dependencies: influxdb (InfluxDBClient).
"""

import logging
import time
from datetime import datetime
from typing import Optional, Dict
from functools import lru_cache
import pandas as pd

try:
    from influxdb import InfluxDBClient
    HAS_INFLUXDB = True
except ImportError:
    HAS_INFLUXDB = False

from .base import Reader

logger = logging.getLogger(__name__)


class LRUCache:
    """Simple LRU cache with TTL"""
    
    def __init__(self, maxsize: int = 1000, ttl: int = 3600):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[tuple, tuple] = {}  # (key) -> (value, timestamp)
        self._access_order: list = []
    
    def get(self, key: tuple) -> Optional[pd.DataFrame]:
        """Get value from cache"""
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
    
    def set(self, key: tuple, value: pd.DataFrame):
        """Set value in cache"""
        # Remove oldest if cache is full
        if len(self._cache) >= self.maxsize and self._access_order:
            oldest_key = self._access_order.pop(0)
            del self._cache[oldest_key]
        
        self._cache[key] = (value, time.time())
        if key not in self._access_order:
            self._access_order.append(key)


class InfluxDBReader(Reader):
    """
    InfluxDB 1.x reader
    
    Reads stock data from InfluxDB with optimizations:
    - LRU cache to reduce duplicate queries
    - Batch read for multiple symbols
    - Parallel queries using ThreadPoolExecutor
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Initialize InfluxDB reader
        
        Args:
            config: Configuration dict with:
                - host: str
                - port: int
                - database: str
                - username: str (optional)
                - password: str (optional)
                - cache_enabled: bool (default: True)
                - cache_size: int (default: 1000)
                - cache_ttl: int (default: 3600)
        """
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
        """Connect to InfluxDB and create database if not exists"""
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
    
    def disconnect(self):
        """Disconnect from InfluxDB"""
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
        """
        Read historical data from InfluxDB
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Time interval
        
        Returns:
            DataFrame with columns: Open, High, Low, Close, Volume
        """
        if not self._connected or not self.client:
            logger.error("Not connected to InfluxDB")
            return None
        
        # Check cache
        cache_key = (symbol, start_date, end_date, interval)
        if self.cache_enabled and self._cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {symbol}")
                return cached.copy()
        
        # Helper function to try reading with a specific symbol
        def _try_read_symbol(symbol_to_try: str) -> Optional[pd.DataFrame]:
            """Try to read data with a specific symbol format"""
            try:
                # Sanitize inputs to prevent injection (InfluxDB uses single quotes)
                safe_symbol = symbol_to_try.replace("'", "''").replace("\\", "\\\\")
                safe_interval = interval.replace("'", "''").replace("\\", "\\\\")
                
                # Build query with sanitized inputs
                start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                
                query = f"""
                SELECT open, high, low, close, volume
                FROM stock_data
                WHERE symbol = '{safe_symbol}'
                AND interval = '{safe_interval}'
                AND time >= '{start_str}'
                AND time < '{end_str}'
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
            # First try with the exact symbol provided
            df = _try_read_symbol(symbol)
            
            # If no data found and symbol starts with "US.", try without prefix
            if df is None and symbol.startswith("US."):
                alt_symbol = symbol[3:]  # Remove "US." prefix
                logger.debug(f"No data found for {symbol}, trying {alt_symbol}")
                df = _try_read_symbol(alt_symbol)
            
            # If no data found and symbol doesn't start with "US.", try with "US." prefix
            elif df is None and not symbol.startswith("US."):
                alt_symbol = f"US.{symbol}"
                logger.debug(f"No data found for {symbol}, trying {alt_symbol}")
                df = _try_read_symbol(alt_symbol)
            
            if df is None:
                return None
            
            # Store in cache
            if self.cache_enabled and self._cache:
                self._cache.set(cache_key, df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading data for {symbol}: {e}", exc_info=True)
            return None
