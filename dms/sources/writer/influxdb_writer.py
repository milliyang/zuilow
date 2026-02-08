"""
InfluxDB 1.x Writer

Write OHLCV to InfluxDB 1.x; single-server or multi-server mode (write to all, primary = lowest latency).

Classes:
    InfluxDBWriter  Writer implementation for InfluxDB 1.x

InfluxDBWriter:
    Config: host, port, database, username, password; optional servers list for multi-server.
    .connect() -> bool                                  Connect (single or multi); create DB if not exists; primary = fastest
    .disconnect()                                       Close all connections
    .write_data(symbol, data, interval) -> bool          Write points to all connected servers
    .get_latest_date(symbol, interval) -> Optional[datetime]  MAX(time) from stock_data
    .clear_database() -> bool                            DROP SERIES FROM stock_data (destructive)

Measurement: stock_data; tags: symbol, interval; fields: open, high, low, close, volume.
Dependencies: influxdb (InfluxDBClient).
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd

try:
    from influxdb import InfluxDBClient
    HAS_INFLUXDB = True
except ImportError:
    HAS_INFLUXDB = False

from .base import Writer

from ...core.symbol import normalize_symbol

logger = logging.getLogger(__name__)


class InfluxDBWriter(Writer):
    """
    InfluxDB 1.x writer
    
    Writes stock data to InfluxDB database.
    Supports multiple servers and batch writing.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Initialize InfluxDB writer
        
        Args:
            config: Configuration dict with:
                - host: str (default: "localhost")
                - port: int (default: 8086)
                - database: str (default: "stock_data")
                - username: str (optional)
                - password: str (optional)
                - servers: list[dict] (optional) - Multi-server mode
        """
        if not HAS_INFLUXDB:
            raise ImportError("Please install influxdb: pip install influxdb")
        
        super().__init__(config)
        
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 8086)
        self.database = self.config.get("database", "stock_data")
        self.username = self.config.get("username", "")
        self.password = self.config.get("password", "")
        
        # Multi-server mode
        self.servers = self.config.get("servers", [])
        self.clients: List[tuple] = []  # (server_id, client, latency)
        self.primary_client: Optional[InfluxDBClient] = None
    
    def connect(self) -> bool:
        """Connect to InfluxDB and create database if not exists"""
        try:
            if self.servers:
                # Multi-server mode
                self._connect_servers()
            else:
                # Single server mode
                # Connect without database first (for faster startup)
                client = InfluxDBClient(
                    host=self.host,
                    port=self.port,
                    username=self.username if self.username else None,
                    password=self.password if self.password else None,
                    timeout=5,  # 5 seconds timeout for faster connection
                )
                # Test connection with ping (quick check)
                client.ping()
                
                # Create database if not exists
                try:
                    databases = client.get_list_database()
                    db_names = [db['name'] for db in databases]
                    if self.database not in db_names:
                        client.create_database(self.database)
                        logger.info(f"Created database: {self.database}")
                    else:
                        logger.debug(f"Database already exists: {self.database}")
                except Exception as e:
                    # If get_list_database fails, try to create anyway
                    logger.warning(f"Could not check database existence, attempting to create: {e}")
                    try:
                        client.create_database(self.database)
                        logger.info(f"Created database: {self.database}")
                    except Exception as create_e:
                        # Database might already exist, continue
                        logger.debug(f"Database creation attempt: {create_e}")
                
                # Switch to the database
                client.switch_database(self.database)
                self.primary_client = client
                self.clients = [("primary", client, 0.0)]
            
            self._connected = True
            logger.info(f"Connected to InfluxDB: {len(self.clients)} server(s), database: {self.database}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            self._connected = False
            return False
    
    def _connect_servers(self):
        """Connect to multiple servers and create database if not exists"""
        import time
        
        for i, server in enumerate(self.servers):
            server_host = server.get("host", self.host)
            server_port = server.get("port", self.port)
            server_database = server.get("database", self.database)
            server_id = f"server{i+1}"
            
            try:
                # Connect without database first (for faster startup)
                start_time = time.time()
                client = InfluxDBClient(
                    host=server_host,
                    port=server_port,
                    username=server.get("username", self.username) if server.get("username") or self.username else None,
                    password=server.get("password", self.password) if server.get("password") or self.password else None,
                    timeout=5,  # 5 seconds timeout for faster connection
                )
                # Test connection with ping (quick check)
                client.ping()
                
                # Create database if not exists
                try:
                    databases = client.get_list_database()
                    db_names = [db['name'] for db in databases]
                    if server_database not in db_names:
                        client.create_database(server_database)
                        logger.info(f"[{server_id}] Created database: {server_database}")
                    else:
                        logger.debug(f"[{server_id}] Database already exists: {server_database}")
                except Exception as e:
                    # If get_list_database fails, try to create anyway
                    logger.warning(f"[{server_id}] Could not check database existence, attempting to create: {e}")
                    try:
                        client.create_database(server_database)
                        logger.info(f"[{server_id}] Created database: {server_database}")
                    except Exception as create_e:
                        # Database might already exist, continue
                        logger.debug(f"[{server_id}] Database creation attempt: {create_e}")
                
                # Switch to the database
                client.switch_database(server_database)
                latency = time.time() - start_time
                
                self.clients.append((server_id, client, latency))
                logger.info(f"Connected to {server_id} ({server_host}:{server_port}), database: {server_database}, latency: {latency:.3f}s")
                
            except Exception as e:
                logger.warning(f"Failed to connect to {server_id} ({server_host}:{server_port}): {e}")
        
        if not self.clients:
            raise ConnectionError("All InfluxDB servers are unavailable")
        
        # Select primary client (fastest)
        self.clients.sort(key=lambda x: x[2])  # Sort by latency
        self.primary_client = self.clients[0][1]
        logger.info(f"Primary server: {self.clients[0][0]}")
    
    def disconnect(self):
        """Disconnect from InfluxDB"""
        for _, client, _ in self.clients:
            try:
                client.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
        
        self.clients = []
        self.primary_client = None
        self._connected = False
        logger.info("Disconnected from InfluxDB")
    
    def write_data(
        self,
        symbol: str,
        data: pd.DataFrame,
        interval: str = "1d",
    ) -> bool:
        """
        Write data to InfluxDB
        
        Args:
            symbol: Stock symbol
            data: DataFrame with columns: Open, High, Low, Close, Volume
            interval: Time interval
        
        Returns:
            True if successful
        """
        if not self._connected:
            logger.error("Not connected to InfluxDB")
            return False
        
        if data is None or data.empty:
            logger.warning(f"Data is empty for {symbol}")
            return False
        
        symbol = normalize_symbol(symbol)
        try:
            # Prepare data points
            points = []
            for timestamp, row in data.iterrows():
                ts = pd.Timestamp(timestamp)
                if ts.tz is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                
                point = {
                    "measurement": "stock_data",
                    "tags": {"symbol": symbol, "interval": interval},
                    "time": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "fields": {
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": float(row["Volume"]),
                    },
                }
                points.append(point)
            
            # Write to all servers
            success_count = 0
            for server_id, client, _ in self.clients:
                try:
                    client.write_points(points)
                    success_count += 1
                except Exception as e:
                    logger.warning(f"[{server_id}] Write failed: {e}")
            
            if success_count > 0:
                logger.info(
                    f"Wrote {symbol}: {len(points)} points to "
                    f"{success_count}/{len(self.clients)} server(s)"
                )
                return True
            else:
                logger.error(f"Failed to write {symbol} to all servers")
                return False
            
        except Exception as e:
            logger.error(f"Error writing data for {symbol}: {e}", exc_info=True)
            return False
    
    def get_latest_date(
        self,
        symbol: str,
        interval: str = "1d",
    ) -> Optional[datetime]:
        """
        Get latest data date using aggregation query
        
        Args:
            symbol: Stock symbol
            interval: Time interval
        
        Returns:
            Latest date or None if no data
        """
        if not self._connected or not self.primary_client:
            logger.error("Not connected to InfluxDB")
            return None
        
        symbol = normalize_symbol(symbol)
        try:
            # Sanitize inputs to prevent injection (InfluxDB uses single quotes)
            safe_symbol = symbol.replace("'", "''").replace("\\", "\\\\")
            safe_interval = interval.replace("'", "''").replace("\\", "\\\\")
            
            # Use aggregation query for fast lookup
            query = f"""
            SELECT MAX(time) as latest_time
            FROM stock_data
            WHERE symbol = '{safe_symbol}' AND interval = '{safe_interval}'
            GROUP BY symbol
            """
            
            result = self.primary_client.query(query)
            
            if not result:
                return None
            
            # Extract latest time from result
            for series in result:
                for point in series:
                    latest_time_str = point.get("latest_time")
                    if latest_time_str:
                        # Parse InfluxDB timestamp
                        if isinstance(latest_time_str, str):
                            # Format: "2024-01-23T10:30:00Z"
                            latest_date = datetime.fromisoformat(
                                latest_time_str.replace("Z", "+00:00")
                            )
                        else:
                            # Already a datetime
                            latest_date = latest_time_str
                        
                        # Convert to timezone-naive datetime
                        if latest_date.tzinfo:
                            latest_date = latest_date.replace(tzinfo=None)
                        
                        return latest_date
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest date for {symbol}: {e}", exc_info=True)
            return None
    
    def clear_database(self) -> bool:
        """
        Clear all data from database (delete all measurements)
        
        WARNING: This is a destructive operation that will delete all data!
        
        Returns:
            True if successful
        """
        if not self._connected or not self.primary_client:
            logger.error("Not connected to InfluxDB")
            return False
        
        try:
            # Delete all data from stock_data measurement
            # InfluxDB query: DROP SERIES FROM stock_data
            query = "DROP SERIES FROM stock_data"
            
            # Execute on all servers
            success_count = 0
            skipped_count = 0
            for server_id, client, _ in self.clients:
                try:
                    client.query(query)
                    success_count += 1
                    logger.info(f"[{server_id}] Cleared all data from database")
                except Exception as e:
                    error_str = str(e).lower()
                    # If database doesn't exist, consider it as success (no data to clear)
                    if "database not found" in error_str or "does not exist" in error_str:
                        skipped_count += 1
                        logger.info(f"[{server_id}] Database does not exist (no data to clear)")
                    else:
                        logger.warning(f"[{server_id}] Failed to clear database: {e}")
            
            # Success if we cleared data or if database doesn't exist (no data to clear)
            if success_count > 0 or skipped_count > 0:
                if success_count > 0:
                    logger.warning(f"Database cleared: {success_count}/{len(self.clients)} server(s) completed")
                if skipped_count > 0:
                    logger.info(f"Database does not exist on {skipped_count} server(s) (no data to clear)")
                return True
            else:
                logger.error("Failed to clear database on all servers")
                return False
                
        except Exception as e:
            error_str = str(e).lower()
            # If database doesn't exist, consider it as success (no data to clear)
            if "database not found" in error_str or "does not exist" in error_str:
                logger.info("Database does not exist (no data to clear)")
                return True
            logger.error(f"Error clearing database: {e}", exc_info=True)
            return False
