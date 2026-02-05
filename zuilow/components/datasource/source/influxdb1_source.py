"""
InfluxDB 1.x data source: single or multi-server, auto-select fastest for read.

Multi-server: write to all servers; read from fastest (by latency). Config via
DataSourceConfig with servers list or host/port/database.

Classes:
    InfluxDB1Source   InfluxDB 1.x DataSource implementation

InfluxDB1Source methods:
    .connect() -> bool
    .disconnect()
    .get_quote(symbol: str) -> dict
    .get_history(symbol, start, end, interval) -> Optional[DataFrame]
    .save_data(symbol, df, interval) -> bool
    .get_latest_date(symbol, interval) -> Optional[datetime]

"""
from datetime import datetime
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, wait
import time
import logging

import pandas as pd

try:
    from influxdb import InfluxDBClient
    HAS_INFLUXDB1 = True
except ImportError:
    HAS_INFLUXDB1 = False
    InfluxDBClient = None

from ..base import DataSource, DataSourceConfig, DataSourceType

logger = logging.getLogger(__name__)


class InfluxDB1Source(DataSource):
    """InfluxDB 1.x data source - single or multi-server, write to all."""

    def __init__(self, config: DataSourceConfig):
        if not HAS_INFLUXDB1:
            raise ImportError("Install influxdb: pip install influxdb")

        super().__init__(config)

        # (server_id, client, latency)
        self.clients: List[Tuple[str, InfluxDBClient, float]] = []
        self.primary_client: Optional[InfluxDBClient] = None
        self.primary_host: str = ""
    
    def connect(self) -> bool:
        """Connect to InfluxDB (single or multi-server)."""
        try:
            if self.config.servers:
                return self._connect_multi_server()
            else:
                return self._connect_single_server()
        except Exception as e:
            logger.error("InfluxDB connect failed: %s", e)
            return False

    def _connect_single_server(self) -> bool:
        """Connect to single server."""
        try:
            client = InfluxDBClient(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                username=self.config.username,
                password=self.config.password,
            )

            client.ping()
            self._ensure_database(client)

            server_id = f"{self.config.host}:{self.config.port}"
            self.clients = [(server_id, client, 0)]
            self.primary_client = client
            self.primary_host = server_id
            self._connected = True

            logger.info("InfluxDB connected: %s", server_id)
            return True

        except Exception as e:
            logger.error("Connect failed: %s", e)
            return False

    def _connect_multi_server(self) -> bool:
        """Connect to multiple servers, pick fastest."""
        logger.info("Checking %d InfluxDB server(s)...", len(self.config.servers))

        with ThreadPoolExecutor(max_workers=len(self.config.servers)) as executor:
            futures = {
                executor.submit(self._check_server, server): server
                for server in self.config.servers
            }
            
            done, not_done = wait(
                futures.keys(),
                timeout=self.config.connect_timeout + 1
            )
            
            for future in done:
                try:
                    result = future.result(timeout=0.1)
                    if result:
                        self.clients.append(result)
                except Exception:
                    pass
            
            for future in not_done:
                future.cancel()

        if not self.clients:
            logger.error("All InfluxDB servers unavailable")
            return False

        self.clients.sort(key=lambda x: x[2])
        self.primary_host, self.primary_client, latency = self.clients[0]

        logger.info("Primary: %s (latency %.1fms)", self.primary_host, latency * 1000)
        logger.info("Available: %d/%d", len(self.clients), len(self.config.servers))
        
        self._connected = True
        return True
    
    def _check_server(self, server: dict) -> Optional[Tuple[str, InfluxDBClient, float]]:
        """Check single server."""
        host = server.get("host", "localhost")
        port = server.get("port", 8086)
        server_id = f"{host}:{port}"
        
        try:
            start = time.time()
            client = InfluxDBClient(
                host=host,
                port=port,
                database=self.config.database,
                username=self.config.username,
                password=self.config.password,
                timeout=self.config.connect_timeout,
            )
            
            client.ping()
            latency = time.time() - start
            
            self._ensure_database(client)
            
            logger.debug("%s ok, latency %.1fms", server_id, latency * 1000)
            return (server_id, client, latency)

        except Exception as e:
            logger.debug("%s unavailable: %s", server_id, str(e)[:50])
            return None

    def _ensure_database(self, client: InfluxDBClient):
        """Ensure database exists."""
        try:
            databases = client.get_list_database()
            db_names = [db["name"] for db in databases]
            if self.config.database not in db_names:
                client.create_database(self.config.database)
                logger.info("Created database: %s", self.config.database)
        except Exception as e:
            logger.warning("Check/create database failed: %s", e)

    def disconnect(self):
        """Disconnect all clients."""
        for server_id, client, _ in self.clients:
            try:
                client.close()
            except Exception:
                pass

        self.clients = []
        self.primary_client = None
        self._connected = False
        logger.info("InfluxDB disconnected")

    def get_quote(self, symbol: str, as_of: Optional[datetime] = None) -> dict:
        """Get latest quote from DB (may not be real-time). as_of ignored."""
        if not self.primary_client:
            return {"symbol": symbol, "error": "Not connected to database"}
        
        try:
            query = f'''
            SELECT "close", "volume"
            FROM "stock_data"
            WHERE "symbol" = '{symbol}'
            ORDER BY time DESC
            LIMIT 1
            '''
            
            result = self.primary_client.query(query)
            points = list(result.get_points())
            
            if not points:
                return {"symbol": symbol, "error": "No data"}
            
            point = points[0]
            return {
                "symbol": symbol,
                "price": point.get("close"),
                "volume": point.get("volume"),
                "timestamp": point.get("time"),
                "source": "influxdb1",
                "note": "Data from DB, may not be real-time",
            }
            
        except Exception as e:
            return {"symbol": symbol, "error": str(e)}
    
    def get_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        as_of: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """Get history data. as_of ignored."""
        if not self.primary_client:
            logger.error("Not connected to database")
            return None
        
        try:
            # Format time
            start_str = self._format_time(start_date)
            end_str = self._format_time(end_date)
            
            # Escape
            symbol_esc = symbol.replace("'", "''")
            interval_esc = interval.replace("'", "''")
            
            query = f'''
            SELECT "open", "high", "low", "close", "volume"
            FROM "stock_data"
            WHERE "symbol" = '{symbol_esc}' 
            AND "interval" = '{interval_esc}'
            AND time >= '{start_str}'
            AND time <= '{end_str}'
            ORDER BY time
            '''
            
            result = self.primary_client.query(query)
            points = list(result.get_points())
            
            if not points:
                return None
            
            df = pd.DataFrame(points)
            
            # Normalize columns
            column_map = {
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
            df.rename(columns=column_map, inplace=True)
            
            # Set time index
            df["time"] = pd.to_datetime(df["time"])
            df.set_index("time", inplace=True)
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df = df.sort_index()
            
            logger.info("Loaded %s: %d rows", symbol, len(df))
            return df
            
        except Exception as e:
            logger.error("Get history failed: %s", e)
            return None
    
    def save_data(
        self,
        symbol: str,
        data: pd.DataFrame,
        interval: str = "1d",
    ) -> bool:
        """Save data to all servers."""
        if not self.clients:
            logger.error("No server available")
            return False
        
        if data is None or data.empty:
            logger.warning("Data empty: %s", symbol)
            return False
        
        try:
            # Build data points
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
                    logger.warning("[%s] Save failed: %s", server_id, e)
            
            logger.info("Saved %s: %d rows to %d/%d server(s)", symbol, len(points), success_count, len(self.clients))
            return success_count > 0
            
        except Exception as e:
            logger.error("Save data failed: %s", e)
            return False
    
    def get_latest_date(
        self,
        symbol: str,
        interval: str = "1d",
    ) -> Optional[datetime]:
        """Get latest data date."""
        if not self.primary_client:
            return None
        
        try:
            symbol_esc = symbol.replace("'", "''")
            interval_esc = interval.replace("'", "''")
            
            query = f'''
            SELECT "close"
            FROM "stock_data"
            WHERE "symbol" = '{symbol_esc}' 
            AND "interval" = '{interval_esc}'
            ORDER BY time DESC
            LIMIT 1
            '''
            
            result = self.primary_client.query(query)
            points = list(result.get_points())
            
            if not points:
                return None
            
            dt = pd.to_datetime(points[0]["time"]).to_pydatetime()
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
            
        except Exception as e:
            logger.error("Get latest date failed: %s", e)
            return None
    
    def _format_time(self, dt: datetime) -> str:
        """Format time to RFC3339."""
        ts = pd.Timestamp(dt)
        if ts.tz is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
