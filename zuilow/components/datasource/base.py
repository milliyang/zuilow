"""
Data source base: abstract interface for all data source implementations.

Classes:
    DataSourceType       Enum of source types
    DataSourceConfig     Dataclass config
    DataSource           Abstract base class

DataSourceType values:
    YFINANCE, INFLUXDB1, INFLUXDB2, POSTGRESQL, SQLITE, MEMORY

DataSourceConfig fields:
    type, host, port, database, username, password, servers, connect_timeout,
    token, org, bucket (InfluxDB2), extra
    .from_dict(data) -> DataSourceConfig

DataSource methods (abstract):
    .connect() -> bool
    .disconnect()
    .get_quote(symbol: str, as_of: Optional[datetime] = None) -> dict
    .get_history(symbol, start, end, interval, as_of: Optional[datetime] = None) -> Optional[DataFrame]
    .save_data(symbol, df, interval) -> bool   (optional)
    .get_latest_date(symbol, interval) -> Optional[datetime]   (optional)
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any
from enum import Enum

import pandas as pd


class DataSourceType(Enum):
    """Data source type."""
    YFINANCE = "yfinance"       # Yahoo Finance (network)
    INFLUXDB1 = "influxdb1"     # InfluxDB 1.x
    INFLUXDB2 = "influxdb2"     # InfluxDB 2.x
    POSTGRESQL = "postgresql"   # PostgreSQL
    SQLITE = "sqlite"           # SQLite (local)
    MEMORY = "memory"           # In-memory cache


@dataclass
class DataSourceConfig:
    """Data source configuration."""
    type: DataSourceType

    # Common DB options
    host: str = "localhost"
    port: int = 8086
    database: str = "stock_data"
    username: Optional[str] = None
    password: Optional[str] = None

    # Multi-server
    servers: list[dict] = field(default_factory=list)
    connect_timeout: float = 3.0

    # InfluxDB 2.x
    token: Optional[str] = None
    org: Optional[str] = None
    bucket: Optional[str] = None

    extra: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "DataSourceConfig":
        """Create config from dict."""
        ds_type = data.get("type", "yfinance")
        if isinstance(ds_type, str):
            ds_type = DataSourceType(ds_type)

        return cls(
            type=ds_type,
            host=data.get("host", "localhost"),
            port=data.get("port", 8086),
            database=data.get("database", "stock_data"),
            username=data.get("username"),
            password=data.get("password"),
            servers=data.get("servers", []),
            connect_timeout=data.get("connect_timeout", 3.0),
            token=data.get("token"),
            org=data.get("org"),
            bucket=data.get("bucket"),
            extra=data.get("extra", {}),
        )


class DataSource(ABC):
    """
    Abstract base class for data sources.
    All implementations must inherit and implement abstract methods.
    """

    def __init__(self, config: Optional[DataSourceConfig] = None):
        self.config = config
        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Connection status."""
        return self._connected

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection."""
        pass

    @abstractmethod
    def disconnect(self):
        """Close connection."""
        pass

    @abstractmethod
    def get_quote(self, symbol: str, as_of: Optional[datetime] = None) -> dict:
        """
        Get real-time quote. as_of: cap at sim time (DMS); others ignore.

        Args:
            symbol: Stock symbol.
            as_of: Optional; cap data at this time (sim mode).

        Returns:
            Quote dict.
        """
        pass

    @abstractmethod
    def get_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        as_of: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Get historical OHLCV data. as_of: cap at sim time (DMS); others ignore.

        Args:
            symbol: Stock symbol.
            start_date: Start date.
            end_date: End date.
            interval: Bar interval (1d, 1h, 5m, etc.).
            as_of: Optional; cap data at this time (sim mode).

        Returns:
            DataFrame with columns: Open, High, Low, Close, Volume
        """
        pass

    def save_data(
        self,
        symbol: str,
        data: pd.DataFrame,
        interval: str = "1d",
    ) -> bool:
        """
        Save data (database sources only).

        Returns:
            True if success.
        """
        raise NotImplementedError("This data source does not support save_data")

    def get_latest_date(
        self,
        symbol: str,
        interval: str = "1d",
    ) -> Optional[datetime]:
        """
        Get latest data date (database sources only).
        """
        return None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        return f"{self.__class__.__name__}({status})"
