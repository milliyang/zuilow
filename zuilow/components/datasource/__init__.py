"""
ZuiLow data source: unified data access, multiple source types.

Source types: yfinance (cached, rate-limited), influxdb1 (InfluxDB 1.x, multi-server),
DmsSource (sai/dms HTTP API). Use DataSourceManager for primary/fallback and
get_manager()/set_manager() for global singleton.

Classes:
    DataSource, DataSourceConfig, DataSourceType   Base (see base.py)
    DataSourceManager, get_manager, set_manager    Manager (see manager.py)
    YFinanceSource, InfluxDB1Source   Implementations (see source/)
"""

from .base import DataSource, DataSourceConfig, DataSourceType
from .source import YFinanceSource, InfluxDB1Source
from .manager import DataSourceManager, get_manager, set_manager

__all__ = [
    "DataSource",
    "DataSourceConfig",
    "DataSourceType",
    "YFinanceSource",
    "InfluxDB1Source",
    "DataSourceManager",
    "get_manager",
    "set_manager",
]
