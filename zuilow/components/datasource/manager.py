"""
Data source manager: multi-source registration, primary/fallback, auto fallback on failure.

Used for: backtest, sim mode, and /api/market/quote fallback when broker (MarketService)
is not connected. MarketService is broker-only and does not use datasource.

Classes:
    DataSourceManager    Register sources; set primary/fallback; get_quote/get_history
                         with automatic fallback on primary failure

DataSourceManager methods:
    .add_source(name: str, source: DataSource) -> DataSourceManager
    .remove_source(name: str) -> bool
    .get_source(name: str) -> Optional[DataSource]
    .set_primary(name: str) -> bool
    .set_fallback(name: str) -> bool
    .list_sources() -> list[str]
    .connect_all() -> Dict[str, bool]
    .disconnect_all()
    .get_quote(symbol: str, source_name: Optional[str] = None) -> dict
    .get_history(symbol, start, end, interval, source_name: Optional[str] = None) -> Optional[DataFrame]
    .save_data(symbol, df, interval, source_name: Optional[str] = None) -> bool
    .from_config(config: dict) -> DataSourceManager   (class method)
    .from_yaml(path: Path) -> DataSourceManager   (class method)

DataSourceManager features:
    - All sources support get_quote(symbol, as_of=None) and get_history(..., as_of=None)
    - get_quote: retry primary once; sim mode (as_of) no fallback; non-sim may fallback
    - get_history: retry primary once, then fallback
    - Load from config dict or YAML file
"""
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict
import logging
import yaml

import pandas as pd

from .base import DataSource, DataSourceConfig, DataSourceType
from .source import YFinanceSource, InfluxDB1Source, DmsSource

logger = logging.getLogger(__name__)


class DataSourceManager:
    """
    Data source manager: register sources, set primary/fallback, auto fallback on failure.
    """

    def __init__(self):
        self._sources: Dict[str, DataSource] = {}
        self._primary: Optional[str] = None
        self._fallback: Optional[str] = None

    def add_source(self, name: str, source: DataSource) -> "DataSourceManager":
        """Add a data source."""
        self._sources[name] = source

        if self._primary is None:
            self._primary = name
        
        return self
    
    def remove_source(self, name: str) -> bool:
        """Remove a data source."""
        if name in self._sources:
            self._sources[name].disconnect()
            del self._sources[name]
            if self._primary == name:
                self._primary = None
            return True
        return False
    
    def get_source(self, name: str) -> Optional[DataSource]:
        """Get source by name; None if not found."""
        return self._sources.get(name)

    def set_primary(self, name: str) -> bool:
        """Set primary data source."""
        if name in self._sources:
            self._primary = name
            return True
        return False
    
    def set_fallback(self, name: str) -> bool:
        """Set fallback data source."""
        if name in self._sources:
            self._fallback = name
            return True
        return False
    
    @property
    def primary(self) -> Optional[DataSource]:
        """Primary data source."""
        if self._primary:
            return self._sources.get(self._primary)
        return None
    
    @property
    def fallback(self) -> Optional[DataSource]:
        """Fallback data source."""
        if self._fallback:
            return self._sources.get(self._fallback)
        return None
    
    def list_sources(self) -> list[str]:
        """List all source names."""
        return list(self._sources.keys())

    def get_symbols(self, source_name: Optional[str] = None) -> list[str]:
        """All symbols from primary or given source (e.g. DMS). Fallback if primary has no get_symbols. Returns [] if none support it."""
        source = self._get_source(source_name)
        if not source:
            return []
        getter = getattr(source, "get_symbols", None)
        if callable(getter):
            try:
                return list(getter())
            except Exception as e:
                logger.debug("get_symbols from %s failed: %s", source_name or self._primary, e)
        if self.fallback and source != self.fallback:
            getter_fb = getattr(self.fallback, "get_symbols", None)
            if callable(getter_fb):
                try:
                    return list(getter_fb())
                except Exception as e:
                    logger.debug("get_symbols from fallback failed: %s", e)
        return []

    def connect_all(self) -> Dict[str, bool]:
        """Connect all sources."""
        results = {}
        for name, source in self._sources.items():
            results[name] = source.connect()
        return results
    
    def disconnect_all(self):
        """Disconnect all sources."""
        for source in self._sources.values():
            source.disconnect()

    def get_quote(self, symbol: str, source_name: Optional[str] = None, as_of: Optional[datetime] = None) -> dict:
        """Get quote; all sources support as_of (or None). Retry primary once; sim mode (as_of) no fallback."""
        source = self._get_source(source_name)
        if not source:
            return {"symbol": symbol, "error": "No data source available"}
        try:
            result = source.get_quote(symbol, as_of=as_of)
        except Exception as e:
            result = {"symbol": symbol, "error": str(e)}

        if "error" in result and source != self.fallback:
            try:
                result = source.get_quote(symbol, as_of=as_of)
            except Exception as e:
                result = {"symbol": symbol, "error": str(e)}
            if "error" not in result:
                return result
            if as_of is None and self.fallback:
                logger.warning("Primary failed after retry, fallback: %s  symbol:%s", self._fallback, symbol)
                result = self.fallback.get_quote(symbol, as_of=None)

        return result
    
    def get_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        source_name: Optional[str] = None,
        as_of: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """Get history; all sources support as_of (or None). Retry primary once, then fallback."""
        source = self._get_source(source_name)
        if not source:
            return None
        try:
            data = source.get_history(symbol, start_date, end_date, interval, as_of=as_of)
        except Exception:
            data = None

        if data is None and source != self.fallback:
            try:
                data = source.get_history(symbol, start_date, end_date, interval, as_of=as_of)
            except Exception:
                data = None
            if data is not None:
                return data
            if self.fallback:
                logger.info("Primary returned no data after retry, fallback: %s", self._fallback)
                try:
                    data = self.fallback.get_history(symbol, start_date, end_date, interval, as_of=as_of)
                except Exception:
                    data = None
        elif data is None and self.fallback and source != self.fallback:
            logger.info("Primary returned no data, fallback: %s", self._fallback)
            try:
                data = self.fallback.get_history(symbol, start_date, end_date, interval, as_of=as_of)
            except Exception:
                data = None

        return data
    
    def save_data(
        self,
        symbol: str,
        data: pd.DataFrame,
        interval: str = "1d",
        source_name: Optional[str] = None,
    ) -> bool:
        """Save data to source."""
        source = self._get_source(source_name)
        if not source:
            return False

        try:
            return source.save_data(symbol, data, interval)
        except NotImplementedError:
            logger.warning("%s does not support save_data", source_name or self._primary)
            return False

    def _get_source(self, name: Optional[str] = None) -> Optional[DataSource]:
        """Get source by name or primary."""
        if name:
            return self._sources.get(name)
        return self.primary
    
    @classmethod
    def from_config(cls, config: dict) -> "DataSourceManager":
        """Create manager from config dict (keys: source names, values: type + options)."""
        manager = cls()

        for name, source_config in config.items():
            source_type = source_config.get("type", "yfinance")

            if source_type == "yfinance":
                source = YFinanceSource()
            elif source_type == "influxdb1":
                ds_config = DataSourceConfig.from_dict(source_config)
                source = InfluxDB1Source(ds_config)
            elif source_type == "dms":
                source = DmsSource(source_config)
            else:
                logger.warning("Unknown data source type: %s", source_type)
                continue
            
            manager.add_source(name, source)
        
        return manager
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> "DataSourceManager":
        """Load config from YAML file."""
        with open(yaml_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        if "datasource" in config:
            return cls.from_config(config["datasource"])
        elif "database" in config:
            return cls.from_config({"database": config["database"]})
        else:
            return cls.from_config(config)
    
    def __repr__(self) -> str:
        primary_name = self._primary if self._primary else "None"
        return f"DataSourceManager(sources={len(self._sources)}, primary={primary_name})"


_default_manager: Optional[DataSourceManager] = None


def get_manager() -> DataSourceManager:
    """Get global data source manager. Default: DMS primary, yfinance fallback if config/dms.yaml exists."""
    global _default_manager
    if _default_manager is None:
        _default_manager = DataSourceManager()
        _default_manager.add_source("yfinance", YFinanceSource())
        _dms_config = Path(__file__).resolve().parent.parent.parent / "config" / "dms.yaml"
        if _dms_config.exists():
            try:
                with open(_dms_config, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                if cfg and isinstance(cfg.get("dms"), dict):
                    _default_manager.add_source("dms", DmsSource(cfg["dms"]))
                    _default_manager.set_primary("dms")
                    _default_manager.set_fallback("yfinance")
                    logger.info("Loaded DMS data source (primary), yfinance as fallback")
            except Exception as e:
                logger.warning("Failed to load config/dms.yaml: %s", e)
    return _default_manager


def set_manager(manager: DataSourceManager):
    """Set global data source manager."""
    global _default_manager
    _default_manager = manager
