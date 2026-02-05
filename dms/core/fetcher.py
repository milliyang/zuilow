"""
Data fetcher manager: manages multiple sources (e.g. yfinance); unified fetch interface and primary fetcher selection.

Used for: tasks fetch history; supports yfinance from fetchers_config; futu placeholder not implemented.

Classes:
    DataFetcher  Fetcher manager

DataFetcher methods:
    .add_fetcher(name, fetcher) -> None            Register fetcher
    .get_fetcher(name=None) -> Optional[Fetcher]   Get fetcher (None = primary)
    .list_fetchers() -> list[str]                  List registered fetcher names
    .primary -> Optional[Fetcher]                  Primary fetcher
    .fetch_history(symbol, start, end, interval, fetcher_name=None) -> Optional[DataFrame]  Fetch history

DataFetcher features:
    - Constructor: fetchers_config e.g. {"yfinance": {"enabled": True, "rate_limit": 0.5, ...}}
"""

import logging
from typing import Optional, Dict
from datetime import datetime
import pandas as pd

from ..sources.fetcher.base import Fetcher
from ..sources.fetcher.yfinance_fetcher import YFinanceFetcher

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    Data fetcher manager
    
    Manages multiple fetcher sources and provides unified interface
    for fetching historical data.
    """
    
    def __init__(self, fetchers_config: Optional[Dict] = None):
        """
        Initialize data fetcher manager
        
        Args:
            fetchers_config: Configuration dict for fetchers
                {
                    "yfinance": {...},
                    "futu": {...}
                }
        """
        self._fetchers: Dict[str, Fetcher] = {}
        self._primary: Optional[str] = None
        
        if fetchers_config:
            self._load_fetchers(fetchers_config)
    
    def _load_fetchers(self, config: Dict):
        """Load fetchers from configuration"""
        # YFinance fetcher
        if "yfinance" in config:
            yf_config = config["yfinance"]
            if yf_config.get("enabled", True):
                self.add_fetcher("yfinance", YFinanceFetcher(yf_config))
                if self._primary is None:
                    self._primary = "yfinance"
        
        # Futu fetcher (optional, can be implemented in the future if needed)
        if "futu" in config:
            futu_config = config["futu"]
            if futu_config.get("enabled", False):
                logger.warning("Futu fetcher is not implemented yet, only yfinance is supported")
    
    def add_fetcher(self, name: str, fetcher: Fetcher):
        """
        Add a fetcher
        
        Args:
            name: Fetcher name
            fetcher: Fetcher instance
        """
        self._fetchers[name] = fetcher
        if self._primary is None:
            self._primary = name
        logger.info(f"Added fetcher: {name}")
    
    def get_fetcher(self, name: Optional[str] = None) -> Optional[Fetcher]:
        """
        Get fetcher by name
        
        Args:
            name: Fetcher name. If None, return primary fetcher
        
        Returns:
            Fetcher instance or None
        """
        if name:
            return self._fetchers.get(name)
        return self._fetchers.get(self._primary) if self._primary else None
    
    def list_fetchers(self) -> list[str]:
        """List all fetcher names"""
        return list(self._fetchers.keys())
    
    @property
    def primary(self) -> Optional[Fetcher]:
        """Primary fetcher"""
        return self.get_fetcher()
    
    def fetch_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        fetcher_name: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical data
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Time interval
            fetcher_name: Specific fetcher to use. If None, use primary fetcher
        
        Returns:
            DataFrame or None
        """
        fetcher = self.get_fetcher(fetcher_name)
        if not fetcher:
            logger.error(f"Fetcher not found: {fetcher_name or self._primary}")
            return None
        
        if not fetcher.enabled:
            logger.warning(f"Fetcher {fetcher_name or self._primary} is disabled")
            return None
        
        try:
            data = fetcher.fetch_history(symbol, start_date, end_date, interval)
            return data
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}", exc_info=True)
            return None
