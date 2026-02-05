"""
Data Fetcher Base Class

Abstract base for fetchers; subclasses must implement fetch_history. Provides validate_data and clean_data.

Classes:
    Fetcher  Abstract base class for data fetchers

Fetcher interface:
    .enabled -> bool                                    Whether fetcher is enabled (from config)
    .fetch_history(symbol, start_date, end_date, interval) -> Optional[DataFrame]  Abstract: fetch OHLCV
    .validate_data(data) -> bool                        Check required columns and non-negative values
    .clean_data(data) -> DataFrame                      Dedupe index, sort, drop all-NaN rows

Fetcher features:
    - fetch_history returns DataFrame with columns: Open, High, Low, Close, Volume; index = DatetimeIndex
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import pandas as pd


class Fetcher(ABC):
    """
    Abstract base class for data fetchers
    
    All fetcher implementations must inherit this class and implement
    the abstract methods.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Initialize fetcher
        
        Args:
            config: Fetcher-specific configuration
        """
        self.config = config or {}
        self._enabled = self.config.get("enabled", True)
    
    @property
    def enabled(self) -> bool:
        """Whether this fetcher is enabled"""
        return self._enabled
    
    @abstractmethod
    def fetch_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical data
        
        Args:
            symbol: Stock symbol (e.g., "US.AAPL", "HK.00700")
            start_date: Start date
            end_date: End date
            interval: Time interval (1d, 1h, 5m, etc.)
        
        Returns:
            DataFrame with columns: Open, High, Low, Close, Volume
            Returns None if fetch fails
        """
        pass
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate fetched data
        
        Args:
            data: DataFrame to validate
        
        Returns:
            True if data is valid
        """
        if data is None or data.empty:
            return False
        
        # Check required columns
        required_columns = ["Open", "High", "Low", "Close", "Volume"]
        if not all(col in data.columns for col in required_columns):
            return False
        
        # Check for negative values
        if (data[["Open", "High", "Low", "Close", "Volume"]] < 0).any().any():
            return False
        
        return True
    
    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean fetched data
        
        Args:
            data: DataFrame to clean
        
        Returns:
            Cleaned DataFrame
        """
        if data is None or data.empty:
            return data
        
        # Remove duplicates
        data = data[~data.index.duplicated(keep='last')]
        
        # Sort by index
        data = data.sort_index()
        
        # Remove rows with all NaN
        data = data.dropna(how='all')
        
        return data
