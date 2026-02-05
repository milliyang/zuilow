"""
Data Writer Base Class

Abstract base for writers; subclasses must implement connect, disconnect, write_data, get_latest_date. write_data_incremental provided.

Classes:
    Writer  Abstract base class for data writers

Writer interface:
    .is_connected -> bool                               Connection status
    .connect() -> bool                                  Abstract: establish connection
    .disconnect()                                       Abstract: close connection
    .write_data(symbol, data, interval) -> bool         Abstract: write full dataset (overwrite for symbol+interval)
    .get_latest_date(symbol, interval) -> Optional[datetime]  Abstract: latest point for incremental
    .write_data_incremental(symbol, data, interval) -> bool   Filter data.index > get_latest_date then write_data

Writer features:
    - Context manager: with Writer(config) as w: ...
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import pandas as pd
import logging


class Writer(ABC):
    """
    Abstract base class for data writers
    
    All writer implementations must inherit this class and implement
    the abstract methods.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Initialize writer
        
        Args:
            config: Writer-specific configuration
        """
        self.config = config or {}
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        """Connection status"""
        return self._connected
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection"""
        pass
    
    @abstractmethod
    def write_data(
        self,
        symbol: str,
        data: pd.DataFrame,
        interval: str = "1d",
    ) -> bool:
        """
        Write data to database
        
        Args:
            symbol: Stock symbol
            data: DataFrame with columns: Open, High, Low, Close, Volume
            interval: Time interval
        
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def get_latest_date(
        self,
        symbol: str,
        interval: str = "1d",
    ) -> Optional[datetime]:
        """
        Get latest data date (for incremental detection)
        
        Args:
            symbol: Stock symbol
            interval: Time interval
        
        Returns:
            Latest date or None if no data
        """
        pass
    
    def write_data_incremental(
        self,
        symbol: str,
        data: pd.DataFrame,
        interval: str = "1d",
    ) -> bool:
        """
        Write data incrementally (avoid duplicates)
        
        Args:
            symbol: Stock symbol
            data: DataFrame to write
            interval: Time interval
        
        Returns:
            True if successful
        """
        if data is None or data.empty:
            return True
        
        original_count = len(data)
        
        # Get existing data's latest date
        latest_date = self.get_latest_date(symbol, interval)
        
        if latest_date:
            # Filter out data that already exists
            data = data[data.index > latest_date]
            filtered_count = original_count - len(data)
            
            if filtered_count > 0:
                logger = logging.getLogger(__name__)
                logger.debug(
                    f"{symbol}: Filtered {filtered_count} duplicate records "
                    f"(already exists before {latest_date.date()})"
                )
        
        if data.empty:
            return True
        
        return self.write_data(symbol, data, interval)
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
