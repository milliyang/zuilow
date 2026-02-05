"""
Data Reader Base Class

Abstract base for readers; subclasses must implement connect, disconnect, read_history. read_batch provided.

Classes:
    Reader  Abstract base class for data readers

Reader methods:
    .is_connected -> bool                               Connection status
    .connect() -> bool                                  Abstract: establish connection
    .disconnect()                                       Abstract: close connection
    .read_history(symbol, start_date, end_date, interval) -> Optional[DataFrame]  Abstract: read OHLCV
    .read_batch(symbols, start_date, end_date, interval) -> Dict[symbol, DataFrame]  Default: parallel read_history

Context manager: with Reader(config) as r: ...
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict
import pandas as pd


class Reader(ABC):
    """
    Abstract base class for data readers
    
    All reader implementations must inherit this class and implement
    the abstract methods.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Initialize reader
        
        Args:
            config: Reader-specific configuration
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
    def read_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> Optional[pd.DataFrame]:
        """
        Read historical data
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Time interval
        
        Returns:
            DataFrame with columns: Open, High, Low, Close, Volume
        """
        pass
    
    def read_batch(
        self,
        symbols: list[str],
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> Dict[str, pd.DataFrame]:
        """
        Batch read multiple symbols (parallel)
        
        Args:
            symbols: List of stock symbols
            start_date: Start date
            end_date: End date
            interval: Time interval
        
        Returns:
            Dict mapping symbol to DataFrame
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self.read_history, symbol, start_date, end_date, interval): symbol
                for symbol in symbols
            }
            
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    data = future.result()
                    if data is not None:
                        results[symbol] = data
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).error(f"Error reading {symbol}: {e}")
        
        return results
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
