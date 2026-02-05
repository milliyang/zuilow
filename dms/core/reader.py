"""
Data reader manager: manages underlying Reader (e.g. InfluxDB); unified read interface and connection state.

Used for: DMS read_history/read_batch; supports type="influxdb1" from db_config.

Classes:
    DataReader  Reader manager

DataReader methods:
    .set_reader(reader) -> None                            Set Reader instance
    .reader -> Optional[Reader]                            Current Reader
    .read_history(symbol, start_date, end_date, interval) -> Optional[DataFrame]   Read history for one symbol
    .read_batch(symbols, start_date, end_date, interval) -> Dict[str, DataFrame]   Batch read

DataReader features:
    - Constructor: reader_config (cache, batch size), db_config (type, host, port, database); type="influxdb1"
"""

import logging
from typing import Optional, Dict
from datetime import datetime
import pandas as pd

from ..sources.reader.base import Reader
from ..sources.reader.influxdb_reader import InfluxDBReader

logger = logging.getLogger(__name__)


class DataReader:
    """
    Data reader manager
    
    Manages reader instances and provides unified interface
    for reading data from databases.
    """
    
    def __init__(self, reader_config: Optional[dict] = None, db_config: Optional[dict] = None):
        """
        Initialize data reader manager
        
        Args:
            reader_config: Reader optimization configuration
            db_config: Database configuration (host, port, database, etc.)
        """
        self._reader: Optional[Reader] = None
        self.reader_config = reader_config or {}
        
        if db_config:
            self._load_reader(db_config)
    
    def _load_reader(self, db_config: dict):
        """Load reader from configuration"""
        db_type = db_config.get("type", "influxdb1")
        
        if db_type == "influxdb1":
            # Merge reader config into db config
            config = {**db_config, **self.reader_config}
            self._reader = InfluxDBReader(config)
            self._reader.connect()
        else:
            raise ValueError(f"Unsupported reader type: {db_type}")
    
    def set_reader(self, reader: Reader):
        """
        Set reader instance
        
        Args:
            reader: Reader instance
        """
        self._reader = reader
    
    @property
    def reader(self) -> Optional[Reader]:
        """Get reader instance"""
        return self._reader
    
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
            DataFrame or None
        """
        if not self._reader:
            logger.error("No reader configured")
            return None
        
        if not self._reader.is_connected:
            logger.error("Reader is not connected")
            return None
        
        try:
            return self._reader.read_history(symbol, start_date, end_date, interval)
        except Exception as e:
            logger.error(f"Error reading data for {symbol}: {e}", exc_info=True)
            return None
    
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
        if not self._reader:
            logger.error("No reader configured")
            return {}
        
        try:
            return self._reader.read_batch(symbols, start_date, end_date, interval)
        except Exception as e:
            logger.error(f"Error batch reading data: {e}", exc_info=True)
            return {}
