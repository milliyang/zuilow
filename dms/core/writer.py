"""
Data writer manager: manages underlying Writer (e.g. InfluxDB); unified write interface, incremental write, latest-date query.

Used for: DMS and tasks write path; supports type="influxdb1"; connection created in _load_writer.

Classes:
    DataWriter  Writer manager

DataWriter methods:
    .set_writer(writer) -> None                     Set Writer instance
    .writer -> Optional[Writer]                     Current Writer
    .write_data(symbol, data, interval) -> bool     Write data (full overwrite for symbol+interval)
    .write_data_incremental(symbol, data, interval) -> bool   Incremental write (append by timestamp)
    .get_latest_date(symbol, interval) -> Optional[datetime]  Latest data date for symbol+interval
    .clear_database() -> bool                       Clear current DB (dangerous)

DataWriter features:
    - Constructor: writer_config e.g. {"type": "influxdb1", "host", "port", "database", ...}
"""

import logging
from typing import Optional
import pandas as pd
from datetime import datetime

from ..sources.writer.base import Writer
from ..sources.writer.influxdb_writer import InfluxDBWriter

logger = logging.getLogger(__name__)


class DataWriter:
    """
    Data writer manager
    
    Manages writer instances and provides unified interface
    for writing data to databases.
    """
    
    def __init__(self, writer_config: Optional[dict] = None):
        """
        Initialize data writer manager
        
        Args:
            writer_config: Configuration dict for writer
                {
                    "type": "influxdb1",
                    "host": "localhost",
                    "port": 8086,
                    "database": "stock_data",
                    ...
                }
        """
        self._writer: Optional[Writer] = None
        
        if writer_config:
            self._load_writer(writer_config)
    
    def _load_writer(self, config: dict):
        """Load writer from configuration"""
        writer_type = config.get("type", "influxdb1")
        
        if writer_type == "influxdb1":
            self._writer = InfluxDBWriter(config)
            self._writer.connect()
        else:
            raise ValueError(f"Unsupported writer type: {writer_type}")
    
    def set_writer(self, writer: Writer):
        """
        Set writer instance
        
        Args:
            writer: Writer instance
        """
        self._writer = writer
    
    @property
    def writer(self) -> Optional[Writer]:
        """Get writer instance"""
        return self._writer
    
    def write_data(
        self,
        symbol: str,
        data: pd.DataFrame,
        interval: str = "1d",
    ) -> bool:
        """
        Write data
        
        Args:
            symbol: Stock symbol
            data: DataFrame to write
            interval: Time interval
        
        Returns:
            True if successful
        """
        if not self._writer:
            logger.error("No writer configured")
            return False
        
        if not self._writer.is_connected:
            logger.error("Writer is not connected")
            return False
        
        try:
            return self._writer.write_data(symbol, data, interval)
        except Exception as e:
            logger.error(f"Error writing data for {symbol}: {e}", exc_info=True)
            return False
    
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
        if not self._writer:
            logger.error("No writer configured")
            return False
        
        try:
            return self._writer.write_data_incremental(symbol, data, interval)
        except Exception as e:
            logger.error(f"Error writing incremental data for {symbol}: {e}", exc_info=True)
            return False
    
    def get_latest_date(
        self,
        symbol: str,
        interval: str = "1d",
    ) -> Optional[datetime]:
        """
        Get latest data date
        
        Args:
            symbol: Stock symbol
            interval: Time interval
        
        Returns:
            Latest date or None
        """
        if not self._writer:
            logger.error("No writer configured")
            return None
        
        if not self._writer.is_connected:
            logger.error("Writer is not connected")
            return None
        
        try:
            return self._writer.get_latest_date(symbol, interval)
        except Exception as e:
            logger.error(f"Error getting latest date for {symbol}: {e}", exc_info=True)
            return None
    
    def clear_database(self) -> bool:
        """
        Clear all data from database
        
        WARNING: This is a destructive operation that will delete all data!
        
        Returns:
            True if successful
        """
        if not self._writer:
            logger.error("No writer configured")
            return False
        
        if not self._writer.is_connected:
            logger.error("Writer is not connected")
            return False
        
        try:
            return self._writer.clear_database()
        except Exception as e:
            logger.error(f"Error clearing database: {e}", exc_info=True)
            return False
