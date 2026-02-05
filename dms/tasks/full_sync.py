"""
Full Sync Task

Re-fetch full history for configured symbols and date range; write and optionally sync to backups.

Classes:
    FullSyncTask  MaintenanceTask implementation for full sync

Config (in config): symbols, interval (default "1d"), start_date, end_date (or initial_days default 3650), sync_backups (default False).
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd

from . import MaintenanceTask

logger = logging.getLogger(__name__)


class FullSyncTask(MaintenanceTask):
    """
    Full sync task
    
    Re-fetches all historical data for specified symbols and time range.
    Used for data repair or initialization.
    """
    
    def __init__(
        self,
        name: str,
        fetcher,
        writer,
        sync_manager: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize full sync task
        
        Args:
            name: Task name
            fetcher: DataFetcher instance
            writer: DataWriter instance
            sync_manager: SyncManager instance (optional)
            config: Task configuration
                - symbols: List of symbols to sync
                - interval: Time interval (default: "1d")
                - start_date: Start date (optional, default: 1 year ago)
                - end_date: End date (optional, default: now)
                - sync_backups: Whether to sync to backups (default: False)
        """
        super().__init__(name, config)
        self.fetcher = fetcher
        self.writer = writer
        self.sync_manager = sync_manager
        
        self.symbols = self.config.get("symbols", [])
        self.interval = self.config.get("interval", "1d")
        self.sync_backups = self.config.get("sync_backups", False)
        
        # Time range
        end_date = self.config.get("end_date")
        if isinstance(end_date, str):
            self.end_date = datetime.fromisoformat(end_date)
        else:
            self.end_date = end_date or datetime.now()
        
        start_date = self.config.get("start_date")
        if isinstance(start_date, str):
            self.start_date = datetime.fromisoformat(start_date)
        else:
            # Default to 10 years for full sync
            initial_days = self.config.get("initial_days", 3650)
            self.start_date = start_date or (self.end_date - timedelta(days=initial_days))
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute full sync
        
        Returns:
            Execution result
        """
        if not self.symbols:
            return {
                "success": False,
                "message": "No symbols configured",
            }
        
        total_fetched = 0
        total_written = 0
        synced_symbols = []
        failed_symbols = []
        
        for symbol in self.symbols:
            try:
                logger.info(
                    f"Full sync {symbol} from {self.start_date.date()} to {self.end_date.date()}"
                )
                
                # Fetch all data
                data = self.fetcher.fetch_history(
                    symbol, self.start_date, self.end_date, self.interval
                )
                
                if data is None or data.empty:
                    logger.warning(f"No data fetched for {symbol}")
                    failed_symbols.append(symbol)
                    continue
                
                # Write to database (overwrite existing)
                success = self.writer.write_data(symbol, data, self.interval)
                
                if success:
                    total_fetched += len(data)
                    total_written += len(data)
                    synced_symbols.append(symbol)
                    logger.info(f"Synced {symbol}: {len(data)} records")
                    
                    # Sync to backups if configured
                    if self.sync_backups and self.sync_manager:
                        try:
                            self.sync_manager.sync_to_all_backups(
                                symbol=symbol,
                                interval=self.interval,
                                sync_mode="full",
                            )
                        except Exception as e:
                            logger.warning(f"Failed to sync {symbol} to backups: {e}")
                else:
                    failed_symbols.append(symbol)
                    logger.error(f"Failed to write data for {symbol}")
                    
            except Exception as e:
                failed_symbols.append(symbol)
                logger.error(f"Error syncing {symbol}: {e}", exc_info=True)
        
        return {
            "success": len(failed_symbols) == 0,
            "message": f"Synced {len(synced_symbols)} symbols, {len(failed_symbols)} failed",
            "data_count": total_written,
            "synced_symbols": synced_symbols,
            "failed_symbols": failed_symbols,
        }
