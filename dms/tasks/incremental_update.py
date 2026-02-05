"""
Incremental Update Task

Per-symbol: get latest date from writer, fetch from fetcher from (latest+1) to now, write and optionally sync to backups.

Classes:
    IncrementalUpdateTask  MaintenanceTask implementation for incremental update

Config (in config): symbols, interval (default "1d"), sync_backups (default False), initial_days (default 1825).
Dependencies: DataFetcher, DataWriter, optional SyncManager; utils.data_quality.check_data_continuity.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd

from . import MaintenanceTask

logger = logging.getLogger(__name__)

# Import data quality utils (handle both package and direct import)
try:
    from ..utils.data_quality import check_data_continuity
except ImportError:
    try:
        from dms.utils.data_quality import check_data_continuity
    except ImportError:
        # Fallback: define a simple version
        def check_data_continuity(symbol, data, interval, max_gap_days=7):
            return {"is_continuous": True, "gaps": [], "total_gaps": 0}


class IncrementalUpdateTask(MaintenanceTask):
    """
    Incremental update task
    
    Checks latest date for each symbol and fetches missing data.
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
        Initialize incremental update task
        
        Args:
            name: Task name
            fetcher: DataFetcher instance
            writer: DataWriter instance
            sync_manager: SyncManager instance (optional)
            config: Task configuration
                - symbols: List of symbols to update
                - interval: Time interval (default: "1d")
                - sync_backups: Whether to sync to backups (default: False)
        """
        super().__init__(name, config)
        self.fetcher = fetcher
        self.writer = writer
        self.sync_manager = sync_manager
        
        self.symbols = self.config.get("symbols", [])
        self.interval = self.config.get("interval", "1d")
        self.sync_backups = self.config.get("sync_backups", False)
        self.initial_days = self.config.get("initial_days", 1825)  # Default: 5 years
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute incremental update
        
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
        updated_symbols = []
        failed_symbols = []
        
        for symbol in self.symbols:
            try:
                # Get latest date from database
                latest_date = self.writer.get_latest_date(symbol, self.interval)
                
                # Determine start date
                if latest_date:
                    start_date = latest_date + timedelta(days=1)
                    # Calculate data gap
                    days_gap = (datetime.now().date() - latest_date.date()).days
                    if days_gap > 7:
                        logger.warning(
                            f"{symbol}: Data gap detected! Latest date: {latest_date.date()}, "
                            f"Gap: {days_gap} days. Auto-recovery will fetch missing data."
                        )
                    elif days_gap > 1:
                        logger.info(
                            f"{symbol}: Latest date: {latest_date.date()}, "
                            f"will fetch {days_gap} days of missing data."
                        )
                else:
                    # No data exists, fetch history based on initial_days config
                    start_date = datetime.now() - timedelta(days=self.initial_days)
                    logger.info(
                        f"{symbol}: No existing data found. "
                        f"Fetching {self.initial_days} days of historical data."
                    )
                
                end_date = datetime.now()
                
                # Skip if start_date >= end_date
                if start_date >= end_date:
                    logger.debug(f"No update needed for {symbol}")
                    continue
                
                # Fetch missing data
                date_range_str = f"{start_date.date()} to {end_date.date()}"
                logger.info(f"Fetching {symbol} from {date_range_str}")
                data = self.fetcher.fetch_history(symbol, start_date, end_date, self.interval)
                
                if data is None or data.empty:
                    logger.warning(f"No data fetched for {symbol}")
                    continue
                
                # Check data continuity (for large gaps)
                if len(data) > 5:  # Only check if we fetched more than 5 days
                    continuity = check_data_continuity(symbol, data, self.interval)
                    if not continuity["is_continuous"]:
                        logger.warning(
                            f"{symbol}: Fetched data has gaps. "
                            f"This may indicate missing trading days or data source issues."
                        )
                
                # Write to database
                success = self.writer.write_data_incremental(symbol, data, self.interval)
                
                if success:
                    total_fetched += len(data)
                    total_written += len(data)
                    updated_symbols.append(symbol)
                    
                    # Enhanced logging with date range
                    if len(data) > 0:
                        first_date = data.index.min().date() if hasattr(data.index.min(), 'date') else data.index.min()
                        last_date = data.index.max().date() if hasattr(data.index.max(), 'date') else data.index.max()
                        logger.info(
                            f"✓ Updated {symbol}: {len(data)} records "
                            f"({first_date} to {last_date})"
                        )
                    
                    # Sync to backups if configured
                    if self.sync_backups and self.sync_manager:
                        try:
                            self.sync_manager.sync_to_all_backups(
                                symbol=symbol,
                                interval=self.interval,
                                sync_mode="incremental",
                            )
                        except Exception as e:
                            logger.warning(f"Failed to sync {symbol} to backups: {e}")
                else:
                    failed_symbols.append(symbol)
                    logger.error(f"Failed to write data for {symbol}")
                    
            except Exception as e:
                failed_symbols.append(symbol)
                logger.error(f"Error updating {symbol}: {e}", exc_info=True)
        
        # Enhanced result with more details
        result = {
            "success": len(failed_symbols) == 0,
            "message": f"Updated {len(updated_symbols)} symbols, {len(failed_symbols)} failed",
            "data_count": total_written,
            "updated_symbols": updated_symbols,
            "failed_symbols": failed_symbols,
            "total_symbols": len(self.symbols),
            "success_rate": f"{len(updated_symbols)/len(self.symbols)*100:.1f}%" if self.symbols else "0%",
        }
        
        # Summary log
        if len(updated_symbols) > 0:
            logger.info(
                f"✓ Task completed successfully: {len(updated_symbols)}/{len(self.symbols)} symbols updated, "
                f"{total_written} records written"
            )
        if len(failed_symbols) > 0:
            logger.error(
                f"✗ Task completed with errors: {len(failed_symbols)} symbols failed: {failed_symbols}"
            )
        
        return result
