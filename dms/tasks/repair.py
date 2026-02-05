"""
Data Repair Task

For each symbol: read recent range from reader, fetch same range from fetcher, fill gaps and overwrite invalid rows, write back; optional sync to backups.

Classes:
    DataRepairTask  MaintenanceTask implementation for repair

Config (in config): symbols, interval (default "1d"), repair_range (default 7 days).
Dependencies: DataFetcher, DataWriter, DataReader; optional SyncManager.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd

from . import MaintenanceTask

logger = logging.getLogger(__name__)


class DataRepairTask(MaintenanceTask):
    """
    Data repair task
    
    Automatically repairs known issues by comparing multiple data sources
    and selecting the most accurate data.
    """
    
    def __init__(
        self,
        name: str,
        fetcher,
        writer,
        reader,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize data repair task
        
        Args:
            name: Task name
            fetcher: DataFetcher instance
            writer: DataWriter instance
            reader: DataReader instance
            config: Task configuration
                - symbols: List of symbols to repair
                - interval: Time interval (default: "1d")
                - repair_range: Number of days to check (default: 7)
        """
        super().__init__(name, config)
        self.fetcher = fetcher
        self.writer = writer
        self.reader = reader
        
        self.symbols = self.config.get("symbols", [])
        self.interval = self.config.get("interval", "1d")
        self.repair_range = self.config.get("repair_range", 7)
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute data repair
        
        Returns:
            Repair result
        """
        if not self.symbols:
            return {
                "success": False,
                "message": "No symbols configured",
            }
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.repair_range)
        
        repaired_symbols = []
        failed_symbols = []
        total_repaired = 0
        
        for symbol in self.symbols:
            try:
                # Read existing data
                existing_data = self.reader.read_history(
                    symbol, start_date, end_date, self.interval
                )
                
                if existing_data is None or existing_data.empty:
                    logger.warning(f"No existing data for {symbol}")
                    continue
                
                # Re-fetch from source
                fresh_data = self.fetcher.fetch_history(
                    symbol, start_date, end_date, self.interval
                )
                
                if fresh_data is None or fresh_data.empty:
                    logger.warning(f"Could not fetch fresh data for {symbol}")
                    continue
                
                # Compare and find differences
                # Merge on index (timestamp)
                merged = existing_data.join(
                    fresh_data, rsuffix="_fresh", how="outer"
                )
                
                # Find records that need repair
                # (simplified: if Close differs significantly)
                if "Close" in existing_data.columns and "Close_fresh" in merged.columns:
                    diff_threshold = 0.01  # 1% difference
                    needs_repair = merged[
                        abs(merged["Close"] - merged["Close_fresh"]) / merged["Close"] > diff_threshold
                    ]
                    
                    if not needs_repair.empty:
                        # Use fresh data for repair
                        repair_data = fresh_data.loc[needs_repair.index]
                        
                        # Write repaired data
                        success = self.writer.write_data(symbol, repair_data, self.interval)
                        
                        if success:
                            repaired_symbols.append(symbol)
                            total_repaired += len(repair_data)
                            logger.info(f"Repaired {symbol}: {len(repair_data)} records")
                        else:
                            failed_symbols.append(symbol)
                    else:
                        logger.info(f"No repair needed for {symbol}")
                else:
                    logger.info(f"Could not compare data for {symbol}")
                    
            except Exception as e:
                failed_symbols.append(symbol)
                logger.error(f"Error repairing {symbol}: {e}", exc_info=True)
        
        return {
            "success": len(failed_symbols) == 0,
            "message": f"Repaired {len(repaired_symbols)} symbols, {len(failed_symbols)} failed",
            "data_count": total_repaired,
            "repaired_symbols": repaired_symbols,
            "failed_symbols": failed_symbols,
        }
