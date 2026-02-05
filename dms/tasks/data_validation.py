"""
Data Validation Task

Read recent data per symbol, check integrity and price reasonableness, report issues (no_data, missing_cols, price_spike).

Classes:
    DataValidationTask  MaintenanceTask implementation; read-only, uses DataReader only

Config (in config): symbols, interval (default "1d"), check_range (default 30 days), max_price_change (default 0.2).
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np

from . import MaintenanceTask

logger = logging.getLogger(__name__)


class DataValidationTask(MaintenanceTask):
    """
    Data validation task
    
    Checks data integrity, price reasonableness, and generates validation reports.
    """
    
    def __init__(
        self,
        name: str,
        reader,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize data validation task
        
        Args:
            name: Task name
            reader: DataReader instance
            config: Task configuration
                - symbols: List of symbols to validate
                - interval: Time interval (default: "1d")
                - check_range: Number of days to check (default: 30)
                - max_price_change: Maximum price change percentage (default: 0.2 = 20%)
        """
        super().__init__(name, config)
        self.reader = reader
        
        self.symbols = self.config.get("symbols", [])
        self.interval = self.config.get("interval", "1d")
        self.check_range = self.config.get("check_range", 30)
        self.max_price_change = self.config.get("max_price_change", 0.2)  # 20%
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute data validation
        
        Returns:
            Validation result with issues found
        """
        if not self.symbols:
            return {
                "success": False,
                "message": "No symbols configured",
            }
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.check_range)
        
        all_issues = []
        validated_symbols = []
        failed_symbols = []
        
        for symbol in self.symbols:
            try:
                # Read data
                data = self.reader.read_history(symbol, start_date, end_date, self.interval)
                
                if data is None or data.empty:
                    failed_symbols.append(symbol)
                    all_issues.append({
                        "symbol": symbol,
                        "type": "no_data",
                        "message": "No data found",
                    })
                    continue
                
                issues = []
                
                # Check for missing values
                missing = data.isnull().sum()
                if missing.any():
                    issues.append({
                        "symbol": symbol,
                        "type": "missing_values",
                        "message": f"Missing values: {missing.to_dict()}",
                    })
                
                # Check price reasonableness
                if "Close" in data.columns:
                    # Calculate daily change
                    data["change_pct"] = data["Close"].pct_change()
                    
                    # Check for extreme changes
                    extreme_changes = data[abs(data["change_pct"]) > self.max_price_change]
                    if not extreme_changes.empty:
                        issues.append({
                            "symbol": symbol,
                            "type": "extreme_price_change",
                            "message": f"Found {len(extreme_changes)} extreme price changes (> {self.max_price_change*100}%)",
                            "dates": extreme_changes.index.tolist(),
                        })
                    
                    # Check for negative prices
                    negative_prices = data[(data["Close"] < 0) | (data["Open"] < 0) | 
                                          (data["High"] < 0) | (data["Low"] < 0)]
                    if not negative_prices.empty:
                        issues.append({
                            "symbol": symbol,
                            "type": "negative_prices",
                            "message": f"Found {len(negative_prices)} records with negative prices",
                        })
                
                # Check volume
                if "Volume" in data.columns:
                    zero_volume = data[data["Volume"] == 0]
                    if not zero_volume.empty:
                        issues.append({
                            "symbol": symbol,
                            "type": "zero_volume",
                            "message": f"Found {len(zero_volume)} records with zero volume",
                        })
                
                # Check OHLC consistency
                if all(col in data.columns for col in ["Open", "High", "Low", "Close"]):
                    invalid_ohlc = data[
                        (data["High"] < data["Low"]) |
                        (data["High"] < data["Open"]) |
                        (data["High"] < data["Close"]) |
                        (data["Low"] > data["Open"]) |
                        (data["Low"] > data["Close"])
                    ]
                    if not invalid_ohlc.empty:
                        issues.append({
                            "symbol": symbol,
                            "type": "invalid_ohlc",
                            "message": f"Found {len(invalid_ohlc)} records with invalid OHLC",
                        })
                
                if issues:
                    all_issues.extend(issues)
                else:
                    validated_symbols.append(symbol)
                    logger.info(f"Validation passed for {symbol}")
                    
            except Exception as e:
                failed_symbols.append(symbol)
                logger.error(f"Error validating {symbol}: {e}", exc_info=True)
                all_issues.append({
                    "symbol": symbol,
                    "type": "validation_error",
                    "message": str(e),
                })
        
        # Log detailed issues
        if all_issues:
            logger.warning(f"Found {len(all_issues)} validation issues:")
            for issue in all_issues:
                issue_type = issue.get("type", "unknown")
                symbol = issue.get("symbol", "unknown")
                message = issue.get("message", "")
                logger.warning(f"  - {symbol}: [{issue_type}] {message}")
        
        return {
            "success": len(failed_symbols) == 0 and len(all_issues) == 0,
            "message": f"Validated {len(validated_symbols)} symbols, found {len(all_issues)} issues",
            "validated_symbols": validated_symbols,
            "failed_symbols": failed_symbols,
            "issues": all_issues,
            "issue_count": len(all_issues),
        }
