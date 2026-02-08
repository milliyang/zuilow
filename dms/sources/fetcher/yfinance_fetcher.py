"""
YFinance Data Fetcher

Fetch historical OHLCV from Yahoo Finance via yfinance; rate limiting, retries, and symbol normalization.

Classes:
    YFinanceFetcher  Fetcher implementation using yfinance

YFinanceFetcher:
    Config: enabled, rate_limit (seconds), retry_times. Symbol normalization: US.xxx -> xxx, HK.00700 -> 0700.HK.
    .fetch_history(symbol, start_date, end_date, interval) -> Optional[DataFrame]  Fetch and validate/clean

Dependencies:
    yfinance. Interval mapping: 1d, 1h, 5m, 15m, 30m, 1wk, 1mo.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import yfinance as yf

from .base import Fetcher

logger = logging.getLogger(__name__)


class YFinanceFetcher(Fetcher):
    """
    YFinance data fetcher
    
    Fetches historical stock data from Yahoo Finance.
    Supports rate limiting and retry mechanism.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Initialize YFinance fetcher
        
        Args:
            config: Configuration dict with:
                - enabled: bool (default: True)
                - rate_limit: float (default: 0.5) - Request interval in seconds
                - retry_times: int (default: 3) - Number of retries
        """
        super().__init__(config)
        self.rate_limit = self.config.get("rate_limit", 0.5)
        self.retry_times = self.config.get("retry_times", 3)
        self._last_request_time = 0.0
    
    def _rate_limit(self):
        """Apply rate limiting"""
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        if elapsed < self.rate_limit:
            sleep_time = self.rate_limit - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.time()
    
    def fetch_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical data from YFinance
        
        Args:
            symbol: Stock symbol (e.g., "US.AAPL" -> "AAPL", "HK.00700" -> "00700.HK")
            start_date: Start date
            end_date: End date
            interval: Time interval (1d, 1h, 5m, etc.)
        
        Returns:
            DataFrame with columns: Open, High, Low, Close, Volume
            Returns None if fetch fails
        """
        if not self.enabled:
            logger.warning("YFinance fetcher is disabled")
            return None
        
        # Normalize symbol (remove prefix like "US." or "HK.")
        symbol_clean = symbol.upper()
        if "." in symbol_clean:
            parts = symbol_clean.split(".", 1)
            if len(parts) == 2:
                prefix, code = parts
                if prefix == "US":
                    symbol_clean = code
                elif prefix == "HK":
                    # Convert HK stock code to 4-digit format for yfinance
                    # e.g., "00700" -> "0700.HK", "09988" -> "9988.HK"
                    try:
                        # Remove leading zeros and convert to int, then format as 4 digits
                        code_int = int(code.lstrip("0") or "0")
                        code_clean = f"{code_int:04d}"  # Format as 4-digit string with leading zeros
                    except ValueError:
                        # If conversion fails, use original code
                        code_clean = code.lstrip("0") or "0"
                    symbol_clean = f"{code_clean}.HK"
                else:
                    symbol_clean = code
        
        # Apply rate limiting
        self._rate_limit()
        
        # Retry mechanism
        last_error = None
        for attempt in range(self.retry_times):
            try:
                ticker = yf.Ticker(symbol_clean)
                
                # Map interval to yfinance format
                interval_map = {
                    "1d": "1d",
                    "1h": "1h",
                    "5m": "5m",
                    "15m": "15m",
                    "30m": "30m",
                    "1wk": "1wk",
                    "1mo": "1mo",
                }
                yf_interval = interval_map.get(interval, "1d")
                
                # yfinance end date is EXCLUSIVE: end="2026-02-06" returns data up to 2026-02-05.
                # Add 1 day so that the last calendar day is included (fixes "latest days never updated").
                end_date_inclusive = end_date.date() if hasattr(end_date, "date") else end_date
                end_exclusive = (end_date_inclusive + timedelta(days=1)).strftime("%Y-%m-%d")
                start_str = start_date.strftime("%Y-%m-%d")
                hist = ticker.history(
                    start=start_str,
                    end=end_exclusive,
                    interval=yf_interval,
                )
                
                if hist.empty:
                    logger.warning(f"No data returned for {symbol} ({symbol_clean})")
                    return None
                
                # Standardize column names
                df = hist[["Open", "High", "Low", "Close", "Volume"]].copy()
                
                # Validate and clean
                if not self.validate_data(df):
                    logger.warning(f"Data validation failed for {symbol}")
                    return None
                
                df = self.clean_data(df)
                
                logger.info(
                    f"Fetched {len(df)} records for {symbol} "
                    f"({start_date.date()} to {end_date.date()})"
                )
                
                return df
                
            except Exception as e:
                last_error = e
                if attempt < self.retry_times - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(
                        f"Fetch failed for {symbol} (attempt {attempt + 1}/{self.retry_times}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch data for {symbol} after {self.retry_times} attempts: {e}")
        
        return None
