"""
Data Fetchers

Fetch historical OHLCV data from external sources; abstract base Fetcher and YFinance implementation.

Classes:
    Fetcher           Abstract base: fetch_history, validate_data, clean_data
    YFinanceFetcher   Yahoo Finance via yfinance; rate limit, retry, symbol normalization (US./HK.)

Exports:
    Fetcher, YFinanceFetcher
"""

from .base import Fetcher
from .yfinance_fetcher import YFinanceFetcher

__all__ = [
    "Fetcher",
    "YFinanceFetcher",
]
