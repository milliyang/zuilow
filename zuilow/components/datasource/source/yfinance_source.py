"""
YFinance data source (Yahoo Finance).

Used by DataSourceManager only (backtest/sim, DMS fallback, /api/market/quote fallback).
MarketService is broker-only and does not use datasource.

Classes:
    YFinanceSource         YFinance data source implementation

YFinanceSource methods:
    .connect() -> bool
    .disconnect()
    .get_quote(symbol: str, as_of: Optional[datetime] = None) -> dict
    .get_history(symbol, start, end, interval, as_of: Optional[datetime] = None) -> Optional[DataFrame]
    .get_info(symbol: str) -> dict
    .clear_cache()
    .cache_stats() -> dict
    .limiter_stats() -> dict

YFinanceSource config:
    DEFAULT_QUOTE_TTL = 60.0      # Quote cache TTL (seconds)
    DEFAULT_HISTORY_TTL = 300.0   # History cache TTL (seconds)
    DEFAULT_INFO_TTL = 3600.0     # Info cache TTL (seconds)

YFinanceSource features:
    - US stocks (AAPL, TSLA, ...)
    - HK stocks (0700.HK, ...)
    - Some A-shares (600519.SS, ...)
    - LRU cache to reduce duplicate requests
    - Token bucket rate limiting
    - Auto retry on transient failures
"""

from datetime import datetime
from typing import Optional
import logging
import pandas as pd

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

from ...control import ctrl
from ..base import DataSource, DataSourceConfig, DataSourceType
from ...utils.cache import LRUCache, CacheConfig
from ...utils.retry import retry, RateLimiter, RateLimiters

logger = logging.getLogger(__name__)


class YFinanceSource(DataSource):
    """
    YFinance data source with caching and rate limiting.

    Supports:
    - US stocks (AAPL, TSLA, ...)
    - HK stocks (0700.HK, ...)
    - Some A-shares (600519.SS, ...)

    Features:
    - LRU cache to reduce duplicate requests
    - Token bucket rate limiting
    - Auto retry on transient failures
    """

    DEFAULT_QUOTE_TTL = 60.0      # Quote cache 1 min
    DEFAULT_HISTORY_TTL = 300.0   # History cache 5 min
    DEFAULT_INFO_TTL = 3600.0     # Info cache 1 hour

    def __init__(
        self,
        config: Optional[DataSourceConfig] = None,
        cache_config: Optional[CacheConfig] = None,
        rate_limiter: Optional[RateLimiter] = None,
        enable_cache: bool = True,
        enable_rate_limit: bool = True,
    ):
        if not HAS_YFINANCE:
            raise ImportError("Install yfinance: pip install yfinance")

        super().__init__(config or DataSourceConfig(type=DataSourceType.YFINANCE))

        self._enable_cache = enable_cache
        self._cache = LRUCache(config=cache_config) if cache_config else LRUCache(
            max_size=500,
            default_ttl=self.DEFAULT_QUOTE_TTL
        )

        self._enable_rate_limit = enable_rate_limit
        self._limiter = rate_limiter or RateLimiters.YFINANCE

    def connect(self) -> bool:
        """YFinance does not require explicit connection."""
        self._connected = True
        logger.info("YFinanceSource ready")
        return True

    def disconnect(self):
        """YFinance does not require explicit disconnect."""
        self._connected = False
        logger.info("YFinanceSource disconnected")

    def _rate_limit(self) -> None:
        """Apply rate limit."""
        if self._enable_rate_limit:
            self._limiter.acquire()

    def _cache_key(self, prefix: str, *args) -> str:
        """Build cache key."""
        return f"yf:{prefix}:{':'.join(str(a) for a in args)}"

    @retry(max_retries=3, base_delay=2.0, jitter=True)
    def get_quote(self, symbol: str, as_of: Optional[datetime] = None) -> dict:
        """
        Get real-time quote (cached and rate-limited). as_of ignored (no sim cap).

        Args:
            symbol: Symbol
            as_of: Ignored (yfinance has no sim cap).

        Returns:
            Quote dict
        """
        symbol = symbol.upper()
        cache_key = self._cache_key("quote", symbol)

        if self._enable_cache:
            cached = self._cache.get(cache_key)
            if cached:
                logger.debug(f"Cache hit: {cache_key}")
                return cached

        self._rate_limit()

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            price = info.get("currentPrice") or info.get("regularMarketPrice", 0)
            prev_close = info.get("previousClose", 0)

            change = price - prev_close if price and prev_close else 0
            change_pct = (change / prev_close * 100) if prev_close else 0

            result = {
                "symbol": symbol,
                "name": info.get("shortName", info.get("longName", symbol)),
                "price": round(price, 2) if price else None,
                "currency": info.get("currency", "USD"),
                "change": round(change, 2),
                "change_pct": f"{change_pct:+.2f}%",
                "volume": info.get("volume", 0),
                "market_cap": info.get("marketCap", 0),
                "pe_ratio": info.get("trailingPE"),
                "high_52w": info.get("fiftyTwoWeekHigh"),
                "low_52w": info.get("fiftyTwoWeekLow"),
                "source": "yfinance",
                "cached": False,
                "timestamp": ctrl.get_current_time_iso(),
            }

            if self._enable_cache:
                self._cache.set(cache_key, result, ttl=self.DEFAULT_QUOTE_TTL)

            return result

        except Exception as e:
            logger.error(f"Failed to get quote {symbol}: {e}")
            return {
                "symbol": symbol,
                "error": str(e),
                "source": "yfinance",
            }

    @retry(max_retries=3, base_delay=2.0, jitter=True)
    def get_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        as_of: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Get history (cached and rate-limited). as_of ignored (no sim cap).

        Args:
            symbol: Symbol
            start_date: Start date
            end_date: End date
            interval: Interval (1d, 1h, 5m, etc.)
            as_of: Ignored (yfinance has no sim cap).

        Returns:
            DataFrame or None
        """
        symbol = symbol.upper()
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        cache_key = self._cache_key("history", symbol, start_str, end_str, interval)

        if self._enable_cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit: {cache_key}")
                return cached

        self._rate_limit()

        try:
            ticker = yf.Ticker(symbol)

            interval_map = {
                "1d": "1d", "1h": "1h", "5m": "5m",
                "15m": "15m", "30m": "30m", "1wk": "1wk", "1mo": "1mo",
            }
            yf_interval = interval_map.get(interval, "1d")

            hist = ticker.history(
                start=start_str,
                end=end_str,
                interval=yf_interval,
            )

            if hist.empty:
                return None

            df = hist[["Open", "High", "Low", "Close", "Volume"]].copy()

            if self._enable_cache:
                self._cache.set(cache_key, df, ttl=self.DEFAULT_HISTORY_TTL)

            return df

        except Exception as e:
            logger.error(f"Failed to get history {symbol}: {e}")
            return None

    @retry(max_retries=2, base_delay=1.0)
    def get_info(self, symbol: str) -> dict:
        """
        Get stock info (cached).

        Args:
            symbol: Symbol

        Returns:
            Info dict
        """
        symbol = symbol.upper()
        cache_key = self._cache_key("info", symbol)

        if self._enable_cache:
            cached = self._cache.get(cache_key)
            if cached:
                logger.debug(f"Cache hit: {cache_key}")
                return cached

        self._rate_limit()

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            result = {
                "symbol": symbol,
                "name": info.get("shortName", info.get("longName", symbol)),
                "sector": info.get("sector", "N/A"),
                "industry": info.get("industry", "N/A"),
                "country": info.get("country", "N/A"),
                "website": info.get("website", "N/A"),
                "employees": info.get("fullTimeEmployees", "N/A"),
                "description": info.get("longBusinessSummary", "")[:500],
                "source": "yfinance",
            }

            if self._enable_cache:
                self._cache.set(cache_key, result, ttl=self.DEFAULT_INFO_TTL)

            return result

        except Exception as e:
            logger.error(f"Failed to get info {symbol}: {e}")
            return {"symbol": symbol, "error": str(e)}

    # ========== Cache ==========

    def clear_cache(self) -> None:
        """Clear cache."""
        self._cache.clear()
        logger.info("YFinanceSource cache cleared")

    def cache_stats(self) -> dict:
        """Get cache stats."""
        return self._cache.stats

    def limiter_stats(self) -> dict:
        """Get rate limiter stats."""
        return self._limiter.stats
