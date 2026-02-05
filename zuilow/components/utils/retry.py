"""
Retry (exponential backoff) and rate-limiting (token bucket).

Classes:
    RetryConfig   max_retries, base_delay, max_delay, exponential_base, jitter, retryable_exceptions
    retry(...)    Decorator: retry on exception with exponential backoff
    RateLimiter   Token bucket; .acquire() blocks until token available
    rate_limited(...)   Decorator: limit calls per second

RetryConfig fields:
    max_retries: int = 3
    base_delay: float = 1.0   (seconds)
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple = (Exception,)

retry decorator:
    @retry(max_retries=3, base_delay=1.0, retryable_exceptions=(requests.RequestException,))

RateLimiter:
    RateLimiter(calls_per_second: float)   Token bucket
    .acquire() -> None   Block until token available
    rate_limited(calls_per_second: float)   Decorator
"""

from __future__ import annotations

import time
import random
import functools
import logging
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, ParamSpec, Type
from threading import Lock

P = ParamSpec("P")
T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Retry config."""
    max_retries: int = 3              # Max retries
    base_delay: float = 1.0           # Base delay (seconds)
    max_delay: float = 60.0           # Max delay (seconds)
    exponential_base: float = 2.0     # Exponential base
    jitter: bool = True               # Add random jitter
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,)


def _calculate_delay(
    attempt: int,
    config: RetryConfig
) -> float:
    """Compute retry delay (exponential backoff)."""
    delay = config.base_delay * (config.exponential_base ** attempt)
    delay = min(delay, config.max_delay)
    if config.jitter:
        jitter_range = delay * 0.25
        delay += random.uniform(0, jitter_range)
    return delay


def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,),
    on_retry: Callable[[Exception, int], None] | None = None,
    config: RetryConfig | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Retry decorator (exponential backoff).

    Args:
        max_retries: Max retries
        base_delay: Base delay (seconds)
        max_delay: Max delay (seconds)
        exponential_base: Exponential base
        jitter: Add jitter
        retryable_exceptions: Retryable exception types
        on_retry: Callback (exception, attempt)
        config: RetryConfig instance
    """
    if config:
        cfg = config
    else:
        cfg = RetryConfig(
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay,
            exponential_base=exponential_base,
            jitter=jitter,
            retryable_exceptions=retryable_exceptions,
        )
    
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Exception | None = None
            
            for attempt in range(cfg.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except cfg.retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt < cfg.max_retries:
                        delay = _calculate_delay(attempt, cfg)
                        
                        logger.warning(
                            f"Retry {attempt + 1}/{cfg.max_retries} for {func.__name__}: "
                            f"{type(e).__name__}: {e}. Waiting {delay:.1f}s"
                        )
                        
                        if on_retry:
                            on_retry(e, attempt + 1)
                        
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All {cfg.max_retries} retries failed for {func.__name__}: "
                            f"{type(e).__name__}: {e}"
                        )
            
            raise last_exception  # type: ignore

        return wrapper

    return decorator


class RateLimiter:
    """
    Token-bucket rate limiter.

    Controls API call frequency.
    """

    def __init__(
        self,
        rate: int = 10,
        per: float = 60.0,
        burst: int | None = None
    ):
        """
        Args:
            rate: Requests allowed per window
            per: Window (seconds)
            burst: Burst capacity, None = rate
        """
        self.rate = rate
        self.per = per
        self.burst = burst or rate
        self._refill_rate = rate / per
        self._tokens = float(self.burst)
        self._last_refill = time.time()
        self._lock = Lock()
        self._total_requests = 0
        self._total_waits = 0
        self._total_wait_time = 0.0

    def _refill(self) -> None:
        """Refill tokens."""
        now = time.time()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self._refill_rate
        self._tokens = min(self._tokens + new_tokens, float(self.burst))
        self._last_refill = now

    def acquire(self, tokens: int = 1, block: bool = True) -> bool:
        """
        Acquire tokens.

        Args:
            tokens: Tokens needed
            block: Block until available

        Returns:
            True if acquired
        """
        with self._lock:
            self._refill()
            self._total_requests += 1
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            
            if not block:
                return False
            needed = tokens - self._tokens
            wait_time = needed / self._refill_rate
            self._total_waits += 1
            self._total_wait_time += wait_time

        logger.debug(f"Rate limited, waiting {wait_time:.2f}s")
        time.sleep(wait_time)
        with self._lock:
            self._refill()
            self._tokens -= tokens
        return True

    def try_acquire(self, tokens: int = 1) -> bool:
        """Non-blocking acquire."""
        return self.acquire(tokens, block=False)

    @property
    def available(self) -> float:
        """Available tokens."""
        with self._lock:
            self._refill()
            return self._tokens
    
    @property
    def stats(self) -> dict[str, Any]:
        """Rate limiter stats."""
        return {
            "rate": f"{self.rate}/{self.per}s",
            "burst": self.burst,
            "available_tokens": f"{self.available:.1f}",
            "total_requests": self._total_requests,
            "total_waits": self._total_waits,
            "total_wait_time": f"{self._total_wait_time:.1f}s",
        }
    
    def reset(self) -> None:
        """Reset limiter."""
        with self._lock:
            self._tokens = float(self.burst)
            self._last_refill = time.time()
            self._total_requests = 0
            self._total_waits = 0
            self._total_wait_time = 0.0


def rate_limited(
    rate: int = 10,
    per: float = 60.0,
    limiter: RateLimiter | None = None
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Rate-limit decorator.

    Args:
        rate: Requests per window
        per: Window (seconds)
        limiter: Limiter instance
    """
    _limiter = limiter or RateLimiter(rate=rate, per=per)

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            _limiter.acquire()
            return func(*args, **kwargs)
        wrapper.limiter = _limiter
        wrapper.limiter_stats = lambda: _limiter.stats
        return wrapper
    return decorator


class RateLimiters:
    """Preset rate limiters."""

    YFINANCE = RateLimiter(rate=50, per=60, burst=10)
    GENERAL = RateLimiter(rate=30, per=60, burst=5)

    @classmethod
    def get(cls, name: str) -> RateLimiter:
        """Get limiter by name."""
        return getattr(cls, name.upper(), cls.GENERAL)
