"""
LRU in-memory cache and @cached decorator to reduce duplicate API calls.

Classes:
    CacheConfig   max_size (default 1000), default_ttl (seconds), enabled
    CacheEntry    value, expire_at, created_at, hits; .is_expired
    LRUCache      LRU eviction; TTL per entry

LRUCache methods:
    .get(key: str) -> Optional[Any]
    .set(key: str, value: Any, ttl: Optional[float] = None) -> None
    .delete(key: str) -> bool
    .clear() -> None
    .stats() -> dict   (size, hits, misses, hit_rate)

cached decorator:
    @cached(ttl=300, max_size=1000, key_builder=...)   Cache function result by args/kwargs

CacheConfig:
    max_size: int = 1000
    default_ttl: float = 300.0   (seconds)
    enabled: bool = True
"""

from __future__ import annotations

import time
import hashlib
import json
import functools
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar, ParamSpec
from threading import Lock

P = ParamSpec("P")
T = TypeVar("T")


@dataclass
class CacheConfig:
    """Cache config."""
    max_size: int = 1000          # Max entries
    default_ttl: float = 300.0    # Default TTL (seconds), 5 min
    enabled: bool = True          # Enable cache


@dataclass
class CacheEntry:
    """Cache entry."""
    value: Any
    expire_at: float
    created_at: float = field(default_factory=time.time)
    hits: int = 0
    
    @property
    def is_expired(self) -> bool:
        return time.time() > self.expire_at


class LRUCache:
    """
    LRU (Least Recently Used) cache.

    - Thread-safe
    - TTL expiration
    - Evicts least recently used
    """

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: float = 300.0,
        config: CacheConfig | None = None
    ):
        if config:
            self.max_size = config.max_size
            self.default_ttl = config.default_ttl
            self.enabled = config.enabled
        else:
            self.max_size = max_size
            self.default_ttl = default_ttl
            self.enabled = True

        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Any | None:
        """Get cached value; None if missing or expired."""
        if not self.enabled:
            return None
            
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            entry = self._cache[key]

            if entry.is_expired:
                del self._cache[key]
                self._misses += 1
                return None

            self._cache.move_to_end(key)
            entry.hits += 1
            self._hits += 1
            
            return entry.value
    
    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Set cache value."""
        if not self.enabled:
            return

        ttl = ttl if ttl is not None else self.default_ttl

        with self._lock:
            if key in self._cache:
                del self._cache[key]
            while len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
            self._cache[key] = CacheEntry(
                value=value,
                expire_at=time.time() + ttl
            )
    
    def delete(self, key: str) -> bool:
        """Delete cache entry."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear cache."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
    
    def cleanup_expired(self) -> int:
        """Remove expired entries; return count removed."""
        with self._lock:
            expired_keys = [
                k for k, v in self._cache.items() 
                if v.is_expired
            ]
            for key in expired_keys:
                del self._cache[key]
            return len(expired_keys)
    
    @property
    def stats(self) -> dict[str, Any]:
        """Cache stats."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": f"{hit_rate:.1%}",
                "enabled": self.enabled,
            }
    
    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None
    
    def __len__(self) -> int:
        return len(self._cache)


_global_cache: LRUCache | None = None


def get_global_cache() -> LRUCache:
    """Get global cache instance."""
    global _global_cache
    if _global_cache is None:
        _global_cache = LRUCache()
    return _global_cache


def _make_cache_key(func: Callable, args: tuple, kwargs: dict) -> str:
    """Build cache key."""
    func_name = f"{func.__module__}.{func.__qualname__}"
    try:
        args_str = json.dumps(args, sort_keys=True, default=str)
        kwargs_str = json.dumps(kwargs, sort_keys=True, default=str)
    except (TypeError, ValueError):
        args_str = repr(args)
        kwargs_str = repr(kwargs)
    content = f"{func_name}:{args_str}:{kwargs_str}"
    return hashlib.md5(content.encode()).hexdigest()


def cached(
    ttl: float | None = None,
    key_func: Callable[..., str] | None = None,
    cache: LRUCache | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Cache decorator.

    Args:
        ttl: TTL (seconds), None = default
        key_func: Custom key function
        cache: Cache instance, None = global
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            cache_instance = cache or get_global_cache()
            if not cache_instance.enabled:
                return func(*args, **kwargs)
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = _make_cache_key(func, args, kwargs)
            cached_value = cache_instance.get(cache_key)
            if cached_value is not None:
                return cached_value
            result = func(*args, **kwargs)
            cache_instance.set(cache_key, result, ttl)
            return result
        wrapper.cache_clear = lambda: (cache or get_global_cache()).clear()
        wrapper.cache_info = lambda: (cache or get_global_cache()).stats
        return wrapper
    return decorator


def cached_short(func: Callable[P, T]) -> Callable[P, T]:
    """Short TTL (1 min)."""
    return cached(ttl=60)(func)


def cached_medium(func: Callable[P, T]) -> Callable[P, T]:
    """Medium TTL (5 min)."""
    return cached(ttl=300)(func)


def cached_long(func: Callable[P, T]) -> Callable[P, T]:
    """Long TTL (1 hour)."""
    return cached(ttl=3600)(func)
