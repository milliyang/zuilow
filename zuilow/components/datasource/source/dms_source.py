"""
DMS data source: sai/dms HTTP API as history data source.

Read-only (no save_data). get_quote returns latest bar from history.
Config (dict): base_url, api_prefix (/api/dms), timeout, headers (optional).
See sai/dms doc/api_reference.md.

Classes:
    DmsSource   DataSource implementation for DMS HTTP API

DmsSource methods:
    .connect() -> bool
    .disconnect()
    .get_quote(symbol: str) -> dict   (latest bar)
    .get_history(symbol, start, end, interval) -> Optional[DataFrame]

"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional, Any, List
from urllib.parse import urljoin

import pandas as pd
import requests

from zuilow.components.control.ctrl import get_current_dt

from ..base import DataSource

logger = logging.getLogger(__name__)


class DmsSource(DataSource):
    """
    DMS HTTP API data source.

    Config (dict): base_url, api_prefix (/api/dms), timeout (sec), headers (optional).
    """

    def __init__(self, config: dict):
        super().__init__(config=None)
        self._config = config or {}
        self._base_url = (self._config.get("base_url") or "http://localhost:11183").rstrip("/")
        self._api_prefix = (self._config.get("api_prefix") or "/api/dms").strip("/")
        self._timeout = int(self._config.get("timeout", 15))
        self._headers = self._config.get("headers") or {}
        self._session: Optional[requests.Session] = None
        # Client cache for get_symbols (TTL seconds; 0 = no cache)
        self._symbols_cache: Optional[List[str]] = None
        self._symbols_cache_ts: float = 0.0
        self._symbols_cache_ttl: int = int(self._config.get("symbols_cache_ttl", 300))

    def _url(self, path: str) -> str:
        base = f"{self._base_url}/{self._api_prefix}"
        return urljoin(base + "/", path.lstrip("/"))

    def connect(self) -> bool:
        try:
            url = self._url("status")
            r = requests.get(url, timeout=self._timeout, headers=self._headers)
            r.raise_for_status()
            self._connected = True
            logger.info("DMS data source connected: %s", self._base_url)
            return True
        except Exception as e:
            logger.warning("DMS data source connect failed: %s", e)
            self._connected = False
            return False

    def disconnect(self) -> None:
        self._connected = False
        if self._session:
            self._session.close()
            self._session = None

    def get_quote(self, symbol: str, as_of: Optional[datetime] = None) -> dict:
        """Approximate quote from latest bar; DMS has no real-time quote. Optional as_of caps data (sim mode)."""
        from datetime import timedelta

        end = as_of or get_current_dt()
        start = end - timedelta(days=5)
        df = self.get_history(symbol, start, end, "1d", as_of=as_of)
        if df is not None and len(df) > 0:
            last = df.iloc[-1]
            return {
                "symbol": symbol,
                "Open": float(last["Open"]),
                "High": float(last["High"]),
                "Low": float(last["Low"]),
                "Close": float(last["Close"]),
                "Volume": int(last["Volume"]) if last.get("Volume") is not None else 0,
                "source": "dms",
            }
        return {"symbol": symbol, "error": "No data or DMS unavailable"}

    def get_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        as_of: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """Fetch history bars via DMS API. Optional as_of caps data at sim time (DMS read/batch as_of)."""
        url = self._url("read/batch")
        payload = {
            "symbols": [symbol],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "interval": interval,
        }
        if as_of is not None:
            payload["as_of"] = as_of.isoformat()
        try:
            r = requests.post(
                url,
                json=payload,
                timeout=self._timeout,
                headers={**self._headers, "Content-Type": "application/json"},
            )
            r.raise_for_status()
            data = r.json()
            raw = data.get(symbol)
            if raw is None:
                return None
            records = raw.get("data") or []
            index_str = raw.get("index") or []
            if not records:
                return None
            df = pd.DataFrame(records)
            if index_str:
                df.index = pd.to_datetime(index_str)
            elif "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"])
                df.set_index("time", inplace=True)
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df = df.sort_index()
            return df
        except Exception as e:
            logger.debug("DMS get_history %s failed: %s", symbol, e)
            return None

    def get_symbols(self) -> List[str]:
        """All symbols from DMS (GET /api/dms/symbols). Client-side cached for symbols_cache_ttl sec (default 300)."""
        now = time.time()
        if self._symbols_cache_ttl > 0 and self._symbols_cache is not None:
            if (now - self._symbols_cache_ts) < self._symbols_cache_ttl:
                return self._symbols_cache
        url = self._url("symbols")
        try:
            r = requests.get(url, timeout=self._timeout, headers=self._headers)
            r.raise_for_status()
            data = r.json()
            symbols = data.get("symbols") or []
            self._symbols_cache = list(symbols)
            self._symbols_cache_ts = now
            return self._symbols_cache
        except Exception as e:
            logger.debug("DMS get_symbols failed: %s", e)
            if self._symbols_cache is not None:
                return self._symbols_cache
            return []

    def get_latest_date(self, symbol: str, interval: str = "1d") -> Optional[datetime]:
        """Get latest data date via DMS /symbol/<symbol>/info."""
        url = self._url(f"symbol/{symbol}/info")
        try:
            r = requests.get(
                url,
                params={"interval": interval},
                timeout=self._timeout,
                headers=self._headers,
            )
            r.raise_for_status()
            info = r.json()
            latest = info.get("latest_date")
            if latest:
                return datetime.fromisoformat(latest.replace("Z", "+00:00"))
            return None
        except Exception as e:
            logger.debug("DMS get_latest_date %s failed: %s", symbol, e)
            return None
