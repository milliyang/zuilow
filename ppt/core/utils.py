"""
PPT shared utilities: symbol normalization, quotes from DMS, current time/sim_mode via core.ctrl.

Used for: all PPT API and simulation; current time/sim delegated to core.ctrl (same API as zuilow/components/control/ctrl.py).

Functions:
    normalize_symbol(symbol) -> str                     Normalize to ZuiLow/Futu format (e.g. 0700.HK -> HK.00700)
    get_quote(symbol) -> dict                           Get quote from DMS (last bar Close); uses sync time (sim/real); dict with price, valid, error
    get_quotes_batch(symbols, max_workers=5) -> dict    Batch quotes from DMS (one read/batch); returns {symbol: quote_dict}
    get_current_datetime_iso() -> str                   Current time (via ctrl); sim: tick or stime; real: now() UTC; ISO str
    get_equity_date() -> date                           Current date (via ctrl.get_current_dt().date())
    is_sim_mode() -> bool                               True if simulation mode (via ctrl)
    set_sim_now_iso(iso) -> None                        Set sim-time in ctrl (tick context); called when X-Simulation-Time arrives

Features:
    - Quotes from DMS only (POST /api/dms/read/batch, last bar Close); set DMS_BASE_URL. Sim: pass as_of for price date.
"""
import os
import time
import logging
from datetime import datetime, timedelta, timezone

_logger = logging.getLogger(__name__)
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Optional, Union

from . import ctrl

# Re-export time/sim from ctrl so callers keep using core.utils
def is_sim_mode() -> bool:
    return ctrl.is_sim_mode()


def set_sim_now_iso(iso: str) -> None:
    """Set sim-time (tick context). Called when tick arrives (X-Simulation-Time)."""
    ctrl.set_time_iso(iso or "")


def get_current_datetime_iso() -> str:
    """Current time as ISO string (sim or real UTC). Via ctrl."""
    return ctrl.get_current_time_iso()


def get_equity_date() -> date:
    """Current date for equity/config. Via ctrl.get_current_dt().date()."""
    return ctrl.get_current_dt().date()


def get_sim_now_iso() -> str:
    """Alias for get_current_datetime_iso(); always returns ISO string (sim or real)."""
    return ctrl.get_current_time_iso()

# ZuiLow/Futu format: HK.00700, US.AAPL, SH.600519, SZ.000001


def _pad_hk_code(code: str) -> str:
    """Pad HK stock code to 5 digits: 700 -> 00700, 0700 -> 00700, 9988 -> 09988."""
    code = code.lstrip('0') or '0'
    return code.zfill(5)


def normalize_symbol(symbol: str) -> str:
    """
    Normalize symbol to ZuiLow/Futu format (used for quotes and storage).

    Accepted input:
    - No prefix: 00700 -> HK.00700, AAPL -> US.AAPL
    - yfinance: 0700.HK, 600519.SS, 000001.SZ
    - Futu: US.AAPL, HK.00700, SH.600519, SZ.000001

    Output: ZuiLow/Futu format (HK.00700, US.AAPL, SH.600519, SZ.000001).
    """
    symbol = symbol.strip().upper()
    if '.' not in symbol:
        if symbol.isdigit() and (len(symbol) <= 5 or symbol.startswith('0')):
            return 'HK.' + _pad_hk_code(symbol)
        return 'US.' + symbol

    parts = symbol.split('.', 1)
    prefix, suffix = parts[0], parts[1]

    # Already Futu-style: HK.0700, HK.00700, US.AAPL, SH.600519, SZ.000001
    if prefix in ('US', 'HK', 'SH', 'SZ'):
        code = suffix
        if prefix == 'HK':
            code = _pad_hk_code(code)
        return f"{prefix}.{code}"

    # yfinance-style: 0700.HK, 600519.SS, 000001.SZ -> Futu format
    if suffix == 'HK':
        return 'HK.' + _pad_hk_code(prefix)
    if suffix == 'SS':
        return 'SH.' + prefix
    if suffix == 'SZ':
        return 'SZ.' + prefix

    return symbol


def _dms_base_and_headers():
    """DMS base URL (no trailing slash) and optional X-API-Key headers for server-to-server auth."""
    base = (os.getenv("DMS_BASE_URL") or "").strip().rstrip("/")
    headers = {}
    api_key = (os.getenv("DMS_API_KEY") or "").strip()
    if api_key:
        headers["X-API-Key"] = api_key
    return base, headers


def _quote_from_dms(symbol: str, dms_base: str, as_of_iso: Optional[str], headers: dict) -> dict:
    """
    Fetch quote from DMS: POST read/batch for one symbol, use last bar Close.
    as_of_iso: in sim mode pass current sim time so DMS caps data; real mode None.
    """
    try:
        import requests
        end_dt = datetime.fromisoformat(as_of_iso.replace("Z", "+00:00")) if as_of_iso else datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=7)
        url = f"{dms_base}/api/dms/read/batch"
        payload = {
            "symbols": [symbol],
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
            "interval": "1d",
        }
        if as_of_iso:
            payload["as_of"] = as_of_iso
        r = requests.post(url, json=payload, timeout=10, headers={**headers, "Content-Type": "application/json"})
        if r.status_code != 200:
            _logger.info("quote dms: symbol=%s -> HTTP %s", symbol, r.status_code)
            return {"symbol": symbol, "price": 0, "error": f"HTTP {r.status_code}", "valid": False}
        data = r.json()
        raw = data.get(symbol)
        if not raw or not raw.get("data"):
            _logger.info("quote dms: symbol=%s -> no data", symbol)
            return {"symbol": symbol, "price": 0, "error": "no data", "valid": False}
        records = raw["data"]
        last = records[-1] if isinstance(records[-1], dict) else {}
        price = last.get("Close") or last.get("close")
        if price is None:
            _logger.info("quote dms: symbol=%s -> no Close in last bar", symbol)
            return {"symbol": symbol, "price": 0, "error": "no Close", "valid": False}
        p = float(price)
        if p <= 0:
            _logger.info("quote dms: symbol=%s -> invalid price=%s", symbol, p)
            return {"symbol": symbol, "price": 0, "error": "invalid price", "valid": False}
        _logger.info("quote dms: symbol=%s -> price=%s", symbol, p)
        return {
            "symbol": symbol,
            "price": p,
            "change": 0,
            "change_pct": 0,
            "name": symbol,
            "currency": "USD",
            "valid": True,
        }
    except Exception as e:
        _logger.info("quote dms: symbol=%s -> error=%s", symbol, e)
        return {"symbol": symbol, "price": 0, "error": str(e), "valid": False}


def _quotes_batch_from_dms(symbols: list, dms_base: str, as_of_iso: Optional[str], headers: dict) -> dict:
    """One POST read/batch for all symbols; parse last bar Close per symbol. Returns {symbol: quote_dict}."""
    if not symbols:
        return {}
    try:
        import requests
        end_dt = datetime.fromisoformat(as_of_iso.replace("Z", "+00:00")) if as_of_iso else datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=7)
        url = f"{dms_base}/api/dms/read/batch"
        payload = {
            "symbols": symbols,
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
            "interval": "1d",
        }
        if as_of_iso:
            payload["as_of"] = as_of_iso
        r = requests.post(url, json=payload, timeout=15, headers={**headers, "Content-Type": "application/json"})
        if r.status_code != 200:
            _logger.info("quotes_batch dms: HTTP %s", r.status_code)
            return {s: {"symbol": s, "price": 0, "error": f"HTTP {r.status_code}", "valid": False} for s in symbols}
        data = r.json()
        result = {}
        for s in symbols:
            raw = data.get(s)
            if not raw or not raw.get("data"):
                result[s] = {"symbol": s, "price": 0, "error": "no data", "valid": False}
                continue
            records = raw["data"]
            last = records[-1] if isinstance(records[-1], dict) else {}
            price = last.get("Close") or last.get("close")
            if price is not None and float(price) > 0:
                p = float(price)
                result[s] = {"symbol": s, "price": p, "change": 0, "change_pct": 0, "name": s, "currency": "USD", "valid": True}
                _logger.info("quote dms: symbol=%s -> price=%s", s, p)
            else:
                result[s] = {"symbol": s, "price": 0, "error": "no Close", "valid": False}
        return result
    except Exception as e:
        _logger.info("quotes_batch dms: error=%s", e)
        return {s: {"symbol": s, "price": 0, "error": str(e), "valid": False} for s in symbols}


def get_quote(symbol: str) -> dict:
    """
    Get quote for one symbol from DMS (last bar Close). Uses sync time: sim mode passes as_of, real mode uses now.
    Returns invalid quote when DMS_BASE_URL is not set.
    """
    symbol = normalize_symbol(symbol)
    dms_base, headers = _dms_base_and_headers()
    if not dms_base:
        return {"symbol": symbol, "price": 0, "error": "DMS_BASE_URL not set", "valid": False}
    as_of_iso = get_current_datetime_iso() if is_sim_mode() else None
    return _quote_from_dms(symbol, dms_base, as_of_iso, headers)


def get_quotes_batch(symbols: list, max_workers: int = 5) -> dict:
    """
    Get quotes for multiple symbols from DMS (one read/batch). Uses sync time: sim passes as_of, real uses now.
    When DMS_BASE_URL is not set, each symbol returns invalid.
    """
    if not symbols:
        return {}
    dms_base, headers = _dms_base_and_headers()
    if not dms_base:
        return {s: {"symbol": s, "price": 0, "error": "DMS_BASE_URL not set", "valid": False} for s in symbols}
    as_of_iso = get_current_datetime_iso() if is_sim_mode() else None
    if len(symbols) == 1:
        return {symbols[0]: _quote_from_dms(symbols[0], dms_base, as_of_iso, headers)}
    return _quotes_batch_from_dms(symbols, dms_base, as_of_iso, headers)
