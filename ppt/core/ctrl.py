"""
Sim control: single place for sim_mode and get/set time_iso; mirrors zuilow/components/control/ctrl.py API.

Used for: PPT simulation time; webhook/trade set tick context from X-Simulation-Time; utils/analytics use get_current_*.

Functions:
    is_sim_mode() -> bool                               True when SIMULATION_MODE or SIMULATION_TIME_URL is set
    parse_sim_time_iso(now_str) -> Optional[datetime]   Parse ISO string to UTC datetime; None on invalid
    get_time_iso() -> str                               Current sim time as ISO string (tick context or fetch stime)
    get_current_time_iso() -> str                       Current time as ISO; sim -> sim time, else real UTC; use for API /now
    get_current_dt() -> datetime                        Current time as datetime (sim or real UTC); use instead of datetime.now()
    get_time_dt() -> Optional[datetime]                 Current sim time as datetime (for quote/history as_of)
    set_time_iso(now_str) -> bool                       Parse and set tick context; returns True if set

Features:
    - Tick context (thread-local) set by webhook/trade; fallback to fetch stime when SIMULATION_TIME_URL set
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timezone
from typing import Optional

_tick_sim_time: threading.local = threading.local()


def is_sim_mode() -> bool:
    """True when SIMULATION_MODE or SIMULATION_TIME_URL is set (PPT keeps SIMULATION_MODE for compatibility)."""
    sim = (os.getenv("SIMULATION_MODE") or "").strip().lower() in ("1", "true", "yes")
    return sim or bool((os.getenv("SIMULATION_TIME_URL") or "").strip())


def parse_sim_time_iso(now_str: str) -> Optional[datetime]:
    if not (now_str := (now_str or "").strip()):
        return None
    s = now_str[:-1] + "+00:00" if now_str.endswith("Z") else now_str
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def set_tick_sim_time(dt: datetime) -> None:
    _tick_sim_time.value = dt


def get_tick_sim_time() -> Optional[datetime]:
    val = getattr(_tick_sim_time, "value", None)
    if val is not None:
        return val
    from .ctrl_stime import fetch_sim_now
    dt = fetch_sim_now()
    if dt is not None:
        _tick_sim_time.value = dt
        return dt
    return None


def clear_tick_sim_time() -> None:
    if hasattr(_tick_sim_time, "value"):
        del _tick_sim_time.value


def get_time_dt() -> Optional[datetime]:
    return get_tick_sim_time()


def get_time_iso() -> Optional[str]:
    dt = get_time_dt()
    return dt.isoformat() if dt else None


def get_current_time_iso() -> str:
    if is_sim_mode():
        s = get_time_iso()
        if not s:
            raise RuntimeError(
                "sim mode but no sim time available (tick context empty and stime fetch failed)"
            )
        return s
    return datetime.now(timezone.utc).isoformat()


def get_current_dt() -> datetime:
    if is_sim_mode():
        dt = get_time_dt()
        if dt is None:
            raise RuntimeError(
                "sim mode but no sim time available (tick context empty and stime fetch failed)"
            )
        return dt
    return datetime.now(timezone.utc)


def set_time_iso(now_str: str) -> bool:
    dt = parse_sim_time_iso(now_str)
    if dt is None:
        return False
    set_tick_sim_time(dt)
    return True
