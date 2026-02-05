"""
Simulation time: fetch current sim time from Simulation Time Service (HTTP GET /now).

Used for: ctrl.get_tick_sim_time() calls fetch_sim_now() when SIMULATION_TIME_URL is set and tick context is empty.

Functions:
    fetch_sim_now() -> Optional[datetime]   GET SIMULATION_TIME_URL/now; return UTC datetime or None
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def fetch_sim_now() -> Optional[datetime]:
    """GET SIMULATION_TIME_URL/now, return UTC datetime or None. Single place for stime fetch."""
    base = (os.getenv("SIMULATION_TIME_URL") or "").strip().rstrip("/")
    if not base:
        return None
    try:
        import requests
        r = requests.get(f"{base}/now", timeout=2)
        r.raise_for_status()
        now_str = (r.json().get("now") or "").strip()
        if not now_str:
            return None
        if now_str.endswith("Z"):
            now_str = now_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(now_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception as e:
        logger.warning("fetch_sim_now failed: %s", e)
        return None
