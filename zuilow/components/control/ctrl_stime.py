"""
Simulation time: fetch_sim_now() GETs Simulation Time Service (HTTP GET /now).

Used when SIMULATION_TIME_URL is set. get_tick_sim_time() in ctrl calls this when tick context is empty.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def fetch_sim_now() -> Optional[datetime]:
    """GET SIMULATION_TIME_URL/now, return UTC datetime or None. Single place for stime fetch."""
    base = (os.getenv("SIMULATION_TIME_URL") or "").strip().rstrip("/")
    if not base:
        return None
    try:
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
        logger.error("fetch_sim_now failed: %s", e)
        return None
