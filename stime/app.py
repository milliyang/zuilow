"""
Simulation Time Service: single source of "current time" for full-stack simulation (ZuiLow, PPT, etc.).

Used for: sim mode; ZuiLow and PPT fetch GET /now or receive X-Simulation-Time on tick so all use the same sim clock.

API:
    GET  /now                      -> {"now": "ISO8601 UTC"}
    POST /set                      body {"now": "ISO8601 UTC"}
    POST /advance                  body {"days"|"hours"|"minutes"|"seconds": N}
    POST /advance-and-tick         body e.g. {"minutes": 120, "steps": 12, "snap_to_boundary": true}; 202, poll /status
    GET  /advance-and-tick/status  -> running, steps_done, steps_total, executed_total, now
    POST /advance-and-tick/cancel  cancel running job
    POST /config                   override tick_urls, tick_timeout (optional)

Features:
    - All times in UTC; default sim time set by DEFAULT_SIM_* at top of file.
    - Advance-and-tick: each step advances time then POSTs to TICK_URLS (e.g. ZuiLow then PPT) with X-Simulation-Time; first URL failure aborts.
    - With 60/120/180 min step, extra tick at market open/close when crossed (if MARKET_OPEN_TIME/MARKET_CLOSE_TIME set).
    - Optional end_date (YYYY-MM-DD) in advance-and-tick body: stop when sim date > end_date.
"""

# ========== Default config (edit here; all overridable by env) ==========
# Sim time (UTC), initial now at startup
DEFAULT_SIM_YEAR = 2024
DEFAULT_SIM_MONTH = 9
DEFAULT_SIM_DAY = 24
DEFAULT_SIM_HOUR = 13
DEFAULT_SIM_MINUTE = 0
DEFAULT_SIM_SECOND = 0

# For 60/120 min step, extra tick at market open/close (env: MARKET_OPEN_TIME, MARKET_CLOSE_TIME, MARKET_TIMEZONE)
DEFAULT_MARKET_OPEN_TIME = "09:30"
DEFAULT_MARKET_CLOSE_TIME = "16:00"
DEFAULT_MARKET_TIMEZONE = "America/New_York"

# Logging and HTTP (env: LOG_FILE, LOG_LEVEL / ZUILOW_TICK_TIMEOUT / STIME_PORT)
DEFAULT_LOG_FILE = "run/logs/stime.log"
DEFAULT_TICK_TIMEOUT = 600                     # 600 seconds
DEFAULT_PORT = 11185
# ==========

from datetime import datetime, timezone, timedelta, time as dt_time, date
from typing import Optional
import os
import re
import logging
import threading
from pathlib import Path
import requests as _requests
from flask import Flask, request, jsonify, send_from_directory

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # no open/close extra tick on Python < 3.9

logger = logging.getLogger(__name__)


def _setup_logging():
    """Configure logging to run/logs/stime.log (or LOG_FILE)."""
    log_level = (os.getenv("LOG_LEVEL") or "INFO").upper()
    log_file = os.getenv("LOG_FILE", DEFAULT_LOG_FILE)
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level, logging.INFO))
    root.handlers.clear()
    fmt = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(getattr(logging, log_level, logging.INFO))
    fh.setFormatter(fmt)
    root.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, log_level, logging.INFO))
    ch.setFormatter(fmt)
    root.addHandler(ch)
    logger.info("Stime logging initialized: level=%s, file=%s", log_level, log_file)


_setup_logging()

# In-memory current sim time (UTC), initialized from DEFAULT_SIM_* above
_current_time: datetime = datetime(
    DEFAULT_SIM_YEAR, DEFAULT_SIM_MONTH, DEFAULT_SIM_DAY,
    DEFAULT_SIM_HOUR, DEFAULT_SIM_MINUTE, DEFAULT_SIM_SECOND,
    tzinfo=timezone.utc,
)

# Tick URLs: multiple URLs (e.g. zuilow then PPT). Web override: POST /config tick_urls (comma-separated)
_tick_urls_override: list[str] | None = None
# Legacy: single URL override (maps to first tick URL when TICK_URLS not set)
_zuilow_tick_url_override: str | None = None
# Tick request timeout (seconds): env ZUILOW_TICK_TIMEOUT or web override (POST /config)
_zuilow_tick_timeout_override: int | None = None


def get_zuilow_tick_url() -> str:
    """First tick URL (backward compat). Prefer get_tick_urls()[0] when using multiple."""
    urls = get_tick_urls()
    return urls[0] if urls else ""


def get_tick_urls() -> list[str]:
    """Ordered list of tick URLs to POST after each advance (env TICK_URLS or ZUILOW_TICK_URL, or web override)."""
    global _tick_urls_override, _zuilow_tick_url_override
    if _tick_urls_override is not None:
        return [u.strip().rstrip("/") for u in _tick_urls_override if u and u.strip()]
    raw = (os.getenv("TICK_URLS") or "").strip()
    if raw:
        return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]
    single = None
    if _zuilow_tick_url_override and _zuilow_tick_url_override.strip():
        single = _zuilow_tick_url_override.strip().rstrip("/")
    if not single:
        single = (os.getenv("ZUILOW_TICK_URL") or "").strip().rstrip("/")
    if not single:
        single = (os.getenv("ZUILOW_TICK_URL_PUBLIC") or "").strip().rstrip("/")
    return [single] if single else []


def get_zuilow_tick_timeout() -> int:
    """Tick request timeout (seconds): web override if set, else ZUILOW_TICK_TIMEOUT (default 600)."""
    if _zuilow_tick_timeout_override is not None and _zuilow_tick_timeout_override > 0:
        return _zuilow_tick_timeout_override
    return int(os.getenv("ZUILOW_TICK_TIMEOUT", str(DEFAULT_TICK_TIMEOUT)) or str(DEFAULT_TICK_TIMEOUT)) or DEFAULT_TICK_TIMEOUT


# Advance-and-tick job state (background run, cancellable, queryable)
_advance_tick_lock = threading.Lock()
_advance_tick_state = {
    "running": False,
    "steps_total": 0,
    "steps_done": 0,
    "executed_total": 0,
    "cancelled": False,
    "error": None,
    "now": None,
}
_advance_tick_cancel_event = threading.Event()


def get_now() -> datetime:
    return _current_time


def set_now(dt: datetime) -> None:
    global _current_time
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    _current_time = dt


def advance(**kwargs) -> datetime:
    global _current_time
    delta = timedelta(**kwargs)
    _current_time = _current_time + delta
    return _current_time


def _snap_to_previous_minute_boundary(dt: datetime, step_minutes: int) -> datetime:
    """Snap datetime to previous step boundary (e.g. 12:11 -> 12:00 for 30 min; 12:11 -> 12:10 for 5 min)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    total_minutes = dt.hour * 60 + dt.minute
    q = (total_minutes // step_minutes) * step_minutes
    return dt.replace(hour=q // 60, minute=q % 60, second=0, microsecond=0)


def _parse_time_hhmm(s: str) -> tuple[int, int] | None:
    """Parse '09:30' or '9:30' -> (9, 30). Returns None if invalid."""
    if not s or not isinstance(s, str):
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})$", s.strip())
    if not m:
        return None
    h, mn = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 23 and 0 <= mn <= 59:
        return (h, mn)
    return None


def _get_market_open_close_utc_today(utc_dt: datetime) -> tuple[datetime | None, datetime | None]:
    """
    Return (open_utc, close_utc) for the calendar day of utc_dt in market timezone.
    Default: 09:30 / 16:00 America/New_York. Override with env MARKET_OPEN_TIME, MARKET_CLOSE_TIME, MARKET_TIMEZONE.
    Set env to empty to disable open/close tick. If ZoneInfo unavailable, returns (None, None).
    """
    if ZoneInfo is None:
        return None, None
    open_raw = (os.getenv("MARKET_OPEN_TIME") if os.getenv("MARKET_OPEN_TIME") is not None else DEFAULT_MARKET_OPEN_TIME).strip()
    close_raw = (os.getenv("MARKET_CLOSE_TIME") if os.getenv("MARKET_CLOSE_TIME") is not None else DEFAULT_MARKET_CLOSE_TIME).strip()
    if not open_raw and not close_raw:
        return None, None
    tz_raw = os.getenv("MARKET_TIMEZONE")
    tz_name = (tz_raw if tz_raw is not None else DEFAULT_MARKET_TIMEZONE).strip() or DEFAULT_MARKET_TIMEZONE
    try:
        market_tz = ZoneInfo(tz_name)
    except Exception:
        return None, None
    open_hm = _parse_time_hhmm(open_raw) if open_raw else None
    close_hm = _parse_time_hhmm(close_raw) if close_raw else None
    if not open_hm and not close_hm:
        return None, None
    market_dt = utc_dt.astimezone(market_tz)
    today = market_dt.date()
    open_utc = close_utc = None
    if open_hm:
        open_local = datetime.combine(today, dt_time(open_hm[0], open_hm[1], 0), tzinfo=market_tz)
        open_utc = open_local.astimezone(timezone.utc)
    if close_hm:
        close_local = datetime.combine(today, dt_time(close_hm[0], close_hm[1], 0), tzinfo=market_tz)
        close_utc = close_local.astimezone(timezone.utc)
    return open_utc, close_utc


def _post_tick(tick_urls: list[str], tick_timeout: int) -> tuple[bool, int]:
    """POST to all tick_urls with current get_now() as X-Simulation-Time. Returns (all_ok, executed_from_first)."""
    sim_now_iso = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
    headers = {"Content-Type": "application/json", "X-Simulation-Time": sim_now_iso}
    webhook_token = os.environ.get("WEBHOOK_TOKEN", "").strip()
    if webhook_token:
        headers["X-Webhook-Token"] = webhook_token
    executed = 0
    for j, url in enumerate(tick_urls):
        try:
            r = _requests.post(url, headers=headers, timeout=tick_timeout)
            logger.info("[Advance+Tick] tick %s -> %d", url, r.status_code)
        except _requests.RequestException as e:
            logger.warning("Tick %s failed: %s", url, e)
            if j == 0:
                return False, 0
            continue
        if r.ok and j == 0:
            d = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            executed = d.get("executed", 0)
        elif not r.ok and j == 0:
            logger.warning("Tick %s HTTP %d: %s", url, r.status_code, r.text[:200])
            return False, 0
    return True, executed


app = Flask(__name__, static_folder="static", static_url_path="/static")


@app.route("/now", methods=["GET"])
def api_now():
    """Return current simulation time in ISO 8601 UTC."""
    now = get_now()
    return jsonify({"now": now.strftime("%Y-%m-%dT%H:%M:%SZ")})


@app.route("/set", methods=["POST"])
def api_set():
    """Set current simulation time. Body: {"now": "2024-01-15T09:35:00Z"}."""
    data = request.get_json(silent=True) or {}
    now_str = (data.get("now") or "").strip()
    if not now_str:
        return jsonify({"error": "missing 'now' (ISO datetime)"}), 400
    try:
        if now_str.endswith("Z"):
            now_str = now_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(now_str)
        set_now(dt)
        now_iso = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info("[Set] sim time set to %s", now_iso)
        return jsonify({"now": now_iso})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/advance", methods=["POST"])
def api_advance():
    """Advance time by delta. Body: {"seconds": 300} or {"minutes": 5} or {"days": 1}."""
    data = request.get_json(silent=True) or {}
    kwargs = {}
    if "days" in data:
        kwargs["days"] = int(data["days"])
    if "hours" in data:
        kwargs["hours"] = int(data["hours"])
    if "minutes" in data:
        kwargs["minutes"] = int(data["minutes"])
    if "seconds" in data:
        kwargs["seconds"] = int(data["seconds"])
    if not kwargs:
        return jsonify({"error": "missing one of: days, hours, minutes, seconds"}), 400
    if any(kwargs[k] < 1 for k in kwargs):
        return jsonify({"error": "days, hours, minutes, seconds must be >= 1"}), 400
    try:
        advance(**kwargs)
        now_iso = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info("[Advance] %s -> %s", kwargs, now_iso)
        return jsonify({"now": now_iso})
    except (TypeError, ValueError) as e:
        return jsonify({"error": str(e)}), 400


def _advance_tick_worker(
    unit: str,
    step_value: int,
    steps_count: int,
    tick_urls: list[str],
    tick_timeout: int,
    snap_to_boundary: bool = False,
    end_date: Optional[date] = None,
):
    """Run steps_count advances; each step POSTs to tick_urls with X-Simulation-Time; first failure aborts. If end_date set, stop when sim date > end_date."""
    global _advance_tick_state
    one_step = {unit: step_value}
    executed_total = 0
    try:
        if snap_to_boundary and unit == "minutes" and step_value in (5, 15, 30, 60, 120, 180):
            set_now(_snap_to_previous_minute_boundary(get_now(), step_value))
        with _advance_tick_lock:
            _advance_tick_state["running"] = True
            _advance_tick_state["steps_total"] = steps_count
            _advance_tick_state["steps_done"] = 0
            _advance_tick_state["executed_total"] = 0
            _advance_tick_state["cancelled"] = False
            _advance_tick_state["error"] = None
            _advance_tick_state["now"] = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
        _advance_tick_cancel_event.clear()
        logger.info("[Advance+Tick] started: %s steps x %s=%s, tick_urls=%s", steps_count, unit, step_value, tick_urls)
        for i in range(steps_count):
            if _advance_tick_cancel_event.is_set():
                with _advance_tick_lock:
                    _advance_tick_state["cancelled"] = True
                logger.info("[Advance+Tick] cancelled (step %d/%d)", i + 1, steps_count)
                break
            advance(**one_step)
            now_after_step = get_now()
            if end_date is not None and now_after_step.date() > end_date:
                set_now(now_after_step - timedelta(**one_step))
                with _advance_tick_lock:
                    _advance_tick_state["steps_done"] = i
                    _advance_tick_state["executed_total"] = executed_total
                    _advance_tick_state["now"] = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.info("[Advance+Tick] stopped at end_date %s (after %d steps)", end_date.isoformat(), i)
                break
            # When 60/120/180-min step: insert one tick at market open and one at close if we crossed them
            step_ok = True
            if unit == "minutes" and step_value in (60, 120, 180):
                prev = now_after_step - timedelta(minutes=step_value)
                open_utc, close_utc = _get_market_open_close_utc_today(now_after_step)
                boundaries = [t for t in (open_utc, close_utc) if t is not None and prev < t < now_after_step]
                boundaries.sort()
                for b in boundaries:
                    set_now(b)
                    logger.info("[Advance+Tick] extra tick at open/close sim_now=%s", get_now().strftime("%Y-%m-%dT%H:%M:%SZ"))
                    ok, ex = _post_tick(tick_urls, tick_timeout)
                    executed_total += ex
                    if not ok:
                        set_now(now_after_step)
                        with _advance_tick_lock:
                            _advance_tick_state["error"] = "Tick failed at open/close"
                            _advance_tick_state["steps_done"] = i + 1
                            _advance_tick_state["executed_total"] = executed_total
                            _advance_tick_state["now"] = now_after_step.strftime("%Y-%m-%dT%H:%M:%SZ")
                        step_ok = False
                        break
                set_now(now_after_step)
            if step_ok:
                sim_now_iso = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.info("[Advance+Tick] step %d/%d sim_now=%s", i + 1, steps_count, sim_now_iso)
                ok, ex = _post_tick(tick_urls, tick_timeout)
                executed_total += ex
                if not ok:
                    with _advance_tick_lock:
                        _advance_tick_state["error"] = _advance_tick_state.get("error") or "Tick failed"
                        _advance_tick_state["steps_done"] = i + 1
                        _advance_tick_state["executed_total"] = executed_total
                        _advance_tick_state["now"] = sim_now_iso
                    step_ok = False
            if not step_ok:
                break
            with _advance_tick_lock:
                _advance_tick_state["steps_done"] = i + 1
                _advance_tick_state["executed_total"] = executed_total
                _advance_tick_state["now"] = sim_now_iso
    except (TypeError, ValueError) as e:
        with _advance_tick_lock:
            _advance_tick_state["error"] = str(e)
        logger.warning("Advance-and-tick error: %s", e)
    finally:
        with _advance_tick_lock:
            _advance_tick_state["running"] = False
            if _advance_tick_state["now"] is None:
                _advance_tick_state["now"] = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
        err = _advance_tick_state.get("error")
        done = _advance_tick_state.get("steps_done", 0)
        total = _advance_tick_state.get("steps_total", 0)
        if err:
            logger.warning("[Advance+Tick] finished (with error): %s, steps_done=%d/%d", err, done, total)
        elif _advance_tick_state.get("cancelled"):
            logger.info("[Advance+Tick] finished: cancelled, steps_done=%d/%d", done, total)
        else:
            logger.info("[Advance+Tick] finished: %d/%d steps, executed_total=%s", done, total, _advance_tick_state.get("executed_total", 0))


@app.route("/advance-and-tick", methods=["POST"])
def api_advance_and_tick():
    """Start advance-and-tick in background; returns 202, poll /advance-and-tick/status for progress."""
    tick_urls = get_tick_urls()
    if not tick_urls:
        return jsonify({"error": "TICK_URLS or ZUILOW_TICK_URL not set (server cannot call tick)"}), 503
    tick_timeout = get_zuilow_tick_timeout()
    if tick_timeout < 1:
        tick_timeout = 600
    data = request.get_json(silent=True) or {}
    unit = None
    step_value = 0
    steps_count = 0
    for key in ("days", "hours", "minutes", "seconds"):
        if key in data:
            unit = key
            step_value = int(data[key])
            break
    if not unit or step_value < 1:
        return jsonify({"error": "missing or invalid body: one of days, hours, minutes, seconds (>= 1)"}), 400
    if "steps" in data:
        steps_count = int(data["steps"])
        if steps_count < 1:
            return jsonify({"error": "steps must be >= 1"}), 400
    else:
        steps_count = step_value
        step_value = 1
    snap_to_boundary = bool(data.get("snap_to_boundary"))
    end_date = None
    if data.get("end_date"):
        try:
            end_date = datetime.strptime(str(data["end_date"])[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass
    with _advance_tick_lock:
        if _advance_tick_state["running"]:
            return jsonify({"error": "advance-and-tick already running", "status": _advance_tick_state}), 409
    logger.info("[Advance+Tick] started background job: %d steps (%s=%s)%s", steps_count, unit, step_value, " end_date=" + end_date.isoformat() if end_date else "")
    t = threading.Thread(
        target=_advance_tick_worker,
        args=(unit, step_value, steps_count, tick_urls, tick_timeout, snap_to_boundary, end_date),
        daemon=True,
    )
    t.start()
    return jsonify({"status": "started", "steps": steps_count}), 202


@app.route("/advance-and-tick/status", methods=["GET"])
def api_advance_and_tick_status():
    """Return current advance-and-tick job state: running, steps_done, steps_total, executed_total, cancelled, error, now."""
    with _advance_tick_lock:
        out = dict(_advance_tick_state)
    return jsonify(out)


@app.route("/advance-and-tick/cancel", methods=["POST"])
def api_advance_and_tick_cancel():
    """Request cancel of the running advance-and-tick job. Next step will not run; current step may still complete."""
    with _advance_tick_lock:
        if not _advance_tick_state["running"]:
            return jsonify({"status": "not_running", "state": _advance_tick_state}), 200
    _advance_tick_cancel_event.set()
    logger.info("[Advance+Tick] cancel requested")
    return jsonify({"status": "cancel_requested"}), 200


@app.route("/config", methods=["GET", "POST"])
def api_config():
    """
    GET: return zuilow_tick_url (first), tick_urls (list), zuilow_tick_timeout (env or web override).
    POST: set overrides. Body {"zuilow_tick_url": "http://..."} or {"tick_urls": "url1,url2"} or {"tick_urls": ["url1","url2"]}, "zuilow_tick_timeout": 3600.
    tick_urls (comma-separated or array) overrides; empty clears. zuilow_tick_url sets single URL (same as tick_urls with one element).
    """
    global _tick_urls_override, _zuilow_tick_url_override, _zuilow_tick_timeout_override
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        if "tick_urls" in data:
            raw = data["tick_urls"]
            if isinstance(raw, list):
                _tick_urls_override = [str(u).strip().rstrip("/") for u in raw if u and str(u).strip()]
            else:
                _tick_urls_override = [u.strip().rstrip("/") for u in str(raw).split(",") if u.strip()]
            if not _tick_urls_override:
                _tick_urls_override = None
        elif "zuilow_tick_url" in data:
            url = (data.get("zuilow_tick_url") or "").strip().rstrip("/")
            _zuilow_tick_url_override = url if url else None
            _tick_urls_override = None
        if "zuilow_tick_timeout" in data:
            t = data["zuilow_tick_timeout"]
            try:
                t = int(t) if t not in (None, "") else 0
            except (TypeError, ValueError):
                t = 0
            _zuilow_tick_timeout_override = t if t > 0 else None
        logger.info("tick_urls: %s, zuilow_tick_timeout override: %s",
                    get_tick_urls(), _zuilow_tick_timeout_override)
    return jsonify({
        "zuilow_tick_url": get_zuilow_tick_url() or "",
        "tick_urls": get_tick_urls(),
        "zuilow_tick_timeout": get_zuilow_tick_timeout(),
    })


@app.route("/")
def index():
    """Serve web UI for set/advance."""
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    port = int(os.getenv("STIME_PORT", str(DEFAULT_PORT)))
    app.run(host="0.0.0.0", port=port, debug=False)
