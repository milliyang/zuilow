"""
Simulation Time Service: single source of "current time" for full-stack simulation.

API:
  GET  /now     -> {"now": "2024-01-15T09:35:00Z"} (ISO 8601 UTC)
  POST /set     body {"now": "2024-01-15T09:35:00Z"}
  POST /advance body {"seconds": 300} or {"minutes": 5} or {"days": 1}

All times stored and returned in UTC. Web UI at /.

Step & trigger (UI "Advance + Trigger ZuiLow tick"): The UI runs N steps (e.g. N days).
Each step: POST /advance (1 unit) -> POST zuilow /api/scheduler/tick -> **wait for response**
before the next step. So ZuiLow always sees the correct sim-time for that step; no race
where sim-time jumps ahead while ZuiLow is still processing.
"""

from datetime import datetime, timezone, timedelta
import os
import logging
import threading
from pathlib import Path
import requests as _requests
from flask import Flask, request, jsonify, send_from_directory

logger = logging.getLogger(__name__)


def _setup_logging():
    """配置日志：写入 run/logs/stime.log，便于排查问题。"""
    log_level = (os.getenv("LOG_LEVEL") or "INFO").upper()
    log_file = os.getenv("LOG_FILE", "run/logs/stime.log")
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
    logger.info("Stime 日志已初始化: 级别=%s, 文件=%s", log_level, log_file)


_setup_logging()

# In-memory current sim time (UTC). 默认初始为 2 年前，便于回测/模拟
_current_time: datetime = datetime.now(timezone.utc) - timedelta(days=730)

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
    """
    Ordered list of tick URLs to POST after each advance.
    Example: [zuilow /api/scheduler/tick, ppt /api/scheduler/tick] -> first zuilow, then PPT 更新净值.
    Env TICK_URLS = comma-separated; else ZUILOW_TICK_URL / web override as single URL.
    """
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
    return int(os.getenv("ZUILOW_TICK_TIMEOUT", "600") or "600") or 600


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
        logger.info("[Set] 仿真时间已设置: %s", now_iso)
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
):
    """Background worker: advance steps_count steps; each step POST to all tick_urls (order: e.g. zuilow then PPT) with X-Simulation-Time. First URL failure aborts; later URL failures are logged."""
    global _advance_tick_state
    one_step = {unit: step_value}
    executed_total = 0
    try:
        if snap_to_boundary and unit == "minutes" and step_value in (5, 15, 30, 60):
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
        logger.info("[Advance+Tick] 开始: %s 步 x %s=%s, tick_urls=%s", steps_count, unit, step_value, tick_urls)
        for i in range(steps_count):
            if _advance_tick_cancel_event.is_set():
                with _advance_tick_lock:
                    _advance_tick_state["cancelled"] = True
                logger.info("[Advance+Tick] 已取消 (step %d/%d)", i + 1, steps_count)
                break
            advance(**one_step)
            sim_now_iso = get_now().strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info("[Advance+Tick] step %d/%d sim_now=%s", i + 1, steps_count, sim_now_iso)
            headers = {"Content-Type": "application/json", "X-Simulation-Time": sim_now_iso}
            webhook_token = os.environ.get("WEBHOOK_TOKEN", "").strip()
            if webhook_token:
                headers["X-Webhook-Token"] = webhook_token
            step_ok = True
            for j, url in enumerate(tick_urls):
                try:
                    r = _requests.post(url, headers=headers, timeout=tick_timeout)
                    logger.info("[Advance+Tick] tick %s -> %d", url, r.status_code)
                except _requests.RequestException as e:
                    err = f"Tick {url} failed: {e}"
                    if j == 0:
                        with _advance_tick_lock:
                            _advance_tick_state["error"] = err
                            _advance_tick_state["steps_done"] = i + 1
                            _advance_tick_state["executed_total"] = executed_total
                            _advance_tick_state["now"] = sim_now_iso
                        logger.warning(err)
                        step_ok = False
                        break
                    logger.warning(err)
                    continue
                if r.ok and j == 0:
                    d = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                    executed_total += d.get("executed", 0)
                elif not r.ok:
                    err_msg = f"Tick {url} HTTP {r.status_code}: {r.text[:200]}"
                    if j == 0:
                        with _advance_tick_lock:
                            _advance_tick_state["error"] = err_msg
                            _advance_tick_state["steps_done"] = i + 1
                            _advance_tick_state["executed_total"] = executed_total
                            _advance_tick_state["now"] = sim_now_iso
                        logger.warning(err_msg)
                        step_ok = False
                        break
                    logger.warning(err_msg)
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
            logger.warning("[Advance+Tick] 结束 (有错误): %s, steps_done=%d/%d", err, done, total)
        elif _advance_tick_state.get("cancelled"):
            logger.info("[Advance+Tick] 结束: 已取消, steps_done=%d/%d", done, total)
        else:
            logger.info("[Advance+Tick] 结束: 完成 %d/%d 步, executed_total=%s", done, total, _advance_tick_state.get("executed_total", 0))


@app.route("/advance-and-tick", methods=["POST"])
def api_advance_and_tick():
    """
    Start advance-by-N + tick in background.
    Each step: advance -> POST each TICK_URLS with X-Simulation-Time (order: e.g. zuilow then PPT).
    Body: {"days": 5} = 5 steps of 1 day each; or {"minutes": 30, "steps": 48} = 48 steps of 30 min each.
    Returns 202 {"status": "started", "steps": N}. Poll GET /advance-and-tick/status for progress.
    POST /advance-and-tick/cancel to cancel. Tick timeout: ZUILOW_TICK_TIMEOUT (default 600s).
    """
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
    with _advance_tick_lock:
        if _advance_tick_state["running"]:
            return jsonify({"error": "advance-and-tick already running", "status": _advance_tick_state}), 409
    logger.info("[Advance+Tick] 已启动后台任务: %d 步 (%s=%s)", steps_count, unit, step_value)
    t = threading.Thread(
        target=_advance_tick_worker,
        args=(unit, step_value, steps_count, tick_urls, tick_timeout, snap_to_boundary),
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
    logger.info("[Advance+Tick] 已请求取消")
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
    port = int(os.getenv("SIM_TIME_PORT", "11185"))
    app.run(host="0.0.0.0", port=port, debug=False)
