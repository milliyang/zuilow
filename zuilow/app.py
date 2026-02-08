"""
ZuiLow web entry (Flask).

Starts the web app. Runtime data under ./run/: logs (run/logs/), scheduler history
(run/db/scheduler_history.db), signals (run/db/signals.db). Project root is zuilow;
parent dir must be in PYTHONPATH (set by start_zuilow script). Loads .env via dotenv.
Registers web Blueprint; login/logout; static files from web/static. Scheduler
instance set via set_scheduler(Scheduler()).

Run: python app.py or ./start_zuilow.sh / start_zuilow.ps1
Env: HOST (default 0.0.0.0), PORT (default 11180), LOG_LEVEL (default INFO), SECRET_KEY
"""

import sys
import os
import logging
import threading
from pathlib import Path

# Project root is zuilow; parent dir must be in PYTHONPATH for import zuilow (set by start_zuilow script)
_root = Path(__file__).resolve().parent
if _root.name == "zuilow" and str(_root.parent) not in sys.path:
    sys.path.insert(0, str(_root.parent))

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect, send_from_directory
from flask_cors import CORS
from flask_login import login_user, logout_user, current_user

# Log dir: ./run/logs
LOG_DIR = _root / "run" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "zuilow.log"


def setup_logging():
    """Configure logging to run/logs."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level, logging.INFO))
    root.handlers.clear()
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(getattr(logging, log_level, logging.INFO))
    fh.setFormatter(fmt)
    root.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, log_level, logging.INFO))
    ch.setFormatter(fmt)
    root.addHandler(ch)


setup_logging()
logging.getLogger("werkzeug").setLevel(logging.INFO)
logging.getLogger("numexpr.utils").setLevel(logging.WARNING)  # Reduce NumExpr thread messages
logger = logging.getLogger(__name__)

app = Flask(__name__)
# SECRET_KEY signs session cookie; if unset, a random value is used per run and old cookies expire after restart. Prefer a fixed value in .env (see env.example).
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY") or os.urandom(24).hex()
# Different cookie name so zuilow and ppt can run in same browser without logging each other out
app.config["SESSION_COOKIE_NAME"] = "zuilow_session"
CORS(app, supports_credentials=True)

# Auth (ref sai/ppt): admin accounts only
from zuilow.web.auth import init_login_manager, authenticate
init_login_manager(app)

# Register Web routes (Blueprint)
from zuilow.web.routes import bp as web_bp
from zuilow.web.app import set_scheduler, get_scheduler, get_ppt_broker
import zuilow.components.control.ctrl as ctrl
from zuilow.components.scheduler import Scheduler

app.register_blueprint(web_bp)

# Login / logout (same as PPT)
STATIC_DIR = _root / "web" / "static"


@app.route("/login")
def login_page():
    if current_user.is_authenticated:
        return redirect("/")
    return send_from_directory(STATIC_DIR, "login.html")


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password", "")
    user = authenticate(username, password)
    if user:
        login_user(user)
        return jsonify({"status": "ok", "user": {"username": user.username, "role": user.role}})
    return jsonify({"error": "Invalid username or password"}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    logout_user()
    return jsonify({"status": "ok"})


@app.route("/api/user")
def api_user():
    if current_user.is_authenticated:
        return jsonify({
            "authenticated": True,
            "username": current_user.username,
            "role": current_user.role,
        })
    return jsonify({"authenticated": False})

# Scheduler instance (loaded from config/scheduler.yaml; start/stop via API).
# Current time: ctrl (stime set via tick, or ctrl fetches from stime).
set_scheduler(Scheduler())


def _auto_connect_ppt():
    """Trigger PPT broker connect once on startup (Paper Trade is usually always present)."""
    try:
        broker = get_ppt_broker()
        if broker and (broker.config.base_url or "").strip():
            broker.connect()
            logger.info("PPT broker auto-connect: %s", "connected" if broker.is_connected else "unreachable")
    except Exception as e:
        logger.debug("PPT broker auto-connect: %s", e)


_thread_ppt = threading.Thread(target=_auto_connect_ppt, daemon=True)
_thread_ppt.start()

# Use DataSourceManager (DMS primary, yfinance fallback) for scheduler market data instead of self-HTTP
# to avoid timeout when /api/market/quote is slow and so DMS is used directly
_sched = get_scheduler()
if _sched and getattr(_sched, "runner", None):
    try:
        from zuilow.components.datasource import get_manager
        def _runner_quote_fn(sym):
            mgr = get_manager()
            if not mgr:
                return None
            t = ctrl.get_tick_sim_time()
            try:
                return mgr.get_quote(sym, as_of=t) if t is not None else mgr.get_quote(sym)
            except TypeError:
                return mgr.get_quote(sym)
        _sched.runner.set_replay_providers(quote_fn=_runner_quote_fn)
        logger.info("Scheduler runner: using DataSourceManager for market data (DMS primary)")
    except Exception as _e:
        logger.debug("Scheduler runner quote_provider not set: %s", _e)

# Static files
if STATIC_DIR.exists():
    app.static_folder = str(STATIC_DIR)
    app.static_url_path = "/static"


if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("ZUILOW_PORT", "11180"))
    logger.info("Starting ZuiLow Web Service on %s:%s", host, port)
    # threaded=True: parallel requests so CSS/JS/icons do not queue
    app.run(host=host, port=port, debug=False, threaded=True)
