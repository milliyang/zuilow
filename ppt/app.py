"""
Paper Trade: simulated trading platform; Flask app, Blueprint registration, static routes, WebSocket, scheduler, auth.

Used for: standalone HTTP service; zuilow forwards orders via webhook; sim mode uses Simulation Time Service.

Entry:
    python app.py or flask run: setup_logging(), load env (LOG_LEVEL, DB_FILE, WEBHOOK_TOKEN, SIMULATION_TIME_URL), register Blueprints, SocketIO, scheduler. Port from env (default 11182).

Features:
    - Blueprints: api/account, trade, watchlist, webhook, analytics, opents; static; WebSocket; scheduler; auth (Flask-Login, WEBHOOK_TOKEN)
"""
import os
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, request, redirect, url_for
from flask_cors import CORS
from flask_socketio import SocketIO
from flask_login import login_user, logout_user, login_required, current_user
from dotenv import load_dotenv
from core import utils as core_utils

# Load environment variables
load_dotenv()

# ============================================================
# Logging
# ============================================================
def setup_logging():
    """Configure logging (file + console)."""
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_file = os.getenv('LOG_FILE', 'run/logs/paper_trade.log')
    
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))
    root_logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(getattr(logging, log_level, logging.INFO))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    logging.info("Logging initialized: level=%s file=%s", log_level, log_file)

#
setup_logging()

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY') or os.urandom(24).hex()
# Different cookie name so zuilow and ppt can run in same browser without logging each other out
app.config['SESSION_COOKIE_NAME'] = 'ppt_session'

# Initialize extensions
CORS(app, supports_credentials=True)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Static files
STATIC_DIR = Path(__file__).parent / 'static'

# ============================================================
# Import Core Modules
# ============================================================

from core import db as database
from core.utils import get_quotes_batch
from core.auth import init_login_manager, authenticate

init_login_manager(app)

# ============================================================
# Register API Blueprints
# ============================================================

from api import all_blueprints
from api import webhook

webhook.init_socketio(socketio)

#
for bp in all_blueprints:
    app.register_blueprint(bp)


# ============================================================
# Equity update (shared)
# ============================================================

def _update_all_accounts_equity():
    """Update equity for all accounts. Uses get_equity_date() (sim or real). Skips dates before account first day."""
    dms_base = (os.getenv('DMS_BASE_URL') or '').strip().rstrip('/')
    date_for_db = core_utils.get_equity_date()
    date_str = date_for_db.isoformat()
    watchlist = {w['symbol']: w for w in database.get_watchlist()}
    for acc in database.get_all_accounts():
        min_date = database.get_min_equity_date(acc['name'])
        if min_date and date_str < min_date:
            logging.debug("[Tick] skip account=%s as_of=%s before first day %s", acc['name'], date_str, min_date)
            continue
        positions = database.get_positions(acc['name'])
        symbols = list(positions.keys()) if positions else []
        if not symbols:
            continue
        quotes = get_quotes_batch(symbols) if dms_base else {}
        for sym in symbols:
            q = quotes.get(sym, {})
            if (q.get('price', 0) <= 0 or not q.get('valid', True)) and watchlist.get(sym) and (watchlist[sym].get('last_price') or 0) > 0:
                quotes[sym] = {'symbol': sym, 'price': watchlist[sym]['last_price'], 'valid': True}
            q2 = quotes.get(sym, {})
            logging.info("[Tick] quote result: account=%s date=%s symbol=%s price=%s valid=%s error=%s",
                         acc['name'], date_str, sym, q2.get('price'), q2.get('valid', True), q2.get('error'))
        if quotes:
            database.update_equity_history(acc['name'], quotes=quotes, as_of_date=date_for_db)
        else:
            database.update_equity_history(acc['name'], as_of_date=date_for_db)


# ============================================================
# Scheduler: only register jobs in real time; no jobs in sim mode
# ============================================================

def setup_scheduler():
    """In sim mode register no jobs (equity driven by tick). In real time register equity Cron + OTS Cron."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logging.warning("[Scheduler] APScheduler not installed, skipping. Install: pip install apscheduler")
        return None

    if core_utils.is_sim_mode():
        logging.debug("[Scheduler] Sim mode: no jobs registered; equity only via /api/scheduler/tick")
        return None

    scheduler = BackgroundScheduler()
    
    # Real time: equity Cron
    schedule_times = os.getenv('EQUITY_UPDATE_SCHEDULE', '5:0,21:30,0:0')
    if schedule_times and schedule_times.lower() != 'off':
        def _job_equity():
            logging.info("[Scheduler] Starting equity update %s", core_utils.get_current_datetime_iso())
            try:
                _update_all_accounts_equity()
                logging.info("[Scheduler] Equity update done")
            except Exception as e:
                logging.exception("[Scheduler] Equity update failed: %s", e)
        for time_str in schedule_times.split(','):
            time_str = time_str.strip()
            if ':' in time_str:
                hour, minute = time_str.split(':')
                scheduler.add_job(_job_equity, CronTrigger(hour=int(hour), minute=int(minute)), id=f'equity_update_{hour}_{minute}')
                logging.info("[Scheduler] Added equity job: %s:%s", hour, minute)
    else:
        logging.info("[Scheduler] Equity update disabled (EQUITY_UPDATE_SCHEDULE=off)")

    # Real time: OTS timestamp Cron
    ots_schedule = os.getenv('OTS_TIMESTAMP_SCHEDULE', '16:0')
    if ots_schedule and ots_schedule.lower() != 'off':
        def _create_daily_ots(label=None):
            def _job():
                logging.info("[Scheduler] Creating daily timestamp %s, label=%s", core_utils.get_current_datetime_iso(), label)
                try:
                    from opents import service
                    result = service.create_daily_timestamp(label=label)
                    if result.get('success'):
                        logging.info("[Scheduler] Timestamp created: %s, label=%s", result.get('date'), label)
                    else:
                        logging.warning("[Scheduler] Timestamp failed: %s", result.get('error'))
                except Exception as e:
                    logging.exception("[Scheduler] Timestamp error: %s", e)
            return _job
        for item in [x.strip() for x in ots_schedule.split(',')]:
            parts = item.split(':')
            if len(parts) >= 2:
                hour, minute = int(parts[0]), int(parts[1])
                label = parts[2] if len(parts) >= 3 else None
                job_id = f'ots_timestamp_{hour}_{minute}' + (f'_{label}' if label else '')
                scheduler.add_job(_create_daily_ots(label=label), CronTrigger(hour=hour, minute=minute), id=job_id)
                logging.info("[Scheduler] Added OTS job: %s:%s%s", hour, minute, f" (label: {label})" if label else "")
    else:
        logging.info("[Scheduler] OTS timestamp disabled (OTS_TIMESTAMP_SCHEDULE=off)")

    scheduler.start()
    logging.info("[Scheduler] Scheduler started")
    return scheduler

_scheduler = setup_scheduler()

# ============================================================
# Login / Logout routes
# ============================================================

@app.route('/login')
def login_page():
    """Login page"""
    if current_user.is_authenticated:
        return redirect('/')
    return send_from_directory(STATIC_DIR, 'login.html')


@app.route('/api/login', methods=['POST'])
def api_login():
    """Login API"""
    data = request.json or {}
    username = data.get('username', '')
    password = data.get('password', '')
    
    user = authenticate(username, password)
    if user:
        login_user(user)
        return jsonify({
            'status': 'ok',
            'user': {'username': user.username, 'role': user.role}
        })
    return jsonify({'error': 'Invalid username or password'}), 401


@app.route('/api/logout', methods=['POST'])
def api_logout():
    """Logout API"""
    logout_user()
    return jsonify({'status': 'ok'})


@app.route('/api/user')
def api_user():
    """Get current user info"""
    if current_user.is_authenticated:
        return jsonify({
            'authenticated': True,
            'username': current_user.username,
            'role': current_user.role
        })
    return jsonify({'authenticated': False})


# ============================================================
# Static Pages & Health Check
# ============================================================

@app.route('/')
@login_required
def index():
    """Dashboard (login required)"""
    return send_from_directory(STATIC_DIR, 'index.html')


@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory(STATIC_DIR, filename)


@app.route('/watchlist')
@login_required
def watchlist_page():
    """Watchlist page (admin only)"""
    if not current_user.is_admin:
        return redirect(url_for('index'))
    return send_from_directory(STATIC_DIR, 'watchlist.html')


@app.route('/cash')
@login_required
def cash_page():
    """Cash operations: deposit / withdraw / reset account (admin only)."""
    if not current_user.is_admin:
        return redirect(url_for('index'))
    return send_from_directory(STATIC_DIR, 'cash.html')


@app.route('/ots')
@login_required
def ots_page():
    """OpenTimestamps page (any logged-in user)"""
    return send_from_directory(STATIC_DIR, 'ots.html')


@app.route('/api/theme')
def api_theme():
    """Public client config: theme for UI. theme=simulate when SIMULATION_MODE or SIMULATION_TIME_URL set (red theme)."""
    theme = 'simulate' if core_utils.is_sim_mode() else 'default'
    return jsonify({'theme': theme})


# Tick equity: one update per as_of_date (avoid duplicate updates on every tick)
_tick_equity_done_dates: set = set()


@app.route('/api/sim_now')
def api_sim_now():
    """Return current time (ISO). Via ctrl: sim = tick or fetch stime; real = now UTC."""
    from core import utils as core_utils
    now_iso = core_utils.get_sim_now_iso()
    return jsonify({'now': now_iso}), 200


@app.route('/api/scheduler/tick', methods=['POST'])
def api_scheduler_tick():
    """
    Sim tick: called by stime after advance; updates all account equity by sim time.
    Header X-Simulation-Time: ISO time (same as webhook); if missing, try stime GET /now.
    When DMS_BASE_URL is set, quotes from DMS (last bar). Optional auth: WEBHOOK_TOKEN / X-Webhook-Token.
    """
    try:
        if not core_utils.is_sim_mode():
            return jsonify({'error': 'Not in simulation mode'}), 400

        webhook_token = os.getenv('WEBHOOK_TOKEN')
        if webhook_token:
            token = request.headers.get('X-Webhook-Token') or (request.get_json(silent=True) or {}).get('token')
            if token != webhook_token:
                return jsonify({'error': 'Unauthorized'}), 401

        sim_header = (request.headers.get('X-Simulation-Time') or '').strip()
        if sim_header:
            core_utils.set_sim_now_iso(sim_header)
        # else: get_current_datetime_iso() uses ctrl, which fetches stime when tick context is empty

        now_iso = core_utils.get_current_datetime_iso()
        as_of_date = datetime.fromisoformat(now_iso.replace('Z', '+00:00')).date()
        date_str = as_of_date.isoformat()
        global _tick_equity_done_dates
        if not _tick_equity_done_dates:
            _tick_equity_done_dates = set(database.get_equity_history_dates())
        if date_str in _tick_equity_done_dates:
            logging.debug("[Tick] as_of_date=%s already updated, skip", date_str)
            out = {'ok': True, 'as_of_date': date_str, 'skipped': True, 'as_of': now_iso}
            return jsonify(out)
        logging.info("[Tick] POST /api/scheduler/tick as_of=%s", now_iso)
        _update_all_accounts_equity()
        _tick_equity_done_dates.add(date_str)
        logging.info("[Tick] equity updated as_of=%s", now_iso)
        out = {'ok': True, 'as_of_date': date_str, 'as_of': now_iso}
        return jsonify(out)
    except Exception as e:
        logging.exception("scheduler/tick failed: %s", e)
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'paper-trade',
        'version': '2.0.0'
    })


@app.route('/api/info')
def info():
    """API information"""
    return jsonify({
        'name': 'Paper Trade API',
        'version': '2.0.0',
        'endpoints': {
            '/': 'Dashboard',
            '/watchlist': 'Watchlist page',
            '/cash': 'Cash ops (deposit/withdraw/reset, admin)',
            '/api/health': 'Health check',
            '/api/accounts': 'GET - list accounts / POST - create account',
            '/api/accounts/switch': 'POST - switch account',
            '/api/account': 'GET - current account',
            '/api/account/reset': 'POST - reset current account',
            '/api/positions': 'GET - positions',
            '/api/orders': 'GET - orders / POST - place order',
            '/api/trades': 'GET - trades',
            '/api/equity': 'GET - equity history',
            '/api/scheduler/tick': 'POST - sim tick (stime, X-Simulation-Time)',
            '/api/watchlist': 'Watchlist',
            '/api/analytics': 'Analytics',
            '/api/simulation': 'Simulation config',
            '/api/webhook': 'POST - Webhook',
            '/api/ots/history': 'GET - OTS history',
            '/api/ots/detail/<date>': 'GET - OTS detail by date',
            '/api/ots/record/<date>': 'GET - OTS record file',
            '/api/ots/proof/<date>': 'GET - OTS proof file',
            '/api/ots/create': 'POST - create OTS (admin)',
            '/api/ots/verify/<date>': 'POST - verify OTS (admin)',
            '/api/ots/info': 'GET - OTS info',
        }
    })


# ============================================================
# WebSocket Events
# ============================================================

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')


@socketio.on('subscribe')
def handle_subscribe(data):
    """Handle market data subscription"""
    symbol = data.get('symbol')
    print(f'Subscribed to: {symbol}')
    socketio.emit('subscribed', {'symbol': symbol, 'status': 'ok'})


# ============================================================
# Main Entry
# ============================================================

if __name__ == '__main__':
    port = int(os.getenv('PORT', 11182))
    debug = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    
    database.init_db()
    
    print(f'\n=== Paper Trade Server ===')
    print(f'Dashboard: http://localhost:{port}/')
    print(f'Watchlist: http://localhost:{port}/watchlist')
    print(f'Cash ops: http://localhost:{port}/cash')
    print(f'API Health: http://localhost:{port}/api/health')
    print(f'Debug: {debug}')
    print(f'==========================\n')
    
    socketio.run(app, host='0.0.0.0', port=port, debug=debug)
