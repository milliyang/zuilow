"""
Analytics and simulation config API: full analytics, Sharpe, drawdown, trade stats, position analysis; simulation config.

Used for: PPT web UI analytics and simulation config; analytics from core.analytics, config from core.simulation.

Endpoints:
    GET  /api/analytics           Full analytics (login)
    GET  /api/analytics/sharpe    Sharpe ratio (login)
    GET  /api/analytics/drawdown  Max drawdown (login)
    GET  /api/analytics/trades    Trade stats (login)
    GET  /api/analytics/positions Position analysis (login)
    GET  /api/simulation          Simulation config (login)
    POST /api/simulation/reload   Reload simulation config (admin)
"""
from flask import Blueprint, jsonify, request
from core import db as database
from core import analytics
from core import simulation
from core.utils import get_quotes_batch
from core.auth import admin_required, login_required_api

bp = Blueprint('analytics_api', __name__)


@bp.route('/api/simulation', methods=['GET'])
@login_required_api
def get_simulation_config():
    """Get simulation config status."""
    return jsonify(simulation.get_simulation_status())


@bp.route('/api/simulation/reload', methods=['POST'])
@admin_required
def reload_simulation_config():
    """Reload simulation config file (admin)."""
    simulation.load_config()
    return jsonify({
        'status': 'ok',
        'message': 'Config reloaded',
        'config': simulation.get_simulation_status()
    })


@bp.route('/api/analytics', methods=['GET'])
@login_required_api
def get_analytics():
    """
    Full analytics. Returns sharpe, drawdown, trade_stats, positions.
    Query: realtime=true to fetch quotes from ZuiLow; else use watchlist cache (sim/live same logic).
    """
    account_name = database.get_current_account_name()
    positions = database.get_positions(account_name)

    quotes = {}
    if request.args.get('realtime', 'false').lower() == 'true':
        if positions:
            quotes = get_quotes_batch(list(positions.keys()))
    else:
        watchlist = {w['symbol']: w for w in database.get_watchlist()}
        for symbol in positions.keys():
            if symbol in watchlist and watchlist[symbol].get('last_price'):
                quotes[symbol] = {'price': watchlist[symbol]['last_price']}

    return jsonify(analytics.get_full_analytics(account_name, quotes))


@bp.route('/api/analytics/sharpe', methods=['GET'])
@login_required_api
def get_sharpe():
    """Get Sharpe ratio."""
    account_name = database.get_current_account_name()
    return jsonify(analytics.calc_sharpe_ratio(account_name))


@bp.route('/api/analytics/drawdown', methods=['GET'])
@login_required_api
def get_drawdown():
    """Get max drawdown."""
    account_name = database.get_current_account_name()
    return jsonify(analytics.calc_max_drawdown(account_name))


@bp.route('/api/analytics/trades', methods=['GET'])
@login_required_api
def get_trade_stats():
    """Get trade stats."""
    account_name = database.get_current_account_name()
    return jsonify(analytics.calc_trade_stats(account_name))


@bp.route('/api/analytics/positions', methods=['GET'])
@login_required_api
def get_position_analysis():
    """Get position analysis (uses watchlist cached prices)."""
    account_name = database.get_current_account_name()
    positions = database.get_positions(account_name)

    quotes = {}
    watchlist = {w['symbol']: w for w in database.get_watchlist()}
    for symbol in positions.keys():
        if symbol in watchlist and watchlist[symbol].get('last_price'):
            quotes[symbol] = {'price': watchlist[symbol]['last_price']}

    return jsonify(analytics.calc_position_analysis(account_name, quotes))
