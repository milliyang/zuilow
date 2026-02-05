"""
Webhook API: receive external trading signals; POST /api/webhook; optional X-Simulation-Time header for sim mode.

Used for: zuilow (or other clients) forward orders; when X-Simulation-Time set, order/trade time uses that; optional WEBHOOK_TOKEN.
"""
import os
from datetime import datetime
from flask import Blueprint, jsonify, request
from core import db as database
from core import simulation
from core.utils import normalize_symbol, get_current_datetime_iso, is_sim_mode, get_quote

bp = Blueprint('webhook', __name__)


def _parse_sim_time(header_value: str):
    """Parse X-Simulation-Time header to datetime or None."""
    if not header_value or not header_value.strip():
        return None
    try:
        s = header_value.strip()
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        return datetime.fromisoformat(s)
    except ValueError:
        return None

socketio = None

def init_socketio(sio):
    """Set SocketIO reference (called from app.py)."""
    global socketio
    socketio = sio


@bp.route('/api/webhook', methods=['POST'])
def webhook():
    """
    Receive external trading signal.

    Formats:
    1. Standard: {"symbol": "AAPL", "side": "buy", "qty": 100, "price": 185}
    2. TradingView: {"ticker": "AAPL", "action": "buy", "contracts": 100, "price": 185}
    3. Minimal: {"symbol": "AAPL", "action": "buy"} (default qty/price)

    Optional: account (target account), token (if WEBHOOK_TOKEN set).
    """
    webhook_token = os.getenv('WEBHOOK_TOKEN')
    if webhook_token:
        token = request.headers.get('X-Webhook-Token') or request.json.get('token')
        if token != webhook_token:
            return jsonify({'error': 'Unauthorized'}), 401

    data = request.json
    if not data:
        return jsonify({'error': 'Data required'}), 400

    symbol = normalize_symbol(data.get('symbol') or data.get('ticker') or '')
    side = (data.get('side') or data.get('action') or '').lower()
    qty = int(data.get('qty') or data.get('contracts') or data.get('quantity') or 100)
    price = float(data.get('price') or data.get('limit_price') or 0)

    if side in ['long', 'buy_to_open', 'buy']:
        side = 'buy'
    elif side in ['short', 'sell_to_close', 'sell', 'close']:
        side = 'sell'

    if not symbol:
        return jsonify({'error': 'symbol required'}), 400
    if side not in ['buy', 'sell']:
        return jsonify({'error': f'Invalid side: {side}, need buy/sell'}), 400
    # price <= 0: treat as market order; resolve price from quote (ZuiLow)
    if price <= 0:
        quote = get_quote(symbol)
        if not quote.get('valid', False) or (quote.get('price') or 0) <= 0:
            return jsonify({'error': f'Market order requires quote; {quote.get("error", "no price")}'}), 400
        price = float(quote['price'])

    account_name = data.get('account') or database.get_current_account_name()
    account = database.get_account(account_name)
    if not account:
        return jsonify({'error': f'Account not found: {account_name}'}), 400

    order_time = _parse_sim_time(request.headers.get('X-Simulation-Time', ''))
    if is_sim_mode() and order_time is None:
        order_time = _parse_sim_time(get_current_datetime_iso())

    sim_result = simulation.simulate_execution(symbol, side, qty, price)

    filled_qty = sim_result['filled_qty']
    exec_price = sim_result['exec_price']
    commission = sim_result['commission']
    filled_value = sim_result['filled_value']
    total_cost = sim_result['total_cost']

    if side == 'buy':
        if total_cost > account['cash']:
            return jsonify({
                'error': f'Insufficient cash: need {total_cost:.2f} (incl commission {commission:.2f}), available {account["cash"]:.2f}'
            }), 400

        new_cash = account['cash'] - total_cost
        database.update_account_cash(account_name, new_cash)

        pos = database.get_position(account_name, symbol)
        if pos:
            old_qty = pos['qty']
            old_value = old_qty * pos['avg_price']
            new_qty = old_qty + filled_qty
            new_avg_price = (old_value + filled_value) / new_qty
            database.update_position(account_name, symbol, new_qty, new_avg_price)
        else:
            database.update_position(account_name, symbol, filled_qty, exec_price)

    elif side == 'sell':
        pos = database.get_position(account_name, symbol)
        if not pos:
            return jsonify({'error': f'No position: {symbol}'}), 400

        if pos['qty'] < filled_qty:
            filled_qty = pos['qty']
            filled_value = filled_qty * exec_price
            total_cost = filled_value - commission

        new_qty = pos['qty'] - filled_qty
        database.update_position(account_name, symbol, new_qty, pos['avg_price'])

        new_cash = account['cash'] + total_cost
        database.update_account_cash(account_name, new_cash)

    slippage_cost = (sim_result.get('slippage') or 0) * filled_qty
    realized_pnl = 0.0
    if side == 'sell' and pos is not None:
        realized_pnl = (exec_price - pos['avg_price']) * filled_qty

    status = 'partial' if sim_result['partial_fill'] else 'filled'
    order_id = database.add_order(account_name, symbol, side, filled_qty, exec_price, status, 'webhook', order_time=order_time)
    database.add_trade(account_name, symbol, side, filled_qty, exec_price, order_time=order_time,
                      commission=commission, slippage=slippage_cost, realized_pnl=realized_pnl)
    database.add_to_watchlist(symbol, symbol)
    database.update_watchlist_quote(symbol, exec_price)

    as_of = order_time.date() if order_time else None
    if not is_sim_mode():
        database.update_equity_history(account_name, as_of_date=as_of)

    updated_account = database.get_account(account_name)

    time_str = (order_time.isoformat() if order_time else get_current_datetime_iso())
    order = {
        'id': order_id,
        'symbol': symbol,
        'side': side,
        'requested_qty': qty,
        'filled_qty': filled_qty,
        'requested_price': price,
        'exec_price': exec_price,
        'value': filled_value,
        'time': time_str,
        'status': status,
        'source': 'webhook'
    }

    sim_info = {
        'slippage': sim_result['slippage'],
        'commission': commission,
        'fill_rate': sim_result['fill_rate'],
        'total_cost': total_cost
    }

    if socketio:
        socketio.emit('trade', {**order, 'simulation': sim_info})

    return jsonify({
        'status': 'ok',
        'order': order,
        'simulation': sim_info,
        'account': account_name,
        'cash': round(updated_account['cash'], 2)
    })
