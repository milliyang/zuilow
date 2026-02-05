"""
Account API: list/create/switch/delete accounts, current account, deposit/withdraw/reset, config.

Used for: PPT web UI; total = cash + position value (quote from ZuiLow); sim uses EOD price, live uses current.

Endpoints:
    GET  /api/accounts         List accounts (login)
    POST /api/accounts         Create account (admin)
    POST /api/accounts/switch  Switch account (login)
    DELETE /api/accounts/<name> Delete account (admin)
    GET  /api/account          Get current account (login)
    POST /api/account/deposit  Deposit cash (admin)
    POST /api/account/withdraw Withdraw cash (admin)
    POST /api/account/reset    Reset current account (admin)
    GET  /api/config           Get config (admin)
"""
import os
from flask import Blueprint, jsonify, request
from core import db as database
from core.utils import get_equity_date, get_quotes_batch
from core.auth import admin_required, login_required_api

bp = Blueprint('account', __name__)

DEFAULT_CAPITAL = database.DEFAULT_CAPITAL


def _compute_market_value(account, positions, as_of_date=None):
    """
    Compute position value and total from quotes (ZuiLow). Sim/live same logic.
    as_of_date is not passed to zuilow; zuilow returns EOD in sim, current in live.
    Fallback: watchlist last_price, then cost.
    Returns (position_value, total_value, pnl, pnl_pct).
    """
    cash = float(account['cash'])
    initial = float(account['initial_capital'])
    if not positions:
        total_value = cash
        position_value = 0.0
        pnl = total_value - initial
        pnl_pct = (pnl / initial) * 100 if initial > 0 else 0.0
        return position_value, total_value, pnl, pnl_pct

    symbols = list(positions.keys())
    quotes = get_quotes_batch(symbols)
    watchlist = {w['symbol']: w for w in database.get_watchlist()}
    for sym in symbols:
        q = quotes.get(sym, {})
        if (q.get('price', 0) <= 0 or not q.get('valid', True)) and watchlist.get(sym) and (watchlist[sym].get('last_price') or 0) > 0:
            quotes[sym] = {'symbol': sym, 'price': watchlist[sym]['last_price'], 'valid': True}

    position_value = 0.0
    for sym, pos in positions.items():
        qty = pos['qty']
        avg_price = pos['avg_price']
        use_quote = (
            quotes.get(sym)
            and quotes[sym].get('valid', True)
            and (quotes[sym].get('price') or 0) > 0
        )
        if use_quote:
            position_value += qty * (quotes[sym].get('price') or 0)
        else:
            position_value += qty * avg_price

    total_value = cash + position_value
    pnl = total_value - initial
    pnl_pct = (pnl / initial) * 100 if initial > 0 else 0.0
    return position_value, total_value, pnl, pnl_pct


@bp.route('/api/accounts', methods=['GET'])
@login_required_api
def list_accounts():
    """List all accounts; total/pnl from quotes (sim/live same)."""
    current = database.get_current_account_name()
    as_of_date = get_equity_date()
    accounts = []
    for acc in database.get_all_accounts():
        positions = database.get_positions(acc['name'])
        _, total_value, pnl, pnl_pct = _compute_market_value(acc, positions, as_of_date)
        accounts.append({
            'name': acc['name'],
            'total_value': round(total_value, 2),
            'pnl': round(pnl, 2),
            'pnl_pct': round(pnl_pct, 2),
            'is_current': acc['name'] == current
        })
    return jsonify({'accounts': accounts, 'current': current})


@bp.route('/api/accounts', methods=['POST'])
@admin_required
def create_new_account():
    """Create account (admin)."""
    data = request.json or {}
    name = data.get('name', '').strip()
    capital = float(data.get('capital', DEFAULT_CAPITAL))

    if not name:
        return jsonify({'error': 'Account name required'}), 400
    if database.get_account(name):
        return jsonify({'error': f'Account {name} already exists'}), 400

    as_of = get_equity_date()
    database.create_account(name, capital, as_of_date=as_of)
    database.set_current_account(name)

    return jsonify({'status': 'ok', 'message': f'Account {name} created', 'current': name})


@bp.route('/api/accounts/switch', methods=['POST'])
@login_required_api
def switch_account():
    """Switch current account."""
    data = request.json or {}
    name = data.get('name', '')

    if not database.get_account(name):
        return jsonify({'error': f'Account {name} not found'}), 400

    database.set_current_account(name)

    return jsonify({'status': 'ok', 'current': name})


@bp.route('/api/accounts/<name>', methods=['DELETE'])
@admin_required
def delete_account_api(name):
    """Delete account (admin)."""
    if not database.get_account(name):
        return jsonify({'error': f'Account {name} not found'}), 400

    all_accounts = database.get_all_accounts()
    if len(all_accounts) <= 1:
        return jsonify({'error': 'At least one account required'}), 400

    database.delete_account(name)

    if database.get_current_account_name() == name:
        remaining = [a for a in all_accounts if a['name'] != name]
        if remaining:
            database.set_current_account(remaining[0]['name'])

    return jsonify({'status': 'ok', 'message': f'Account {name} deleted'})


@bp.route('/api/account', methods=['GET'])
@login_required_api
def get_account_api():
    """Get account. Optional query: account=<name>; else current account."""
    account_name = (request.args.get('account') or '').strip() or database.get_current_account_name()
    account = database.get_account(account_name)
    if not account:
        return jsonify({'error': f'Account not found: {account_name}'}), 400
    positions = database.get_positions(account_name)
    as_of_date = get_equity_date()
    position_value, total_value, pnl, pnl_pct = _compute_market_value(account, positions, as_of_date)
    cost_stats = database.get_account_cost_stats(account_name)

    return jsonify({
        'name': account_name,
        'initial_capital': account['initial_capital'],
        'cash': round(account['cash'], 2),
        'position_value': round(position_value, 2),
        'total_value': round(total_value, 2),
        'pnl': round(pnl, 2),
        'pnl_pct': round(pnl_pct, 2),
        'created_at': account['created_at'],
        'cost_stats': {
            'total_commission': round(cost_stats['total_commission'], 2),
            'total_slippage': round(cost_stats['total_slippage'], 2),
            'total_realized_pnl': round(cost_stats['total_realized_pnl'], 2),
        },
    })


@bp.route('/api/account/deposit', methods=['POST'])
@admin_required
def deposit_cash():
    """Deposit cash into current account (admin)."""
    data = request.get_json(silent=True) or {}
    amount = data.get('amount')
    if amount is None:
        return jsonify({'error': 'amount required'}), 400
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({'error': 'amount must be a number'}), 400
    if amount <= 0:
        return jsonify({'error': 'Deposit amount must be > 0'}), 400
    account_name = database.get_current_account_name()
    account = database.get_account(account_name)
    if not account:
        return jsonify({'error': 'Account not found'}), 400
    new_cash = float(account['cash']) + amount
    database.update_account_cash(account_name, new_cash)
    return jsonify({
        'status': 'ok',
        'message': f'Deposited {amount:.2f}',
        'cash': round(new_cash, 2),
    })


@bp.route('/api/account/withdraw', methods=['POST'])
@admin_required
def withdraw_cash():
    """Withdraw cash from current account (admin)."""
    data = request.get_json(silent=True) or {}
    amount = data.get('amount')
    if amount is None:
        return jsonify({'error': 'amount required'}), 400
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({'error': 'amount must be a number'}), 400
    if amount <= 0:
        return jsonify({'error': 'Withdraw amount must be > 0'}), 400
    account_name = database.get_current_account_name()
    account = database.get_account(account_name)
    if not account:
        return jsonify({'error': 'Account not found'}), 400
    cash = float(account['cash'])
    if cash < amount:
        return jsonify({'error': f'Insufficient cash: {cash:.2f}'}), 400
    new_cash = cash - amount
    database.update_account_cash(account_name, new_cash)
    return jsonify({
        'status': 'ok',
        'message': f'Withdrew {amount:.2f}',
        'cash': round(new_cash, 2),
    })


@bp.route('/api/account/reset', methods=['POST'])
@admin_required
def reset_account_api():
    """Reset current account: clear positions/orders/trades/equity history, set cash to initial (admin)."""
    account_name = database.get_current_account_name()
    account = database.get_account(account_name)

    data = request.get_json(silent=True) or {}
    capital = data.get('capital', account['initial_capital'])

    as_of = get_equity_date()
    database.reset_account(account_name, capital, as_of_date=as_of)

    return jsonify({'status': 'ok', 'message': f'Account {account_name} reset, initial capital: {capital}'})


@bp.route('/api/config', methods=['GET'])
@admin_required
def get_config():
    """Get system config (admin)."""
    webhook_token = os.getenv('WEBHOOK_TOKEN', '')

    return jsonify({
        'webhook_token': webhook_token,
        'webhook_token_set': bool(webhook_token)
    })
