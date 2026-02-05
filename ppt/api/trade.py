"""
Trading and quotes API: positions, quote(s), orders, place order, trades, equity history, export CSV.

Used for: PPT web UI and clients; sim/live same logic; place order uses simulation.execute_order.

Endpoints:
    GET  /api/positions       Get positions (login)
    GET  /api/quote/<symbol>  Single quote (login)
    GET  /api/quotes          Batch quotes (login)
    GET  /api/orders          Order history (login)
    POST /api/orders          Place order (admin)
    GET  /api/trades          Trades (login)
    GET  /api/equity          Equity history (login)
    POST /api/equity/update   Update equity (admin)
    GET  /api/export/trades   Export trades CSV (login)
    GET  /api/export/equity   Export equity CSV (login)
"""
import os
from datetime import datetime
from flask import Blueprint, jsonify, request, Response
from core import db as database
from core import simulation
from core.utils import get_quote, get_quotes_batch, normalize_symbol, get_equity_date, get_current_datetime_iso, is_sim_mode
from core.auth import admin_required, login_required_api

bp = Blueprint('trade', __name__)


@bp.route('/api/positions', methods=['GET'])
@login_required_api
def get_positions_api():
    """Get positions. Optional query: account=<name>; else current account."""
    account_name = (request.args.get('account') or '').strip() or database.get_current_account_name()
    if not database.get_account(account_name):
        return jsonify({'error': f'Account not found: {account_name}'}), 400
    db_positions = database.get_positions(account_name)
    realtime = request.args.get('realtime', 'false').lower() == 'true'

    watchlist = {w['symbol']: w for w in database.get_watchlist()}

    positions = []
    total_cost = 0
    total_market_value = 0
    total_pnl = 0

    for symbol, pos in db_positions.items():
        cost = pos['qty'] * pos['avg_price']
        total_cost += cost

        item = {
            'symbol': symbol,
            'qty': pos['qty'],
            'avg_price': round(pos['avg_price'], 2),
            'cost': round(cost, 2),
        }

        current_price = 0
        if realtime:
            quote = get_quote(symbol)
            current_price = quote.get('price', 0) if quote.get('valid', False) else 0
            if current_price > 0:
                if symbol not in watchlist:
                    database.add_to_watchlist(symbol, quote.get('name', symbol))
                database.update_watchlist_quote(symbol, current_price, quote.get('name', symbol))
            elif symbol in watchlist and (watchlist[symbol].get('last_price') or 0) > 0:
                current_price = watchlist[symbol]['last_price']
        else:
            if symbol in watchlist and watchlist[symbol].get('last_price'):
                current_price = watchlist[symbol]['last_price']

        if current_price > 0:
            market_value = pos['qty'] * current_price
            pnl = market_value - cost
            pnl_pct = (pnl / cost) * 100 if cost > 0 else 0

            item.update({
                'current_price': round(current_price, 2),
                'market_value': round(market_value, 2),
                'pnl': round(pnl, 2),
                'pnl_pct': round(pnl_pct, 2),
            })

            total_market_value += market_value
            total_pnl += pnl
        else:
            item['market_value'] = round(cost, 2)
            total_market_value += cost

        positions.append(item)

    result = {'positions': positions}
    if realtime:
        result['summary'] = {
            'total_cost': round(total_cost, 2),
            'total_market_value': round(total_market_value, 2),
            'total_pnl': round(total_pnl, 2),
            'total_pnl_pct': round((total_pnl / total_cost) * 100, 2) if total_cost > 0 else 0
        }

    return jsonify(result)


@bp.route('/api/quote/<symbol>', methods=['GET'])
@login_required_api
def get_symbol_quote(symbol):
    """Get single symbol quote."""
    quote = get_quote(normalize_symbol(symbol))
    return jsonify(quote)


@bp.route('/api/quotes', methods=['GET'])
@login_required_api
def get_multi_quotes():
    """Get batch quotes."""
    raw_symbols = request.args.get('symbols', '').split(',')
    symbols = [normalize_symbol(s) for s in raw_symbols if s.strip()]
    if not symbols:
        return jsonify({'error': 'symbols required'}), 400

    quotes = get_quotes_batch(symbols)
    return jsonify({'quotes': quotes})


@bp.route('/api/orders', methods=['GET'])
@login_required_api
def get_orders_api():
    """Get order history. Optional query: account=<name>; else current account."""
    account_name = (request.args.get('account') or '').strip() or database.get_current_account_name()
    if not database.get_account(account_name):
        return jsonify({'error': f'Account not found: {account_name}'}), 400
    limit = min(max(int(request.args.get('limit', 50)), 1), 200)
    orders = database.get_orders(account_name, limit=limit)
    return jsonify({'orders': orders})


@bp.route('/api/orders', methods=['POST'])
@admin_required
def place_order():
    """Place order (admin; simulation: slippage, commission, partial fill)."""
    data = request.json
    if not data:
        return jsonify({'error': 'Order data required'}), 400

    symbol = normalize_symbol(data.get('symbol', ''))
    side = data.get('side', '').lower()
    qty = int(data.get('qty', 0))
    price = float(data.get('price', 0))

    if not all([symbol, side in ['buy', 'sell'], qty > 0, price > 0]):
        return jsonify({'error': 'Invalid: symbol, side(buy/sell), qty, price'}), 400

    account_name = database.get_current_account_name()
    account = database.get_account(account_name)

    order_time = None
    sim_header = request.headers.get('X-Simulation-Time', '').strip()
    if sim_header:
        try:
            s = sim_header.replace('Z', '+00:00') if sim_header.endswith('Z') else sim_header
            order_time = datetime.fromisoformat(s)
        except ValueError:
            pass
    if is_sim_mode() and order_time is None:
        try:
            s = get_current_datetime_iso().replace('Z', '+00:00')
            order_time = datetime.fromisoformat(s)
        except (ValueError, TypeError):
            pass

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
        if not pos or pos['qty'] < filled_qty:
            return jsonify({'error': f'Insufficient position: {symbol}'}), 400

        new_qty = pos['qty'] - filled_qty
        database.update_position(account_name, symbol, new_qty, pos['avg_price'])

        new_cash = account['cash'] + total_cost
        database.update_account_cash(account_name, new_cash)

    slippage_cost = (sim_result.get('slippage') or 0) * filled_qty
    realized_pnl = 0.0
    if side == 'sell' and pos is not None:
        realized_pnl = (exec_price - pos['avg_price']) * filled_qty

    status = 'partial' if sim_result['partial_fill'] else 'filled'
    order_id = database.add_order(account_name, symbol, side, filled_qty, exec_price, status, 'web', order_time=order_time)
    database.add_trade(account_name, symbol, side, filled_qty, exec_price, order_time=order_time,
                      commission=commission, slippage=slippage_cost, realized_pnl=realized_pnl)
    database.add_to_watchlist(symbol, symbol)
    database.update_watchlist_quote(symbol, exec_price)

    as_of = order_time.date() if order_time else None
    if not is_sim_mode():
        database.update_equity_history(account_name, as_of_date=as_of)

    updated_account = database.get_account(account_name)

    time_str = (order_time.isoformat() if order_time else get_current_datetime_iso())
    return jsonify({
        'status': 'ok',
        'order': {
            'id': order_id,
            'symbol': symbol,
            'side': side,
            'requested_qty': qty,
            'filled_qty': filled_qty,
            'requested_price': price,
            'exec_price': exec_price,
            'value': filled_value,
            'time': time_str,
            'status': status
        },
        'simulation': {
            'slippage': sim_result['slippage'],
            'commission': commission,
            'fill_rate': sim_result['fill_rate'],
            'total_cost': total_cost
        },
        'cash': round(updated_account['cash'], 2)
    })


@bp.route('/api/trades', methods=['GET'])
@login_required_api
def get_trades_api():
    """Get trades. Optional query: account=<name>; else current account."""
    account_name = (request.args.get('account') or '').strip() or database.get_current_account_name()
    if not database.get_account(account_name):
        return jsonify({'error': f'Account not found: {account_name}'}), 400
    limit = min(max(int(request.args.get('limit', 100)), 1), 500)
    trades = database.get_trades(account_name, limit=limit)
    return jsonify({'trades': trades})


@bp.route('/api/equity', methods=['GET'])
@login_required_api
def get_equity_history_api():
    """Get equity history."""
    account_name = database.get_current_account_name()
    account = database.get_account(account_name)
    history = database.get_equity_history(account_name)
    return jsonify({
        'history': history,
        'initial_capital': account['initial_capital']
    })


@bp.route('/api/equity/update', methods=['POST'])
@admin_required
def update_equity_with_market_price():
    """Update today equity with market price (admin). In sim mode use stime date (get_equity_date() fetches if needed)."""
    as_of_date = get_equity_date()
    results = []
    failed_symbols = []

    for acc in database.get_all_accounts():
        account_name = acc['name']
        positions = database.get_positions(account_name)

        if not positions:
            database.update_equity_history(account_name, as_of_date=as_of_date)
            results.append({'account': account_name, 'status': 'ok', 'positions': 0})
            continue

        symbols = list(positions.keys())
        quotes = get_quotes_batch(symbols)
        watchlist = {w['symbol']: w for w in database.get_watchlist()}
        for symbol in symbols:
            q = quotes.get(symbol, {})
            if (q.get('price', 0) <= 0 or not q.get('valid', True)) and watchlist.get(symbol) and (watchlist[symbol].get('last_price') or 0) > 0:
                quotes[symbol] = {'symbol': symbol, 'price': watchlist[symbol]['last_price'], 'valid': True}
            if (quotes.get(symbol) or {}).get('price', 0) <= 0 or not (quotes.get(symbol) or {}).get('valid', True):
                failed_symbols.append(symbol)

        database.update_equity_history(account_name, quotes=quotes, as_of_date=as_of_date)

        results.append({
            'account': account_name,
            'status': 'ok',
            'positions': len(positions),
            'quote_failed': [s for s in symbols if s in failed_symbols]
        })

    return jsonify({
        'message': f'Updated {len(results)} accounts',
        'results': results,
        'failed_symbols': list(set(failed_symbols)),
        'tip': 'Failed symbols use cost price'
    })


@bp.route('/api/export/trades', methods=['GET'])
@login_required_api
def export_trades_csv():
    """Export trades CSV."""
    account_name = database.get_current_account_name()
    trades = database.get_trades(account_name, limit=10000)

    lines = ['time,symbol,side,qty,price,value']

    for t in trades:
        lines.append(f"{t['time']},{t['symbol']},{t['side']},{t['qty']},{t['price']:.2f},{t['value']:.2f}")

    csv_content = '\n'.join(lines)

    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment;filename=trades_{account_name}_{get_equity_date().strftime("%Y%m%d")}.csv'}
    )


@bp.route('/api/export/equity', methods=['GET'])
@login_required_api
def export_equity_csv():
    """Export equity history CSV."""
    account_name = database.get_current_account_name()
    history = database.get_equity_history(account_name)

    lines = ['date,equity,pnl,pnl_pct']

    for h in history:
        lines.append(f"{h['date']},{h['equity']},{h['pnl']},{h['pnl_pct']}")

    csv_content = '\n'.join(lines)

    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment;filename=equity_{account_name}_{get_equity_date().strftime("%Y%m%d")}.csv'}
    )
