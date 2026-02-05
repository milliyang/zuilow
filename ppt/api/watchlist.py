"""
Watchlist API: get/add/remove symbols, refresh quotes from ZuiLow; admin only; on refresh failure keep last_price.

Used for: PPT web UI watchlist; quotes from ZuiLow (sim/live same logic).

Endpoints:
    GET    /api/watchlist          Get watchlist (admin)
    POST   /api/watchlist          Add symbol (admin)
    DELETE /api/watchlist/<symbol> Remove symbol (admin)
    POST   /api/watchlist/refresh  Refresh quotes from ZuiLow (admin)
    GET    /api/watchlist/test     Test ZuiLow quote service (admin)
    POST   /api/watchlist/clear    Clear watchlist (admin)
    POST   /api/watchlist/init     Init default watchlist (admin)
"""
import time
from flask import Blueprint, jsonify, request
from core import db as database
from core.utils import get_quote, normalize_symbol
from core.auth import admin_required

bp = Blueprint('watchlist', __name__)


@bp.route('/api/watchlist', methods=['GET'])
@admin_required
def get_watchlist():
    """Get watchlist."""
    watchlist = database.get_watchlist()
    return jsonify({'watchlist': watchlist})


@bp.route('/api/watchlist', methods=['POST'])
@admin_required
def add_watchlist():
    """Add symbol to watchlist."""
    data = request.json or {}
    symbol = data.get('symbol', '').strip()

    if not symbol:
        return jsonify({'error': 'symbol required'}), 400

    symbol = normalize_symbol(symbol)
    name = data.get('name', symbol)

    if database.add_to_watchlist(symbol, name):
        return jsonify({'status': 'ok', 'symbol': symbol})
    else:
        return jsonify({'error': f'{symbol} already in watchlist'}), 400


@bp.route('/api/watchlist/<symbol>', methods=['DELETE'])
@admin_required
def remove_watchlist(symbol):
    """Remove symbol from watchlist."""
    symbol = normalize_symbol(symbol)
    if database.remove_from_watchlist(symbol):
        return jsonify({'status': 'ok', 'symbol': symbol})
    else:
        return jsonify({'error': f'{symbol} not in watchlist'}), 400


@bp.route('/api/watchlist/refresh', methods=['POST'])
@admin_required
def refresh_watchlist():
    """Refresh watchlist quotes from ZuiLow (sim/live same). On failure keep last_price."""
    watchlist = database.get_watchlist()

    if not watchlist:
        return jsonify({'message': 'Watchlist empty', 'results': []})

    results = []
    ok_count = 0
    fail_count = 0

    for item in watchlist:
        symbol = item['symbol']
        try:
            quote = get_quote(symbol)

            if quote.get('valid', False) and quote.get('price', 0) > 0:
                database.update_watchlist_quote(
                    symbol,
                    quote['price'],
                    name=quote.get('name'),
                    status='ok'
                )
                results.append({
                    'symbol': symbol,
                    'status': 'ok',
                    'price': quote['price'],
                    'name': quote.get('name', symbol)
                })
                ok_count += 1
            else:
                error = quote.get('error', 'No quote')
                database.update_watchlist_quote(
                    symbol, item.get('last_price') or 0, status='error', error=error
                )
                results.append({
                    'symbol': symbol,
                    'status': 'error',
                    'error': error
                })
                fail_count += 1
        except Exception as e:
            database.update_watchlist_quote(
                symbol, item.get('last_price') or 0, status='error', error=str(e)
            )
            results.append({
                'symbol': symbol,
                'status': 'error',
                'error': str(e)
            })
            fail_count += 1

    return jsonify({
        'message': f'Refresh done: {ok_count} ok, {fail_count} fail (quotes from ZuiLow)',
        'ok': ok_count,
        'fail': fail_count,
        'results': results
    })


@bp.route('/api/watchlist/test', methods=['GET'])
@admin_required
def test_quote_service():
    """Test ZuiLow quote service (AAPL); sim/live same logic."""
    start = time.time()

    try:
        quote = get_quote('AAPL')
        elapsed = round((time.time() - start) * 1000)

        if quote.get('valid', False) and quote.get('price', 0) > 0:
            return jsonify({
                'status': 'ok',
                'message': 'ZuiLow quote service ok',
                'test_symbol': 'AAPL',
                'price': quote['price'],
                'latency_ms': elapsed
            })
        else:
            err = quote.get('error') or 'invalid or empty data'
            hint = ' Set DMS_BASE_URL and ensure DMS is reachable.' if (quote.get('error') == 'DMS_BASE_URL not set') else ''
            return jsonify({
                'status': 'error',
                'message': f'DMS quote failed: {err}.{hint}',
                'error': quote.get('error'),
                'latency_ms': elapsed
            })
    except Exception as e:
        elapsed = round((time.time() - start) * 1000)
        return jsonify({
            'status': 'error',
            'message': f'DMS quote service error: {str(e)}',
            'latency_ms': elapsed
        })


@bp.route('/api/watchlist/clear', methods=['POST'])
@admin_required
def clear_watchlist():
    """Clear watchlist."""
    database.clear_watchlist()
    return jsonify({'status': 'ok', 'message': 'Watchlist cleared'})


@bp.route('/api/watchlist/init', methods=['POST'])
@admin_required
def init_default_watchlist():
    """Init default watchlist."""
    result = database.init_default_watchlist()
    return jsonify({
        'status': 'ok',
        'message': f'Added {len(result["added"])}, skipped {len(result["skipped"])} existing',
        'added': result['added'],
        'skipped': result['skipped']
    })
