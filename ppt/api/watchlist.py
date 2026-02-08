"""
Watchlist API: get/add/remove symbols, refresh quotes from DMS (batch); admin only; on refresh failure keep last_price.

Used for: PPT web UI watchlist. When DMS_BASE_URL is set, quotes from DMS via get_quotes_batch (one read/batch).

Endpoints:
    GET    /api/watchlist          Get watchlist (admin)
    POST   /api/watchlist          Add symbol (admin)
    DELETE /api/watchlist/<symbol> Remove symbol (admin)
    POST   /api/watchlist/refresh  Refresh quotes from DMS batch (admin)
    POST   /api/watchlist/batch-names  Batch update display names only (admin)
    POST   /api/watchlist/refresh-names-from-positions  Update watchlist name from DMS for each position symbol (admin)
    GET    /api/watchlist/test     Test DMS quote service (admin)
    POST   /api/watchlist/clear    Clear watchlist (admin)
    POST   /api/watchlist/init     Init default watchlist (admin)
"""
import time
from flask import Blueprint, jsonify, request
from core import db as database
from core.utils import get_quote, get_quotes_batch, normalize_symbol
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
    """Refresh watchlist quotes from DMS (one get_quotes_batch). On failure keep last_price.
    Body: optional as_of_iso (ISO datetime) to price as of that time; when omitted, sim mode uses current sim time."""
    watchlist = database.get_watchlist()

    if not watchlist:
        return jsonify({'message': 'Watchlist empty', 'results': []})

    symbols = [item['symbol'] for item in watchlist]
    by_symbol = {item['symbol']: item for item in watchlist}

    data = request.get_json(silent=True) or {}
    as_of_iso = (data.get('as_of_iso') or data.get('as_of') or '').strip() or None

    quotes = get_quotes_batch(symbols, as_of_iso=as_of_iso)

    results = []
    ok_count = 0
    fail_count = 0

    for symbol in symbols:
        quote = quotes.get(symbol, {})
        item = by_symbol.get(symbol, {})
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

    return jsonify({
        'message': f'Refresh done: {ok_count} ok, {fail_count} fail (DMS batch)',
        'ok': ok_count,
        'fail': fail_count,
        'results': results
    })


@bp.route('/api/watchlist/batch-names', methods=['POST'])
@admin_required
def batch_update_names():
    """Batch update display names only. Body: { \"updates\": [ {\"symbol\": \"US.AAPL\", \"name\": \"Apple Inc\"}, ... ] }. Does not add symbols; only updates name for existing watchlist rows."""
    data = request.get_json(silent=True) or {}
    updates = data.get('updates')
    if not isinstance(updates, list):
        return jsonify({'error': 'updates must be a list of {symbol, name}'}), 400
    updated = 0
    errors = []
    for item in updates:
        symbol = (item.get('symbol') or '').strip()
        name = (item.get('name') or '').strip()
        if not symbol:
            continue
        symbol = normalize_symbol(symbol)
        if database.update_watchlist_name(symbol, name):
            updated += 1
        # else: symbol not in watchlist, skip (no error)
    return jsonify({'updated': updated, 'errors': errors})


@bp.route('/api/watchlist/refresh-names-from-positions', methods=['POST'])
@admin_required
def refresh_names_from_positions():
    """For current account positions: get symbols, fetch names from DMS (get_quotes_batch), update watchlist name only."""
    account_name = database.get_current_account_name()
    if not database.get_account(account_name):
        return jsonify({'error': f'Account not found: {account_name}'}), 400
    positions = database.get_positions(account_name)
    if not positions:
        return jsonify({'message': 'No positions', 'updated': 0})
    symbols = list(positions.keys())
    quotes = get_quotes_batch(symbols)
    updated = 0
    for symbol in symbols:
        quote = quotes.get(symbol, {})
        name = quote.get('name') if quote.get('valid') else None
        if not name:
            name = symbol
        if database.update_watchlist_name(symbol, name):
            updated += 1
    return jsonify({'message': f'Updated {updated} name(s) from positions (DMS)', 'updated': updated})


@bp.route('/api/watchlist/test', methods=['GET'])
@admin_required
def test_quote_service():
    """Test DMS quote service (AAPL); uses get_quote which uses DMS when DMS_BASE_URL set."""
    start = time.time()

    try:
        quote = get_quote('AAPL')
        elapsed = round((time.time() - start) * 1000)

        if quote.get('valid', False) and quote.get('price', 0) > 0:
            return jsonify({
                'status': 'ok',
                'message': 'DMS quote service ok',
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
