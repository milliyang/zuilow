"""
API module.

Blueprints:
- account: account management (deposit/withdraw/reset)
- trade: trading & quotes
- watchlist: quote monitor
- analytics_api: analytics
- webhook: webhook signals
- opentimestamps: OpenTimestamps service
"""

from .account import bp as account_bp
from .trade import bp as trade_bp
from .watchlist import bp as watchlist_bp
from .analytics_api import bp as analytics_bp
from .webhook import bp as webhook_bp
from opents.api import bp as ots_bp

all_blueprints = [
    account_bp,
    trade_bp,
    watchlist_bp,
    analytics_bp,
    webhook_bp,
    ots_bp,
]

__all__ = [
    'account_bp', 'trade_bp', 'watchlist_bp',
    'analytics_bp', 'webhook_bp', 'ots_bp',
    'all_blueprints'
]
