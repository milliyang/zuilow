"""
ZuiLow web UI (Flask Blueprint).

Register bp in project root app.py. Pages: /, /dashboard, /backtest, /futu, /scheduler,
/signals, /strategies, /brokers, /status. API: /api/order, /api/account, /api/signals,
/api/scheduler/*, /api/backtest, /api/futu/*, /api/market/*, etc. Auth: config/users.yaml,
Flask-Login, X-Webhook-Token for server-to-server.

Classes:
    bp   Flask Blueprint

"""

from .routes import bp

__all__ = ["bp"]
