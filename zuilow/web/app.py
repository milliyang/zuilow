"""
ZuiLow web: shared state and helpers for Flask Blueprint (routes in routes.py).

Global state: _futu_broker, _market_service, _scheduler.
Config: futu (FutuConfig defaults), ppt (config/brokers/ppt.yaml: base_url, webhook_token),
accounts (config/accounts/*.yaml).

Functions:
    get_scheduler() -> Optional[Scheduler]
    set_scheduler(scheduler) -> None
    get_futu_broker() -> Optional[FutuGateway]
    set_futu_broker(broker) -> None
    get_market_service() -> Optional[MarketService]
    set_market_service(service) -> None
    get_page(name: str) -> str   (dashboard, backtest, futu, scheduler, signals, strategies, brokers, status)
    execute_backtest(params: dict) -> dict
    get_account_by_name(name: str) -> Optional[dict]
    list_accounts_config() -> list[dict]   (name, type only)

Constants:
    WEBHOOK_TOKEN

"""

from __future__ import annotations

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ========== Global state ==========

# Futu broker instance
_futu_broker: Any = None
_futu_config: Any = None

# IBKR broker instance (optional; for account type ibkr)
_ibkr_broker: Any = None

# PPT broker instance (quote/history from DMS, trading from PPT)
_ppt_broker: Any = None

# Mixed market data service
_market_service: Any = None

# Strategy scheduler
_scheduler: Any = None

def _load_futu_defaults() -> dict:
    """Load Futu defaults from config file."""
    import yaml
    from pathlib import Path
    
    config_path = Path(__file__).parent.parent / "config" / "brokers" / "futu.yaml"
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return data.get('futu', {})
    return {}

_futu_defaults = _load_futu_defaults()

# ========== PPT broker config (config/brokers/ppt.yaml) ==========

def _load_ppt_config() -> dict:
    """Load PPT broker config from config/brokers/ppt.yaml (base_url, webhook_token)."""
    import yaml
    config_path = Path(__file__).parent.parent / "config" / "brokers" / "ppt.yaml"
    if not config_path.exists():
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return dict(data.get("ppt", {}))
    except Exception as e:
        logger.debug("Load ppt config: %s", e)
        return {}

_ppt_config = _load_ppt_config()

# Env override for auth: X-Webhook-Token check (auth.py). PPT base_url is from get_ppt_broker().config (ppt.yaml).
WEBHOOK_TOKEN = (os.getenv("WEBHOOK_TOKEN") or _ppt_config.get("webhook_token") or "").strip()


# ========== Account abstraction (config/accounts/*.yaml) ==========

def _load_accounts_config() -> list[dict]:
    """Load named accounts from config/accounts/ (paper.yaml, futu.yaml, ibkr.yaml)."""
    import yaml
    accounts_dir = Path(__file__).parent.parent / "config" / "accounts"
    if not accounts_dir.is_dir():
        return []
    combined = []
    for name in ("paper.yaml", "futu.yaml", "ibkr.yaml"):
        config_path = accounts_dir / name
        if not config_path.exists():
            continue
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            combined.extend(data.get("accounts") or [])
        except Exception as e:
            logger.warning("Load accounts %s failed: %s", name, e)
    return combined


_ACCOUNTS_LIST: list[dict] = _load_accounts_config()


def get_account_by_name(name: str) -> dict | None:
    """
    Look up account config by name (case-sensitive, strip whitespace).
    Returns e.g. { "name": "paper trade account 01", "type": "paper", "paper_account": "acc 01" }
    """
    key = (name or "").strip()
    if not key:
        return None
    for acc in _ACCOUNTS_LIST:
        if (acc.get("name") or "").strip() == key:
            return acc
    return None


def list_accounts_config() -> list[dict]:
    """
    List configured accounts (name + type only, no internal params). For strategy/UI selection.
    """
    return [
        {"name": (a.get("name") or "").strip(), "type": (a.get("type") or "paper").lower()}
        for a in _ACCOUNTS_LIST
        if (a.get("name") or "").strip()
    ]


def get_accounts_list() -> list[dict]:
    """Full list of account configs (for Brokers page etc.). Read-only."""
    return list(_ACCOUNTS_LIST)




# (Routes in web/routes.py Flask Blueprint)


def get_scheduler():
    """Return global scheduler instance (may be None)."""
    return _scheduler


def set_scheduler(scheduler):
    """Set global scheduler instance."""
    global _scheduler
    _scheduler = scheduler


def get_futu_broker():
    """Return global Futu broker instance (may be None)."""
    return _futu_broker


def set_futu_broker(broker):
    """Set global Futu broker instance."""
    global _futu_broker
    _futu_broker = broker


def get_ibkr_broker():
    """Return global IBKR broker instance (may be None)."""
    return _ibkr_broker


def set_ibkr_broker(broker):
    """Set global IBKR broker instance."""
    global _ibkr_broker
    _ibkr_broker = broker


def get_ppt_broker():
    """Return global PPT broker (lazy init only; connect via Brokers page or POST /api/brokers/ppt/connect)."""
    global _ppt_broker
    if _ppt_broker is None:
        try:
            from zuilow.components.brokers import PptGateway
            _ppt_broker = PptGateway()
        except Exception as e:
            logger.debug("PptGateway init: %s", e)
    return _ppt_broker


def set_ppt_broker(broker):
    """Set global PPT broker instance."""
    global _ppt_broker
    _ppt_broker = broker


def get_market_service():
    """Return global market service instance (may be None)."""
    return _market_service


def set_market_service(service):
    """Set global market service instance."""
    global _market_service
    _market_service = service


# ========== Helpers ==========


def get_page(name: str) -> str:
    """Return page HTML by name."""
    static_dir = Path(__file__).parent / "static"
    file_map = {
        "dashboard": "dashboard.html",
        "backtest": "backtest.html",
        "live": "live.html",
        "scheduler": "scheduler.html",
        "status": "status.html",
        "signals": "signals.html",
        "strategies": "strategies.html",
        "brokers": "brokers.html",
    }
    filename = file_map.get(name)
    if filename:
        static_file = static_dir / filename
        if static_file.exists():
            return static_file.read_text(encoding="utf-8")
    return "<h1>404 - Page Not Found</h1>"


def execute_backtest(params: dict) -> dict:
    """Run backtest (params: symbol, strategy, start_date, end_date, initial_capital, short_period, long_period, rsi_period, rsi_oversold, rsi_overbought)."""
    import numpy as np
    import pandas as pd

    from zuilow.components.backtest import (
        BacktestEngine, BacktestConfig,
        BuyAndHold, SMAStrategy, RSIStrategy,
    )

    symbol = params.get("symbol", "SPY")
    strategy_name = params.get("strategy", "sma")
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    initial_capital = float(params.get("initial_capital", 100000))
    short_period = int(params.get("short_period", 5))
    long_period = int(params.get("long_period", 20))
    rsi_period = int(params.get("rsi_period", 14))
    rsi_oversold = float(params.get("rsi_oversold", 30))
    rsi_overbought = float(params.get("rsi_overbought", 70))

    # Fetch data
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        if start_date and end_date:
            data = ticker.history(start=start_date, end=end_date)
        else:
            data = ticker.history(period="1y")
        if data.empty:
            raise ValueError(f"Failed to fetch data for {symbol}")
        data = data[["Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        logger.warning("Real data fetch failed, using synthetic data: %s", e)
        np.random.seed(42)
        days = 252
        dates = pd.date_range(start="2025-01-01", periods=days, freq="D")
        returns = np.random.normal(0.0003, 0.02, days)
        prices = 100 * np.cumprod(1 + returns)
        data = pd.DataFrame({
            "Open": prices * (1 + np.random.normal(0, 0.005, days)),
            "High": prices * (1 + abs(np.random.normal(0, 0.01, days))),
            "Low": prices * (1 - abs(np.random.normal(0, 0.01, days))),
            "Close": prices,
            "Volume": np.random.randint(1000000, 10000000, days),
        }, index=dates)

    if strategy_name == "sma":
        strategy = SMAStrategy(short_period=short_period, long_period=long_period)
    elif strategy_name == "rsi":
        strategy = RSIStrategy(period=rsi_period, oversold=rsi_oversold, overbought=rsi_overbought)
    else:
        strategy = BuyAndHold()

    engine = BacktestEngine(BacktestConfig(
        initial_capital=initial_capital,
        commission_rate=0.001,
        verbose=False,
    ))
    result = engine.run(strategy, data, symbol=symbol)

    return {
        "success": True,
        "summary": {
            "strategy": strategy_name,
            "symbol": symbol,
            "period": f"{result.start_date:%Y-%m-%d} ~ {result.end_date:%Y-%m-%d}",
            "initial_capital": result.initial_capital,
            "final_equity": round(result.final_equity, 2),
            "total_return": round(result.total_return, 2),
            "total_return_pct": round(result.total_return_pct, 2),
        },
        "metrics": {
            "annual_return": round(result.metrics.annual_return, 2),
            "max_drawdown": round(result.metrics.max_drawdown, 2),
            "sharpe_ratio": round(result.metrics.sharpe_ratio, 2),
            "win_rate": round(result.metrics.win_rate, 1),
            "profit_factor": round(result.metrics.profit_factor, 2),
            "total_trades": result.metrics.total_trades,
        },
        "equity_curve": [
            {"date": t.isoformat(), "equity": round(e, 2)}
            for t, e in result.equity_curve[::max(1, len(result.equity_curve)//100)]
        ],
        "trades": [
            {
                "id": t.id,
                "side": t.side.value,
                "quantity": round(t.quantity, 2),
                "price": round(t.price, 2),
                "pnl": round(t.pnl, 2),
                "timestamp": t.timestamp.isoformat(),
            }
            for t in result.trades[:50]
        ],
    }
