"""
ZuiLow web routes (Flask Blueprint).

Page routes (login required):
    /, /dashboard   Account dashboard
    /backtest       Backtest UI
    /futu           Futu control panel
    /scheduler      Scheduler UI
    /signals        Trading signals list
    /strategies     Strategy-centric view
    /brokers        Brokers overview (Futu, IBKR, PaperTrade)
    /status         System status (datasource, accounts)
    /login          Login page

API routes (selected):
    GET/POST /api/order/mode   Get/set trading mode (paper | live | both)
    POST /api/order   Place order (symbol, side, qty, account?, mode?, price?)
    GET /api/account?account=<name>   Account info (optional account filter)
    GET /api/accounts   List configured accounts (name, type)
    GET /api/signals?account=&market=&status=&kind=&date_from=&date_to=&page=&limit=   List signals (paginated, optional date range)
    POST /api/signals/<id>/cancel   Cancel pending signal
    GET /api/strategies   Strategy-centric view (strategy -> jobs, run counts)
    POST /api/scheduler/start, POST /api/scheduler/stop, GET /api/scheduler/status,
    GET /api/scheduler/jobs, GET /api/scheduler/history, GET /api/scheduler/statistics
    POST /api/backtest   Run backtest (params: symbol, strategy, start_date, end_date, ...)
    GET /api/futu/*, POST /api/futu/*   Futu connection and trading
    GET /api/market/quote/<symbol>, GET /api/market/history   Market data
    GET /api/order/status   Order status (Futu)

Register: app.register_blueprint(web.routes.bp)
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict
from flask import Blueprint, request, jsonify, redirect, session
from flask_login import login_required

import datetime as dt

from .app import (
    get_page,
    execute_backtest,
    get_scheduler,
    set_scheduler,
    get_futu_broker,
    set_futu_broker,
    get_ibkr_broker,
    set_ibkr_broker,
    get_ppt_broker,
    set_ppt_broker,
    get_market_service,
    get_account_by_name,
    get_accounts_list,
    list_accounts_config,
)
from zuilow.components.scheduler.history import get_history_db

import zuilow.components.control.ctrl as ctrl

logger = logging.getLogger(__name__)



bp = Blueprint("web", __name__)


# ========== Config (theme for simulate mode) ==========

@bp.route("/api/config")
def api_config():
    """Return client config (e.g. theme). theme=simulate when SIMULATION_TIME_URL set (red theme)."""
    theme = "simulate" if ctrl.is_sim_mode() else "default"
    return jsonify({"theme": theme})


@bp.route("/api/now")
def api_now():
    """Return current time for UI 'Updated HH:MM:SS'. Sim/real unified via ctrl.get_current_time_iso()."""
    return jsonify({"now": ctrl.get_current_time_iso()})


def _account_type_to_gateway(acc_type: str) -> str:
    """Map account type to Live page gateway key: futu -> futu, ibkr -> ib, paper -> ppt."""
    t = (acc_type or "").lower()
    if t == "ibkr":
        return "ib"
    if t == "paper":
        return "ppt"
    return "futu"


def _live_account_from_request() -> str | None:
    """Resolve live account: query/body 'account' or session['live_account']. None if missing."""
    account = (request.args.get("account") or (request.get_json(silent=True) or {}).get("account") or "").strip()
    if account:
        return account
    return session.get("live_account")


@bp.route("/api/live/session", methods=["GET"])
@login_required
def api_live_session_get():
    """Get current Live page session: account and gateway (from account type)."""
    account = session.get("live_account")
    if not account:
        return jsonify({"account": None, "gateway": "futu"})
    acc_cfg = get_account_by_name(account)
    if not acc_cfg:
        return jsonify({"account": account, "gateway": "futu"})
    gateway = _account_type_to_gateway(acc_cfg.get("type") or "")
    return jsonify({"account": account, "gateway": gateway})


@bp.route("/api/live/session", methods=["POST"])
@login_required
def api_live_session_post():
    """Set Live page session account. Body: { \"account\": \"ibkr-main\" }. Validates account exists."""
    data = request.get_json(silent=True) or {}
    account = (data.get("account") or "").strip() or None
    if not account:
        session.pop("live_account", None)
        return jsonify({"account": None, "gateway": "futu"})
    acc_cfg = get_account_by_name(account)
    if not acc_cfg:
        return jsonify({"error": f"Unknown account: {account}"}), 400
    session["live_account"] = account
    gateway = _account_type_to_gateway(acc_cfg.get("type") or "")
    return jsonify({"account": account, "gateway": gateway})


# ========== Page routes (login required, ref PPT) ==========

@bp.route("/")
@login_required
def index():
    """Dashboard (default page)."""
    return get_page("dashboard")


@bp.route("/dashboard")
@login_required
def dashboard():
    """Dashboard page."""
    return get_page("dashboard")


@bp.route("/backtest")
@login_required
def backtest():
    """Backtest page."""
    return get_page("backtest")


@bp.route("/futu")
@login_required
def futu():
    """Futu panel (redirect to Live)."""
    return redirect("/live", code=302)


@bp.route("/live")
@login_required
def live():
    """Live trading page (per-account: gateway selector, quick order, account, quote, positions, orders)."""
    return get_page("live")


@bp.route("/scheduler")
@login_required
def scheduler_page():
    """Scheduler page."""
    return get_page("scheduler")


@bp.route("/status")
@login_required
def status_page():
    """System status page (data sources, paper accounts)."""
    return get_page("status")


@bp.route("/signals")
@login_required
def signals_page():
    """Trading signals page."""
    return get_page("signals")


@bp.route("/strategies")
@login_required
def strategies_page():
    """Strategy-centric view."""
    return get_page("strategies")


@bp.route("/brokers")
@login_required
def brokers_page():
    """Brokers overview (Futu, IBKR, PaperTrade)."""
    return get_page("brokers")


# ========== API: order mode ==========

# Fallback mode when no account is specified (paper | live | both). Not persisted.
#_DEFAULT_NO_ACCOUNT_MODE = "paper"
_DEFAULT_NO_ACCOUNT_MODE = "live"


@bp.route("/api/order/mode", methods=["GET"])
def api_order_mode_get():
    """Return default mode when no account is given (fixed: paper)."""
    return jsonify({"mode": _DEFAULT_NO_ACCOUNT_MODE})


@bp.route("/api/order/mode", methods=["POST"])
def api_order_mode_post():
    """Accept mode for API compatibility; no global state. Returns same mode if valid."""
    data = request.get_json(silent=True) or {}
    mode = (data.get("mode") or _DEFAULT_NO_ACCOUNT_MODE).lower()
    if mode not in ("paper", "live", "both"):
        return jsonify({"error": "Invalid mode"}), 400
    return jsonify({"mode": mode})


def _resolve_order_price(symbol: str, account_name: str | None = None) -> float:
    """Resolve order price from quote when price not provided. Prefer broker for account so quote matches order gateway."""
    if account_name:
        acc_cfg = get_account_by_name(account_name)
        if acc_cfg:
            acc_type = (acc_cfg.get("type") or "paper").lower()
            broker = None
            if acc_type == "futu":
                broker = get_futu_broker()
            elif acc_type == "ibkr":
                broker = get_ibkr_broker()
            if broker and getattr(broker, "is_connected", False):
                try:
                    quote = broker.get_quote(symbol)
                    if quote and quote.get("error") is None and (quote.get("price") is not None or quote.get("Close") is not None):
                        return float(quote.get("price") or quote.get("Close") or 0)
                except Exception as e:
                    logger.debug("Broker quote for order price %s: %s", symbol, e)
    mgr = _get_datasource_manager()
    if not mgr:
        return 0.0
    try:
        quote = mgr.get_quote(symbol)
        if quote and ("price" in quote or "Close" in quote):
            return float(quote.get("price") or quote.get("Close") or 0)
    except Exception as e:
        logger.debug("Resolve order price %s: %s", symbol, e)
    return 0.0


@bp.route("/api/order", methods=["POST"])
def api_order_post():
    """
    Place order. Two modes:
    1) With account (recommended): account name from config/accounts.yaml, routed by type.
    2) Without account: use global mode (legacy).
    When price is 0 or missing: market order (no quote required). When price>0: limit order.
    """
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip()
    side = (data.get("side") or "buy").lower()
    qty = int(data.get("qty") or data.get("quantity") or 0)
    price = float(data.get("price") or 0)
    account_name = (data.get("account") or "").strip() or session.get("live_account")
    mode = (data.get("mode") or _DEFAULT_NO_ACCOUNT_MODE).lower()

    if not symbol or side not in ("buy", "sell") or qty <= 0:
        return jsonify({"error": "Require symbol, side(buy/sell), qty>0"}), 400

    from zuilow.components.backtest.types import OrderType
    if price <= 0:
        order_type = OrderType.MARKET
        price = 0.0
    else:
        order_type = OrderType.LIMIT

    result = {"symbol": symbol, "side": side, "qty": qty, "price": price, "order_type": order_type.value}

    # Route by account name (account abstraction)
    if account_name:
        acc_cfg = get_account_by_name(account_name)
        if not acc_cfg:
            return jsonify({"error": f"Unknown account: {account_name}", "hint": "Use GET /api/accounts for list"}), 400
        acc_type = (acc_cfg.get("type") or "paper").lower()
        result["account"] = account_name
        result["account_type"] = acc_type

        if acc_type == "paper":
            ppt = get_ppt_broker()
            if not ppt or not ppt.is_connected:
                return jsonify({**result, "error": "PPT broker not connected (Connect on Brokers page)"}), 503
            paper_account = (acc_cfg.get("paper_account") or "").strip() or account_name
            try:
                import requests
                base = (ppt.config.base_url or "").strip().rstrip("/")
                url = f"{base}/api/webhook"
                headers = {"Content-Type": "application/json"}
                if (ppt.config.webhook_token or "").strip():
                    headers["X-Webhook-Token"] = ppt.config.webhook_token.strip()
                sim_time = request.headers.get("X-Simulation-Time", "").strip()
                if not sim_time:
                    sim_time = ctrl.get_time_iso() or ""
                if sim_time:
                    headers["X-Simulation-Time"] = sim_time
                payload = {
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "price": price,
                    "account": paper_account,
                }
                r = requests.post(url, json=payload, headers=headers, timeout=10)
                if r.status_code == 200:
                    result["result"] = r.json()
                elif r.status_code == 401:
                    result["error"] = "Webhook token invalid or not configured"
                else:
                    result["error"] = r.text or str(r.status_code)
            except Exception as e:
                result["error"] = str(e)
            return jsonify(result)

        if acc_type == "futu":
            broker = get_futu_broker()
            if not broker or not broker.is_connected:
                return jsonify({**result, "error": "Futu not connected"}), 503
            futu_acc_id = acc_cfg.get("futu_acc_id")
            acc_id = int(futu_acc_id) if futu_acc_id is not None else None
            env_raw = (acc_cfg.get("env") or "").strip().upper()
            trd_env = env_raw if env_raw in ("REAL", "SIMULATE") else None
            try:
                from zuilow.components.backtest.types import OrderSide
                order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
                order_id = broker.place_order(symbol=symbol, side=order_side, quantity=qty, price=price or None, order_type=order_type, acc_id=acc_id, trd_env=trd_env, account_name=account_name)
                result["result"] = {"ok": True, "order_id": order_id} if order_id else {"error": "Order failed"}
            except Exception as e:
                result["error"] = str(e)
            return jsonify(result)

        if acc_type == "ibkr":
            broker = get_ibkr_broker()
            if not broker or not getattr(broker, "is_connected", False):
                return jsonify({**result, "error": "IBKR not connected"}), 503
            ibkr_account = (acc_cfg.get("ibkr_account_id") or acc_cfg.get("ibkr_account") or "").strip() or None
            try:
                from zuilow.components.backtest.types import OrderSide
                order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
                order_id = broker.place_order(symbol=symbol, side=order_side, quantity=qty, price=price or None, order_type=order_type, account=ibkr_account)
                result["result"] = {"ok": True, "order_id": order_id} if order_id else {"error": "Order failed"}
            except Exception as e:
                result["error"] = str(e)
            return jsonify(result)

        # Other types placeholder
        return jsonify({**result, "error": f"Account type not implemented: {acc_type}"}), 501

    # Fallback: route by global mode when account not specified
    if mode not in ("paper", "live", "both"):
        return jsonify({"error": "mode must be paper / live / both"}), 400
    result["mode"] = mode

    if mode in ("paper", "both"):
        try:
            import requests
            ppt = get_ppt_broker()
            base = (ppt.config.base_url or "").strip().rstrip("/")
            url = f"{base}/api/webhook"
            headers = {"Content-Type": "application/json"}
            if (ppt.config.webhook_token or "").strip():
                headers["X-Webhook-Token"] = ppt.config.webhook_token.strip()
            sim_time = request.headers.get("X-Simulation-Time", "").strip() or ctrl.get_time_iso() or ""
            if sim_time:
                headers["X-Simulation-Time"] = sim_time
            payload = {"symbol": symbol, "side": side, "qty": qty, "price": price}
            r = requests.post(url, json=payload, headers=headers, timeout=10)
            if r.status_code == 200:
                result["paper"] = r.json()
            elif r.status_code == 401:
                result["paper"] = {"error": "Webhook token invalid or not configured"}
            else:
                result["paper"] = {"error": r.text or str(r.status_code)}
        except Exception as e:
            result["paper"] = {"error": str(e)}

    if mode in ("live", "both"):
        broker = get_futu_broker()
        if not broker or not broker.is_connected:
            result["live"] = {"error": "Futu not connected"}
        else:
            try:
                from zuilow.components.backtest.types import OrderSide
                order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
                order_id = broker.place_order(symbol=symbol, side=order_side, quantity=qty, price=price or None, order_type=order_type)
                result["live"] = {"ok": True, "order_id": order_id} if order_id else {"error": "Order failed"}
            except Exception as e:
                result["live"] = {"error": str(e)}

    return jsonify(result)


@bp.route("/api/order/<order_id>", methods=["DELETE"])
def api_order_delete(order_id):
    """Unified cancel order. Optional query: account=<name>; else session['live_account']. Routes by account type (futu/ibkr)."""
    account_name = _live_account_from_request()
    if not account_name:
        return jsonify({"error": "No account (set session or pass ?account=)"}), 400
    acc_cfg = get_account_by_name(account_name)
    if not acc_cfg:
        return jsonify({"error": f"Unknown account: {account_name}"}), 400
    acc_type = (acc_cfg.get("type") or "").lower()
    if acc_type == "futu":
        broker = get_futu_broker()
        if not broker or not broker.is_connected:
            return jsonify({"error": "Futu not connected"}), 503
        fid = acc_cfg.get("futu_acc_id")
        acc_id = int(fid) if fid is not None else None
        env_raw = (acc_cfg.get("env") or "").strip().upper()
        trd_env = env_raw if env_raw in ("REAL", "SIMULATE") else None
        if broker.cancel_order(order_id, acc_id=acc_id, trd_env=trd_env):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Cancel failed"}), 500
    if acc_type == "ibkr":
        _ibkr_ensure_event_loop()
        broker = get_ibkr_broker()
        if not broker or not getattr(broker, "is_connected", False):
            return jsonify({"error": "IBKR not connected"}), 503
        acc_id = (acc_cfg.get("ibkr_account_id") or acc_cfg.get("ibkr_account") or "").strip() or None
        if broker.cancel_order(order_id, account=acc_id):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Cancel failed"}), 500
    return jsonify({"error": f"Cancel not supported for account type: {acc_type}"}), 501


# ========== API: account & trades (dashboard) ==========

def _ppt_base() -> str:
    return (get_ppt_broker().config.base_url or "").strip().rstrip("/")


def _build_live_account(acc_id: int | None = None, trd_env: str | None = None):
    """Build account dict from Futu. acc_id/trd_env: optional; trd_env REAL/SIMULATE from account config."""
    broker = get_futu_broker()
    if not broker or not broker.is_connected:
        return None
    info = broker.get_account_info(acc_id=acc_id, trd_env=trd_env)
    positions = broker.get_positions(acc_id=acc_id, trd_env=trd_env)
    if info is None:
        return None
    equity = float(info.get("total_assets", 0))
    cash = float(info.get("cash", 0))
    market_value = float(info.get("market_value", info.get("market_val", 0)))
    pnl = sum(float(p.get("pnl", 0)) for p in positions)
    pnl_pct = (pnl / (equity - pnl) * 100) if (equity - pnl) and equity else 0.0
    pos_list = [
        {
            "symbol": p.get("symbol", ""),
            "quantity": int(p.get("quantity", 0)),
            "avg_price": float(p.get("avg_price", 0)),
            "current_price": float(p.get("current_price", 0)),
            "market_value": float(p.get("market_value", 0)),
            "pnl": float(p.get("pnl", 0)),
            "pnl_pct": float(p.get("pnl_pct", 0)),
        }
        for p in positions
    ]
    return {
        "equity": round(equity, 2),
        "cash": round(cash, 2),
        "market_value": round(market_value, 2),
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
        "source": "Futu",
        "mode": "live",
        "positions": pos_list,
    }


def _build_live_account_ibkr(broker, account: str | None = None):
    """Build account dict from IBKR (total_assets, cash, market_value, power, pnl, positions). account: optional IB account id."""
    if not broker or not getattr(broker, "is_connected", False):
        return None
    info = broker.get_account_info(account=account)
    positions = broker.get_positions(account=account)
    if info is None:
        return None
    equity = float(info.get("total_assets", 0))
    cash = float(info.get("cash", 0))
    market_value = float(info.get("market_value", 0))
    power = float(info.get("available_funds", info.get("power", cash)))
    pnl = sum(float(p.get("pnl", 0)) for p in positions)
    pnl_pct = (pnl / (equity - pnl) * 100) if (equity - pnl) and equity else 0.0
    pos_list = [
        {
            "symbol": p.get("symbol", ""),
            "name": p.get("name", ""),
            "quantity": int(p.get("quantity", 0)),
            "available": int(p.get("available", p.get("quantity", 0))),
            "avg_price": float(p.get("avg_price", 0)),
            "current_price": float(p.get("current_price", 0)),
            "market_value": float(p.get("market_value", 0)),
            "pnl": float(p.get("pnl", 0)),
            "pnl_pct": float(p.get("pnl_pct", 0)),
        }
        for p in positions
    ]
    return {
        "equity": round(equity, 2),
        "total_assets": round(equity, 2),
        "cash": round(cash, 2),
        "market_value": round(market_value, 2),
        "power": round(power, 2),
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
        "source": "IBKR",
        "mode": "live",
        "positions": pos_list,
    }


def _build_paper_account(acc_data: dict, pos_data: dict | None) -> dict:
    """Build dashboard account from PPT /api/account + /api/positions."""
    total_value = float(acc_data.get("total_value", 0))
    cash = float(acc_data.get("cash", 0))
    position_value = float(acc_data.get("position_value", 0))
    pnl = float(acc_data.get("pnl", 0))
    pnl_pct = float(acc_data.get("pnl_pct", 0))
    positions = []
    if pos_data and isinstance(pos_data.get("positions"), list):
        for p in pos_data["positions"]:
            positions.append({
                "symbol": p.get("symbol", ""),
                "quantity": int(p.get("qty", 0)),
                "avg_price": float(p.get("avg_price", 0)),
                "current_price": float(p.get("current_price", 0)),
                "market_value": float(p.get("market_value", p.get("cost", 0))),
                "pnl": float(p.get("pnl", 0)),
                "pnl_pct": float(p.get("pnl_pct", 0)),
            })
    return {
        "equity": round(total_value, 2),
        "cash": round(cash, 2),
        "market_value": round(position_value, 2),
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
        "source": "Paper Trade",
        "mode": "paper",
        "positions": positions,
    }


@bp.route("/api/account")
def api_account():
    """
    Account info. Optional query: account=<name> (from config/accounts.yaml).
    When not given, uses session['live_account'] if set, else default mode paper.
    """
    account_name = _live_account_from_request()
    if account_name:
        acc_cfg = get_account_by_name(account_name)
        if not acc_cfg:
            return jsonify({"error": f"Unknown account: {account_name}"}), 400
        acc_type = (acc_cfg.get("type") or "paper").lower()
        if acc_type == "paper":
            paper_account = (acc_cfg.get("paper_account") or "").strip() or account_name
            ppt = get_ppt_broker()
            acc = ppt.get_account_raw(paper_account) if ppt else None
            pos = ppt.get_positions_raw(paper_account) if ppt else None
            if acc:
                out = _build_paper_account(acc, pos)
                out["account"] = account_name
                out["account_type"] = acc_type
                return jsonify(out)
            return jsonify({
                "account": account_name, "account_type": acc_type,
                "equity": 0, "cash": 0, "market_value": 0, "pnl": 0, "pnl_pct": 0,
                "source": "Paper Trade", "positions": [],
            })
        if acc_type == "futu":
            broker = get_futu_broker()
            if not broker or not broker.is_connected:
                return jsonify({"error": "Futu not connected", "account": account_name}), 503
            futu_acc_id = acc_cfg.get("futu_acc_id")
            acc_id = int(futu_acc_id) if futu_acc_id is not None else None
            env_raw = (acc_cfg.get("env") or "").strip().upper()
            trd_env = env_raw if env_raw in ("REAL", "SIMULATE") else None
            out = _build_live_account(acc_id=acc_id, trd_env=trd_env)
            if out:
                out["account"] = account_name
                out["account_type"] = acc_type
                return jsonify(out)
            return jsonify({
                "account": account_name, "account_type": acc_type,
                "equity": 0, "cash": 0, "market_value": 0, "pnl": 0, "pnl_pct": 0,
                "source": "Futu", "positions": [],
            })
        if acc_type == "ibkr":
            broker = get_ibkr_broker()
            if not broker or not getattr(broker, "is_connected", False):
                return jsonify({"error": "IBKR not connected", "account": account_name}), 503
            ibkr_account = (acc_cfg.get("ibkr_account_id") or acc_cfg.get("ibkr_account") or "").strip() or None
            out = _build_live_account_ibkr(broker, account=ibkr_account)
            if out:
                out["account"] = account_name
                out["account_type"] = acc_type
                return jsonify(out)
            return jsonify({
                "error": "IBKR account query failed (timeout or disconnected). Check server log for _run_on_connection_loop.",
                "account": account_name,
            }), 503
        return jsonify({"error": f"Unsupported account type: {acc_type}", "account": account_name}), 501

    mode = _DEFAULT_NO_ACCOUNT_MODE
    if mode == "paper":
        ppt = get_ppt_broker()
        acc = ppt.get_account_raw() if ppt else None
        pos = ppt.get_positions_raw() if ppt else None
        if acc:
            return jsonify(_build_paper_account(acc, pos))
        return jsonify({
            "equity": 0, "cash": 0, "market_value": 0, "pnl": 0, "pnl_pct": 0,
            "source": "Paper Trade", "mode": "paper", "positions": [],
        })
    if mode == "live":
        out = _build_live_account()
        if out:
            return jsonify(out)
        return jsonify({
            "equity": 0, "cash": 0, "market_value": 0, "pnl": 0, "pnl_pct": 0,
            "source": "Futu", "mode": "live", "positions": [],
        })
    if mode == "both":
        ppt = get_ppt_broker()
        acc_paper = ppt.get_account_raw() if ppt else None
        pos_paper = ppt.get_positions_raw() if ppt else None
        live = _build_live_account()
        paper_acc = _build_paper_account(acc_paper, pos_paper) if acc_paper else None
        paper_equity = paper_acc["equity"] if paper_acc else 0
        paper_cash = paper_acc["cash"] if paper_acc else 0
        paper_mv = paper_acc["market_value"] if paper_acc else 0
        paper_pnl = paper_acc["pnl"] if paper_acc else 0
        paper_pnl_pct = paper_acc["pnl_pct"] if paper_acc else 0
        live_equity = live["equity"] if live else 0
        live_cash = live["cash"] if live else 0
        live_mv = live["market_value"] if live else 0
        live_pnl = live["pnl"] if live else 0
        live_pnl_pct = live["pnl_pct"] if live else 0
        return jsonify({
            "mode": "both",
            "equity": round(paper_equity + live_equity, 2),
            "cash": round(paper_cash + live_cash, 2),
            "market_value": round(paper_mv + live_mv, 2),
            "pnl": round(paper_pnl + live_pnl, 2),
            "pnl_pct": round((paper_pnl_pct + live_pnl_pct) / 2, 2) if (paper_acc or live) else 0,
            "paper": paper_acc or {"equity": 0, "cash": 0, "market_value": 0, "pnl": 0, "pnl_pct": 0, "positions": []},
            "live": live or {"equity": 0, "cash": 0, "market_value": 0, "pnl": 0, "pnl_pct": 0, "positions": []},
        })
    return jsonify({"equity": 0, "cash": 0, "market_value": 0, "pnl": 0, "pnl_pct": 0, "positions": []})


@bp.route("/api/orders")
def api_orders():
    """Unified orders list. Optional query: account=<name>; else session['live_account']. Routes by account type; paper from PPT broker."""
    account_name = _live_account_from_request()
    if not account_name:
        return jsonify({"orders": []})
    acc_cfg = get_account_by_name(account_name)
    if not acc_cfg:
        return jsonify({"error": f"Unknown account: {account_name}"}), 400
    acc_type = (acc_cfg.get("type") or "paper").lower()
    if acc_type == "paper":
        ppt = get_ppt_broker()
        if not ppt or not ppt.is_connected:
            return jsonify({"orders": []})
        paper_account = (acc_cfg.get("paper_account") or "").strip() or account_name
        orders = ppt.get_orders(account=paper_account)
        return jsonify({"orders": orders or []})
    if acc_type == "futu":
        broker = get_futu_broker()
        if not broker or not broker.is_connected:
            return jsonify({"error": "Futu not connected"}), 503
        fid = acc_cfg.get("futu_acc_id")
        acc_id = int(fid) if fid is not None else None
        env_raw = (acc_cfg.get("env") or "").strip().upper()
        trd_env = env_raw if env_raw in ("REAL", "SIMULATE") else None
        orders = broker.get_orders(acc_id=acc_id, trd_env=trd_env)
        return jsonify({"orders": orders or []})
    if acc_type == "ibkr":
        _ibkr_ensure_event_loop()
        broker = get_ibkr_broker()
        if not broker or not getattr(broker, "is_connected", False):
            return jsonify({"error": "IBKR not connected"}), 503
        acc_id = (acc_cfg.get("ibkr_account_id") or acc_cfg.get("ibkr_account") or "").strip() or None
        orders = broker.get_orders(account=acc_id)
        return jsonify({"orders": orders or []})
    return jsonify({"orders": []})


@bp.route("/api/positions")
def api_positions():
    """Unified positions list. Optional query: account=<name>; else session['live_account']. Routes by account type; paper returns []."""
    account_name = _live_account_from_request()
    if not account_name:
        return jsonify({"positions": []})
    acc_cfg = get_account_by_name(account_name)
    if not acc_cfg:
        return jsonify({"error": f"Unknown account: {account_name}"}), 400
    acc_type = (acc_cfg.get("type") or "paper").lower()
    if acc_type == "paper":
        paper_account = (acc_cfg.get("paper_account") or "").strip() or account_name
        ppt = get_ppt_broker()
        pos = ppt.get_positions_raw(paper_account) if ppt else None
        positions = (pos.get("positions") or []) if isinstance(pos, dict) else []
        return jsonify({"positions": positions})
    if acc_type == "futu":
        broker = get_futu_broker()
        if not broker or not broker.is_connected:
            return jsonify({"error": "Futu not connected"}), 503
        fid = acc_cfg.get("futu_acc_id")
        acc_id = int(fid) if fid is not None else None
        env_raw = (acc_cfg.get("env") or "").strip().upper()
        trd_env = env_raw if env_raw in ("REAL", "SIMULATE") else None
        positions = broker.get_positions(acc_id=acc_id, trd_env=trd_env)
        return jsonify({"positions": positions or []})
    if acc_type == "ibkr":
        _ibkr_ensure_event_loop()
        broker = get_ibkr_broker()
        if not broker or not getattr(broker, "is_connected", False):
            return jsonify({"error": "IBKR not connected"}), 503
        acc_id = (acc_cfg.get("ibkr_account_id") or acc_cfg.get("ibkr_account") or "").strip() or None
        positions = broker.get_positions(account=acc_id)
        return jsonify({"positions": positions or []})
    return jsonify({"positions": []})


def _trades_slice(trades: list, page: int, limit: int) -> tuple[list, int]:
    """Apply pagination: return (slice, total)."""
    total = len(trades)
    page = max(1, page)
    limit = min(max(1, limit), 200)
    offset = (page - 1) * limit
    return trades[offset : offset + limit], total


@bp.route("/api/trades")
def api_trades():
    """
    Recent trades. Optional query: account=<name>, page=1, limit=20.
    When account is given, returns that account's trades only.
    """
    account_name = (request.args.get("account") or "").strip() or None
    page = request.args.get("page", 1, type=int)
    limit = request.args.get("limit", 20, type=int)
    limit = min(max(limit, 1), 200)

    if account_name:
        acc_cfg = get_account_by_name(account_name)
        if not acc_cfg:
            return jsonify({"error": f"Unknown account: {account_name}"}), 400
        acc_type = (acc_cfg.get("type") or "paper").lower()
        trades = []
        if acc_type == "paper":
            paper_account = (acc_cfg.get("paper_account") or "").strip() or account_name
            ppt = get_ppt_broker()
            data = ppt.get_trades(account=paper_account, page=page, limit=limit) if ppt else None
            if data and isinstance(data.get("trades"), list):
                for t in data["trades"]:
                    trades.append({
                        "timestamp": t.get("time", ""),
                        "symbol": t.get("symbol", ""),
                        "side": t.get("side", "buy"),
                        "quantity": int(t.get("qty", 0)),
                        "price": float(t.get("price", 0)),
                        "value": float(t.get("value", 0)),
                        "pnl": 0,
                        "source": "paper",
                    })
        elif acc_type == "futu":
            broker = get_futu_broker()
            if broker and broker.is_connected:
                futu_acc_id = acc_cfg.get("futu_acc_id")
                acc_id = int(futu_acc_id) if futu_acc_id is not None else None
                env_raw = (acc_cfg.get("env") or "").strip().upper()
                trd_env = env_raw if env_raw in ("REAL", "SIMULATE") else None
                deals = broker.get_deals(acc_id=acc_id, trd_env=trd_env)
                for d in deals:
                    trades.append({
                        "timestamp": d.get("timestamp", d.get("created_at", "")),
                        "symbol": d.get("symbol", ""),
                        "side": d.get("side", "buy"),
                        "quantity": int(d.get("quantity", 0)),
                        "price": float(d.get("price", 0)),
                        "pnl": 0,
                        "source": "live",
                    })
        elif acc_type == "ibkr":
            broker = get_ibkr_broker()
            if broker and getattr(broker, "is_connected", False):
                ibkr_account = (acc_cfg.get("ibkr_account_id") or acc_cfg.get("ibkr_account") or "").strip() or None
                deals = broker.get_deals(account=ibkr_account)
                for d in deals:
                    trades.append({
                        "timestamp": d.get("timestamp", d.get("created_at", "")),
                        "symbol": d.get("symbol", ""),
                        "side": d.get("side", "buy"),
                        "quantity": int(d.get("quantity", 0)),
                        "price": float(d.get("price", 0)),
                        "pnl": 0,
                        "source": "live",
                    })
        trades.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        slice_trades, total = _trades_slice(trades, page, limit)
        return jsonify({"trades": slice_trades, "total": total, "page": page, "limit": limit, "account": account_name})

    mode = _DEFAULT_NO_ACCOUNT_MODE
    trades = []
    if mode in ("paper", "both"):
        ppt = get_ppt_broker()
        data = ppt.get_trades(page=page, limit=limit) if ppt else None
        if data and isinstance(data.get("trades"), list):
            for t in data["trades"]:
                trades.append({
                    "timestamp": t.get("time", ""),
                    "symbol": t.get("symbol", ""),
                    "side": t.get("side", "buy"),
                    "quantity": int(t.get("qty", 0)),
                    "price": float(t.get("price", 0)),
                    "value": float(t.get("value", 0)),
                    "pnl": 0,
                    "source": "paper",
                })
    if mode in ("live", "both"):
        broker = get_futu_broker()
        if broker and broker.is_connected:
            deals = broker.get_deals()
            for d in deals:
                trades.append({
                    "timestamp": d.get("timestamp", d.get("created_at", "")),
                    "symbol": d.get("symbol", ""),
                    "side": d.get("side", "buy"),
                    "quantity": int(d.get("quantity", 0)),
                    "price": float(d.get("price", 0)),
                    "pnl": 0,
                    "source": "live",
                })
    if mode == "both":
        trades.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    slice_trades, total = _trades_slice(trades, page, limit)
    return jsonify({"trades": slice_trades, "total": total, "page": page, "limit": limit, "mode": mode})


# ========== API: order status ==========

@bp.route("/api/order/status")
def api_order_status():
    """Gateway status: live (Futu) and paper (PPT broker). PPT connected = both PPT and DMS reachable."""
    broker = get_futu_broker()
    ppt = get_ppt_broker()
    paper_connected = ppt.is_connected if ppt else False
    return jsonify({
        "live_broker": {
            "connected": broker.is_connected if broker else False,
            "type": "Futu" if (broker and broker.is_connected) else None,
        },
        "paper_trade": {
            "connected": paper_connected,
            "url": _ppt_base() or None,
        },
    })


@bp.route("/api/accounts")
def api_accounts_list():
    """
    List configured accounts (name + type) for strategy/UI. From config/accounts.yaml.
    """
    accounts = list_accounts_config()
    return jsonify({"accounts": accounts})


# ========== API: system status (data sources, paper accounts) ==========

def _get_datasource_manager():
    """Get global datasource manager."""
    try:
        from zuilow.components.datasource import get_manager
        return get_manager()
    except Exception as e:
        logger.debug("Datasource manager: %s", e)
        return None


@bp.route("/api/system/datasources")
def api_system_datasources():
    """List data sources with connection status."""
    mgr = _get_datasource_manager()
    if mgr is None:
        return jsonify({"sources": []})
    sources = []
    for name in mgr.list_sources():
        src = mgr.get_source(name)
        if src is None:
            continue
        sources.append({
            "name": name,
            "type": src.__class__.__name__.replace("Source", ""),
            "connected": getattr(src, "_connected", False),
        })
    return jsonify({"sources": sources})


@bp.route("/api/system/datasources/<name>/test", methods=["POST"])
def api_system_datasource_test(name: str):
    """Test data source by fetching history (default SPY, last 5 days)."""
    mgr = _get_datasource_manager()
    if mgr is None:
        return jsonify({"ok": False, "error": "No datasource manager"}), 500
    src = mgr.get_source(name)
    if src is None:
        return jsonify({"ok": False, "error": f"Unknown source: {name}"}), 404
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "SPY").strip() or "SPY"
    end_dt = ctrl.get_current_dt()
    start_dt = end_dt - dt.timedelta(days=int(data.get("days", 5)))
    try:
        if not getattr(src, "_connected", False):
            src.connect()
        df = src.get_history(symbol, start_dt, end_dt, "1d")
        if df is not None and len(df) > 0:
            return jsonify({"ok": True, "symbol": symbol, "rows": len(df), "source": name})
        return jsonify({"ok": False, "error": "No data returned", "symbol": symbol}), 400
    except Exception as e:
        logger.exception("Datasource test failed: %s", name)
        return jsonify({"ok": False, "error": str(e), "symbol": symbol}), 500


@bp.route("/api/system/accounts")
def api_system_accounts():
    """List paper (PPT broker) accounts and overall connection status."""
    ppt = get_ppt_broker()
    data = ppt.get_accounts() if ppt else None
    if data is None:
        return jsonify({
            "connected": False,
            "paper_trade_url": _ppt_base() or None,
            "accounts": [],
        })
    accounts = []
    for acc in data.get("accounts", []):
        accounts.append({
            "name": acc.get("name", ""),
            "total_value": acc.get("total_value", 0),
            "pnl": acc.get("pnl", 0),
            "pnl_pct": acc.get("pnl_pct", 0),
            "is_current": acc.get("is_current", False),
        })
    return jsonify({
        "connected": True,
        "paper_trade_url": _ppt_base() or None,
        "current": data.get("current", ""),
        "accounts": accounts,
    })


@bp.route("/api/system/accounts/test", methods=["POST"])
def api_system_accounts_test():
    """Test PPT broker connection (GET /api/accounts)."""
    ppt = get_ppt_broker()
    data = ppt.get_accounts() if ppt else None
    if data is not None:
        return jsonify({"ok": True, "message": "PPT broker connected"})
    return jsonify({"ok": False, "error": "Connection failed", "url": _ppt_base() or ""}), 503


@bp.route("/api/system/accounts/<name>/test", methods=["POST"])
def api_system_account_test(name: str):
    """Test specific paper account: GET /api/account?account=name."""
    try:
        if not _ppt_base():
            return jsonify({"ok": False, "error": "PPT broker not configured (config/brokers/ppt.yaml)"}), 503
        ppt = get_ppt_broker()
        acc = ppt.get_account_raw(name, require_connected=False) if ppt else None
        if acc is None:
            return jsonify({"ok": False, "error": "Failed to get account (PPT unreachable or account not found)"}), 503
        if acc.get("name") != name:
            return jsonify({"ok": False, "error": f"Account returned name {acc.get('name')}, expected {name}"}), 400
        return jsonify({"ok": True, "account": name, "cash": acc.get("cash"), "total_value": acc.get("total_value")})
    except Exception as e:
        logger.exception("Account test failed: %s", name)
        return jsonify({"ok": False, "error": str(e)}), 500


# ========== API: signals ==========

@bp.route("/api/signals")
@login_required
def api_signals():
    """List trading signals (query: account, market, status, kind, date_from, date_to, page, limit)."""
    from zuilow.components.signals import get_signal_store
    account = request.args.get("account") or None
    market = request.args.get("market") or None
    status = request.args.get("status") or None
    kind = request.args.get("kind") or None
    date_from = request.args.get("date_from") or None  # YYYY-MM-DD
    date_to = request.args.get("date_to") or None      # YYYY-MM-DD
    page = request.args.get("page", 1, type=int)
    page = max(1, page)
    limit = request.args.get("limit", 50, type=int)
    limit = min(max(limit, 1), 500)
    offset = (page - 1) * limit
    store = get_signal_store()
    total = store.count_signals(
        account=account, market=market, status=status, kind=kind,
        date_from=date_from, date_to=date_to,
    )
    signals = store.list_signals(
        account=account, market=market, status=status, kind=kind,
        date_from=date_from, date_to=date_to,
        offset=offset, limit=limit,
    )
    return jsonify({
        "signals": [s.to_dict() for s in signals],
        "total": total,
        "page": page,
        "limit": limit,
    })


@bp.route("/api/signals/<int:signal_id>/cancel", methods=["POST"])
@login_required
def api_signal_cancel(signal_id):
    """Cancel a pending signal."""
    from zuilow.components.signals import get_signal_store
    store = get_signal_store()
    ok = store.cancel(signal_id)
    return jsonify({"ok": ok})


# ========== API: strategies (aggregate view) ==========

@bp.route("/api/strategies")
@login_required
def api_strategies():
    """Strategy-centric view: strategy name -> jobs, run counts."""
    s = get_scheduler()
    jobs = s.get_jobs() if s else []
    by_strategy = {}
    for j in jobs:
        if not j.strategy:
            continue
        if j.strategy not in by_strategy:
            by_strategy[j.strategy] = {"strategy": j.strategy, "jobs": [], "total_runs": 0}
        by_strategy[j.strategy]["jobs"].append({
            "name": j.name, "trigger": j.trigger, "run_count": j.run_count, "last_run": j.last_run.isoformat() if j.last_run else None,
        })
        by_strategy[j.strategy]["total_runs"] += j.run_count
    return jsonify(list(by_strategy.values()))


# ========== API: scheduler ==========

def _ensure_scheduler():
    """Lazy-create scheduler if not set."""
    s = get_scheduler()
    if s is None:
        from zuilow.components.scheduler import Scheduler
        set_scheduler(Scheduler())
        s = get_scheduler()
    return s


@bp.route("/api/scheduler/start", methods=["POST"])
def api_scheduler_start():
    """Start scheduler."""
    s = _ensure_scheduler()
    s.start()
    return jsonify({"ok": True, "running": s.is_running})


@bp.route("/api/scheduler/stop", methods=["POST"])
def api_scheduler_stop():
    """Stop scheduler."""
    s = get_scheduler()
    if s is None:
        return jsonify({"ok": True, "running": False})
    s.stop()
    return jsonify({"ok": True, "running": s.is_running})


@bp.route("/api/scheduler/tick", methods=["POST"])
def api_scheduler_tick():
    """Run one scheduler tick. Prefer X-Simulation-Time header (set_time_iso); else get_tick_sim_time() (fetch from stime)."""
    now_str = request.headers.get("X-Simulation-Time")
    tick_time_set = False
    if now_str:
        tick_time_set = ctrl.set_time_iso(now_str)
        if not tick_time_set:
            logger.warning("Tick: invalid X-Simulation-Time %s", (now_str or "")[:50])
    if not tick_time_set:
        if ctrl.get_tick_sim_time() is not None:
            tick_time_set = True
    try:
        s = _ensure_scheduler()
        n = s.run_one_tick()
        return jsonify({"executed": n})
    finally:
        if tick_time_set:
            ctrl.clear_tick_sim_time()


@bp.route("/api/scheduler/status")
def api_scheduler_status():
    """Get scheduler status and job list."""
    s = get_scheduler()
    if s is None:
        return jsonify({"running": False, "jobs": []})
    jobs = []
    for j in s.get_jobs():
        jobs.append({
            "name": j.name,
            "strategy": j.strategy,
            "symbols": j.symbols,
            "trigger": j.trigger,
            "mode": j.mode,
            "enabled": j.enabled,
            "is_running": j.is_running,
            "last_run": j.last_run.isoformat() if j.last_run else None,
            "next_run": j.next_run.isoformat() if j.next_run else None,
            "run_count": j.run_count,
            "error_count": j.error_count,
        })
    return jsonify({"running": s.is_running, "jobs": jobs})


@bp.route("/api/scheduler/statistics")
def api_scheduler_statistics():
    """Get scheduler run statistics."""
    db = get_history_db()
    return jsonify(db.get_statistics())

@bp.route("/api/scheduler/history")
def api_scheduler_history():
    """Get scheduler job run history (paginated). Query: job_name, page, limit."""
    db = get_history_db()
    job_name = request.args.get("job_name") or None
    page = request.args.get("page", 1, type=int)
    page = max(1, page)
    limit = request.args.get("limit", 30, type=int)
    limit = min(max(limit, 1), 100)
    offset = (page - 1) * limit
    stats = db.get_statistics(job_name=job_name)
    total = stats.get("total_runs", 0)
    records = db.get_history(job_name=job_name, limit=limit, offset=offset)
    return jsonify({
        "histories": [r.to_dict() for r in records],
        "total": total,
        "page": page,
        "limit": limit,
    })


# ========== API: backtest ==========

@bp.route("/api/backtest", methods=["POST"])
def api_backtest():
    """Run backtest."""
    data = request.get_json(silent=True) or {}
    try:
        result = execute_backtest(data)
        return jsonify(result)
    except Exception as e:
        logger.exception("Backtest failed")
        return jsonify({"success": False, "error": str(e)}), 500


# ========== API: Futu ==========

@bp.route("/api/futu/connect", methods=["POST"])
def api_futu_connect():
    """Connect to FutuOpenD. Form params (host, port, unlock_password, rsa_file, acc_id) override config. Trading env is per-account (accounts.yaml)."""
    data = request.get_json(silent=True) or {}
    try:
        from zuilow.components.brokers.futu_gateway import FutuGateway, FutuConfig
        from pathlib import Path
        config_path = Path(__file__).parent.parent / "config" / "brokers" / "futu.yaml"
        config = FutuConfig.from_yaml(str(config_path))
        # Request params override config
        config.host = data.get("host") or config.host or "127.0.0.1"
        config.port = int(data.get("port", config.port or 11111))
        if "unlock_password" in data:
            config.unlock_password = data.get("unlock_password") or ""
        if "rsa_file" in data and data.get("rsa_file"):
            config.rsa_file = data["rsa_file"]
        if "acc_id" in data and data.get("acc_id"):
            try:
                config.acc_id = int(data["acc_id"])
            except (TypeError, ValueError):
                pass
        gateway = FutuGateway(config)
        if gateway.connect():
            set_futu_broker(gateway)
            real_names = [
                (a.get("name") or "").strip()
                for a in get_accounts_list()
                if (a.get("type") or "").lower() == "futu"
                and ((a.get("env") or "").strip().upper() == "REAL")
                and (a.get("name") or "").strip()
            ]
            gateway.set_real_account_names(real_names)
            return jsonify({"ok": True, "acc_id": gateway.current_account_id})
        return jsonify({"ok": False, "error": "Connect failed"}), 500
    except Exception as e:
        logger.exception("Futu connect failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/futu/disconnect", methods=["POST"])
def api_futu_disconnect():
    """Disconnect Futu."""
    broker = get_futu_broker()
    if broker:
        broker.disconnect()
        set_futu_broker(None)
    return jsonify({"ok": True})


def _load_futu_config_for_api() -> dict:
    """Load Futu config from futu.yaml for API (host, port, rsa_file). Trading env is per-account (accounts.yaml)."""
    from pathlib import Path
    import yaml
    config_path = Path(__file__).parent.parent / "config" / "brokers" / "futu.yaml"
    if not config_path.exists():
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return dict(data.get("futu", {}))
    except Exception:
        return {}


@bp.route("/api/futu/status")
def api_futu_status():
    """Futu connection status and config (for Brokers page form defaults)."""
    broker = get_futu_broker()
    cfg = _load_futu_config_for_api()
    return jsonify({
        "connected": broker.is_connected if broker else False,
        "acc_id": broker.current_account_id if broker and broker.is_connected else None,
        "config": {
            "host": cfg.get("host") or "127.0.0.1",
            "port": cfg.get("port") or 11111,
            "rsa_file": cfg.get("rsa_file") or "",
        },
    })


@bp.route("/api/brokers/ppt/config")
def api_brokers_ppt_config():
    broker = get_ppt_broker()
    if not broker:
        return jsonify({"configured": False, "base_url": None, "dms_base_url": None})
    base_url, dms_base_url = _ppt_broker_urls(broker)
    return jsonify({"configured": bool(base_url), "base_url": base_url, "dms_base_url": dms_base_url})


@bp.route("/api/brokers/ppt/status")
def api_brokers_ppt_status():
    """PPT broker status: connected (both PPT and DMS ok), ppt_ok, dms_ok, base_url, dms_base_url."""
    broker = get_ppt_broker()
    if not broker:
        return jsonify({"connected": False, "ppt_ok": False, "dms_ok": False, "base_url": None, "dms_base_url": None})
    base_url, dms_base_url = _ppt_broker_urls(broker)
    return jsonify({
        "connected": broker.is_connected,
        "ppt_ok": getattr(broker, "_ppt_ok", False),
        "dms_ok": getattr(broker, "_dms_ok", False),
        "base_url": base_url,
        "dms_base_url": dms_base_url,
    })


def _ppt_broker_urls(broker):
    """Return (base_url, dms_base_url) from broker config. In simulate, display base_url as localhost (PPT_DISPLAY_BASE_URL)."""
    base_url = (broker.config.base_url or "").strip().rstrip("/") or None
    if os.environ.get("PPT_DISPLAY_BASE_URL"):
        base_url = (os.environ.get("PPT_DISPLAY_BASE_URL") or "").strip().rstrip("/") or base_url
    dms_cfg = getattr(broker.config, "dms_config", None) or {}
    dms_base_url = (dms_cfg.get("base_url") or "").strip().rstrip("/") or None if isinstance(dms_cfg, dict) else None
    return base_url, dms_base_url


@bp.route("/api/brokers/ppt/connect", methods=["POST"])
def api_brokers_ppt_connect():
    """Connect PPT broker (PPT + DMS both must be reachable). Returns status and detail."""
    broker = get_ppt_broker()
    try:
        connected = broker.connect()
        ppt_ok = getattr(broker, "_ppt_ok", False)
        dms_ok = getattr(broker, "_dms_ok", False)
        base_url, dms_base_url = _ppt_broker_urls(broker)
        if connected:
            msg = "PPT broker connected (PPT and DMS reachable)."
        elif not ppt_ok and not dms_ok:
            msg = "PPT and DMS both unreachable. Check base_url and data source."
        elif not ppt_ok:
            msg = "PPT service unreachable. Check base_url in config/brokers/ppt.yaml."
        else:
            msg = "DMS (data source) unreachable. Check data source config."
        return jsonify({"ok": True, "connected": connected, "ppt_ok": ppt_ok, "dms_ok": dms_ok, "message": msg, "base_url": base_url, "dms_base_url": dms_base_url})
    except Exception as e:
        return jsonify({"ok": False, "connected": False, "message": str(e)}), 503


@bp.route("/api/brokers/ppt/disconnect", methods=["POST"])
def api_brokers_ppt_disconnect():
    """Disconnect PPT broker (really off until connect again)."""
    broker = get_ppt_broker()
    broker.disconnect()
    return jsonify({"ok": True, "connected": False})


@bp.route("/api/brokers/ppt/test")
def api_brokers_ppt_test():
    """Test PPT broker: run connect() and return connected, ppt_ok, dms_ok (same as Connect)."""
    broker = get_ppt_broker()
    try:
        connected = broker.connect()
        ppt_ok = getattr(broker, "_ppt_ok", False)
        dms_ok = getattr(broker, "_dms_ok", False)
        base_url, dms_base_url = _ppt_broker_urls(broker)
        payload = {"ok": connected, "connected": connected, "ppt_ok": ppt_ok, "dms_ok": dms_ok, "base_url": base_url, "dms_base_url": dms_base_url}
        if connected:
            payload["message"] = "PPT broker connected (PPT and DMS reachable)."
            return jsonify(payload)
        payload["message"] = "PPT unreachable." if not ppt_ok else "DMS unreachable."
        return jsonify(payload), 503
    except Exception as e:
        return jsonify({"ok": False, "connected": False, "message": str(e)}), 503


# ========== API: IBKR ==========

def _load_ibkr_config_for_api() -> dict:
    """Load IBKR config from ibkr.yaml for API (host, port, client_id, read_only, account)."""
    from pathlib import Path
    import yaml
    config_path = Path(__file__).parent.parent / "config" / "brokers" / "ibkr.yaml"
    if not config_path.exists():
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return dict(data.get("ibkr", {}))
    except Exception:
        return {}


def _ibkr_market_data_type_label(type_id: int) -> str:
    if type_id == 1:
        return "Live"
    if type_id == 2:
        return "Frozen"
    if type_id == 3:
        return "Delayed"
    if type_id == 4:
        return "Delayed frozen"
    return "Live"


@bp.route("/api/ibkr/status")
def api_ibkr_status():
    """IBKR connection status and config (for Brokers page). Includes market_data_type and read_only (live when connected)."""
    broker = get_ibkr_broker()
    cfg = _load_ibkr_config_for_api()
    connected = broker.is_connected if broker else False
    market_data_type = getattr(broker, "_market_data_type", 1) if broker else 1
    read_only = getattr(broker.config, "read_only", cfg.get("read_only", False)) if broker else cfg.get("read_only", False)
    return jsonify({
        "connected": connected,
        "config": {
            "host": cfg.get("host") or "127.0.0.1",
            "port": cfg.get("port") or 7497,
            "client_id": cfg.get("client_id", 1),
            "read_only": read_only,
            "account": cfg.get("account") or "",
        },
        "market_data_type": market_data_type,
        "market_data_type_label": _ibkr_market_data_type_label(market_data_type),
    })


@bp.route("/api/ibkr/read_only", methods=["POST"])
def api_ibkr_read_only():
    """Set IBKR read_only at runtime. Body: { \"read_only\": true|false }. Only when connected."""
    broker = get_ibkr_broker()
    if not broker or not getattr(broker, "is_connected", False):
        return jsonify({"ok": False, "error": "IBKR not connected"}), 503
    data = request.get_json(silent=True) or {}
    broker.config.read_only = bool(data.get("read_only", False))
    return jsonify({"ok": True, "read_only": broker.config.read_only})


@bp.route("/api/ibkr/market_data_type", methods=["POST"])
def api_ibkr_market_data_type():
    """Set IBKR market data type. Body: { \"type_id\": 1 } (Live) or { \"type_id\": 3 } (Delayed). Only when connected."""
    broker = get_ibkr_broker()
    if not broker or not getattr(broker, "is_connected", False):
        return jsonify({"ok": False, "error": "IBKR not connected"}), 503
    data = request.get_json(silent=True) or {}
    type_id = data.get("type_id")
    if type_id is None:
        return jsonify({"ok": False, "error": "Require type_id: 1=Live, 3=Delayed"}), 400
    try:
        type_id = int(type_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "type_id must be 1 or 3"}), 400
    if type_id not in (1, 2, 3, 4):
        return jsonify({"ok": False, "error": "type_id must be 1 (Live), 2 (Frozen), 3 (Delayed), 4 (Delayed frozen)"}), 400
    broker.set_market_data_type(type_id)
    return jsonify({"ok": True, "market_data_type": type_id, "market_data_type_label": _ibkr_market_data_type_label(type_id)})


@bp.route("/api/ibkr/connect", methods=["POST"])
def api_ibkr_connect():
    """Connect to TWS/IB Gateway. Body: host, port (optional, override config)."""
    _ibkr_ensure_event_loop()
    data = request.get_json(silent=True) or {}
    try:
        from zuilow.components.brokers.ibkr_gateway import IbkrGateway, IbkrConfig
        from pathlib import Path
        config_path = Path(__file__).parent.parent / "config" / "brokers" / "ibkr.yaml"
        config = IbkrConfig.from_yaml(str(config_path))
        config.host = data.get("host") or config.host or "127.0.0.1"
        config.port = int(data.get("port", config.port or 7497))
        if "client_id" in data and data["client_id"] is not None:
            config.client_id = int(data["client_id"])
        if "read_only" in data:
            config.read_only = bool(data["read_only"])
        if "account" in data:
            config.account = (data.get("account") or "").strip()
        gateway = IbkrGateway(config)
        if gateway.connect(host=config.host, port=config.port):
            set_ibkr_broker(gateway)
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Connect failed (timeout or refused)"}), 500
    except OSError as e:
        logger.exception("IBKR connect failed: %s", e)
        msg = str(e)
        if getattr(e, "errno", None) == 113:
            msg += ". No route to host – check TWS/Gateway is running on the target and API port is open."
        elif getattr(e, "errno", None) == 111:
            msg += ". Connection refused – is TWS/Gateway listening on this port?"
        return jsonify({"ok": False, "error": msg}), 500
    except Exception as e:
        logger.exception("IBKR connect failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/ibkr/disconnect", methods=["POST"])
def api_ibkr_disconnect():
    """Disconnect IBKR."""
    broker = get_ibkr_broker()
    if broker:
        broker.disconnect()
        set_ibkr_broker(None)
    return jsonify({"ok": True})


def _ibkr_ensure_event_loop() -> None:
    """Ensure current thread has an asyncio event loop (ib_insync needs it; Flask request runs in a thread that has none)."""
    import asyncio
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _ibkr_account_id_from_request() -> str | None:
    """Resolve IBKR account id from query/body account name (config/accounts.yaml). None = use broker default."""
    account_name = (request.args.get("account") or (request.get_json(silent=True) or {}).get("account") or "").strip()
    if not account_name:
        return None
    acc_cfg = get_account_by_name(account_name)
    if not acc_cfg or (acc_cfg.get("type") or "").lower() != "ibkr":
        return None
    return (acc_cfg.get("ibkr_account_id") or acc_cfg.get("ibkr_account") or "").strip() or None


@bp.route("/api/ibkr/account")
def api_ibkr_account():
    """IBKR account info. Optional query: account=<name> (from config/accounts.yaml)."""
    _ibkr_ensure_event_loop()
    broker = get_ibkr_broker()
    if not broker or not getattr(broker, "is_connected", False):
        return jsonify({"error": "Not connected"}), 503
    acc_id = _ibkr_account_id_from_request()
    info = broker.get_account_info(account=acc_id)
    if info is None:
        return jsonify({"error": "Query failed"}), 500
    return jsonify(info)


@bp.route("/api/ibkr/positions")
def api_ibkr_positions():
    """IBKR positions. Optional query: account=<name>."""
    _ibkr_ensure_event_loop()
    broker = get_ibkr_broker()
    if not broker or not getattr(broker, "is_connected", False):
        return jsonify({"error": "Not connected"}), 503
    acc_id = _ibkr_account_id_from_request()
    positions = broker.get_positions(account=acc_id)
    return jsonify({"positions": positions or []})


@bp.route("/api/ibkr/orders")
def api_ibkr_orders():
    """IBKR orders. Optional query: account=<name>."""
    _ibkr_ensure_event_loop()
    broker = get_ibkr_broker()
    if not broker or not getattr(broker, "is_connected", False):
        return jsonify({"error": "Not connected"}), 503
    acc_id = _ibkr_account_id_from_request()
    orders = broker.get_orders(account=acc_id)
    return jsonify({"orders": orders or []})


@bp.route("/api/ibkr/order", methods=["POST"])
def api_ibkr_order_post():
    """Place IBKR order. Body: symbol, side (buy|sell), quantity, price (optional). Optional body: account=<name>."""
    _ibkr_ensure_event_loop()
    broker = get_ibkr_broker()
    if not broker or not getattr(broker, "is_connected", False):
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip()
    side = (data.get("side") or "buy").lower()
    quantity = int(data.get("quantity", 0))
    price = data.get("price")
    if not symbol or quantity <= 0:
        return jsonify({"error": "Invalid symbol or quantity"}), 400
    acc_id = _ibkr_account_id_from_request()
    try:
        from zuilow.components.backtest.types import OrderSide
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        order_id = broker.place_order(symbol=symbol, side=order_side, quantity=quantity, price=price, account=acc_id)
        if order_id:
            return jsonify({"ok": True, "order_id": order_id})
        return jsonify({"ok": False, "error": "Place order failed"}), 500
    except Exception as e:
        logger.exception("IBKR place order failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/ibkr/order/<order_id>", methods=["DELETE"])
def api_ibkr_order_delete(order_id):
    """Cancel IBKR order. Optional query or body: account=<name>."""
    _ibkr_ensure_event_loop()
    broker = get_ibkr_broker()
    if not broker or not getattr(broker, "is_connected", False):
        return jsonify({"error": "Not connected"}), 503
    acc_id = _ibkr_account_id_from_request()
    try:
        if broker.cancel_order(order_id, account=acc_id):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "Cancel failed"}), 500
    except Exception as e:
        logger.exception("IBKR cancel order failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/brokers/futu/accounts")
def api_brokers_futu_accounts():
    """List Futu Real accounts (from accounts.yaml env=REAL) with enabled state (from gateway; requires connected)."""
    broker = get_futu_broker()
    accounts = [
        {
            "name": (a.get("name") or "").strip(),
            "enabled": broker.is_account_enabled((a.get("name") or "").strip()) if (broker and broker.is_connected) else False,
        }
        for a in get_accounts_list()
        if (a.get("type") or "").lower() == "futu" and ((a.get("env") or "").strip().upper() == "REAL")
        and (a.get("name") or "").strip()
    ]
    return jsonify({"accounts": accounts})


@bp.route("/api/brokers/futu/accounts/<name>/enabled", methods=["POST"])
def api_brokers_futu_account_enabled(name):
    """Set enabled state for a Futu Real account. Body: { \"enabled\": true|false }. Requires Futu connected."""
    broker = get_futu_broker()
    if not broker or not broker.is_connected:
        return jsonify({"error": "Futu not connected"}), 503
    acc_cfg = get_account_by_name(name)
    if not acc_cfg or (acc_cfg.get("type") or "").lower() != "futu" or (acc_cfg.get("env") or "").strip().upper() != "REAL":
        return jsonify({"error": "Not a Futu Real account"}), 400
    data = request.get_json(silent=True) or {}
    enabled = data.get("enabled")
    if enabled is None:
        return jsonify({"error": "Missing 'enabled' (true|false)"}), 400
    broker.set_account_enabled(name, bool(enabled))
    return jsonify({"ok": True, "enabled": bool(enabled)})


def _futu_acc_id_from_request() -> int | None:
    """Resolve Futu acc_id from query/body account name (config/accounts.yaml). None = use broker default."""
    acc_id, _ = _futu_acc_id_and_env_from_request()
    return acc_id


def _futu_acc_id_and_env_from_request() -> tuple[int | None, str | None]:
    """
    Resolve (acc_id, trd_env) from query/body account name (config/accounts.yaml).
    trd_env: account's env (REAL/SIMULATE) from accounts.yaml; None = use broker config default.
    """
    account_name = (request.args.get("account") or (request.get_json(silent=True) or {}).get("account") or "").strip()
    if not account_name:
        return None, None
    acc_cfg = get_account_by_name(account_name)
    if not acc_cfg or (acc_cfg.get("type") or "").lower() != "futu":
        return None, None
    fid = acc_cfg.get("futu_acc_id")
    acc_id = int(fid) if fid is not None else None
    env_raw = (acc_cfg.get("env") or "").strip().upper()
    trd_env = env_raw if env_raw in ("REAL", "SIMULATE") else None
    return acc_id, trd_env


def _futu_account_name_from_request() -> str | None:
    """Current request account name (query or body)."""
    return (request.args.get("account") or (request.get_json(silent=True) or {}).get("account") or "").strip() or None


def _futu_validate_acc_id(broker, acc_id: int | None, account_name: str | None, trd_env: str | None = None):
    """
    If acc_id is set and no per-account trd_env, check acc_id exists in current connection (get_account_list).
    When trd_env is set (per-account env), skip validation so Real/Simulate accounts both work.
    Return (True, None) or (False, (response, status_code)).
    """
    if acc_id is None:
        return True, None
    if trd_env:
        return True, None  # per-account env: use it for request, skip list validation
    try:
        acc_list = broker.get_account_list()
    except Exception:
        return True, None
    valid_ids = {int(a.get("acc_id")) for a in acc_list if a.get("acc_id") is not None}
    if acc_id in valid_ids:
        return True, None
    label = account_name or f"acc_id={acc_id}"
    msg = (
        f"Account '{label}' is not available in the current Futu connection. "
        "Connect OpenD with the correct market/env on the Brokers page (e.g. Simulate for paper accounts), or choose another account."
    )
    return False, (jsonify({"error": msg, "code": "acc_id_not_available", "available_acc_ids": list(valid_ids)}), 400)


@bp.route("/api/futu/account")
def api_futu_account():
    """Futu account info. Optional query: account=<name>. Uses account's env (REAL/SIMULATE) from config."""
    broker = get_futu_broker()
    if not broker or not broker.is_connected:
        return jsonify({"error": "Not connected"}), 503
    acc_id, trd_env = _futu_acc_id_and_env_from_request()
    account_name = _futu_account_name_from_request()
    ok, err = _futu_validate_acc_id(broker, acc_id, account_name, trd_env=trd_env)
    if not ok:
        return err
    info = broker.get_account_info(acc_id=acc_id, trd_env=trd_env)
    if info is None:
        return jsonify({"error": "Query failed"}), 500
    return jsonify(info)


@bp.route("/api/futu/positions")
def api_futu_positions():
    """Futu positions. Optional query: account=<name>. Uses account's env (REAL/SIMULATE) from config."""
    broker = get_futu_broker()
    if not broker or not broker.is_connected:
        return jsonify({"error": "Not connected"}), 503
    acc_id, trd_env = _futu_acc_id_and_env_from_request()
    account_name = _futu_account_name_from_request()
    ok, err = _futu_validate_acc_id(broker, acc_id, account_name, trd_env=trd_env)
    if not ok:
        return err
    positions = broker.get_positions(acc_id=acc_id, trd_env=trd_env)
    return jsonify({"positions": positions or []})


@bp.route("/api/futu/orders")
def api_futu_orders():
    """Futu order list. Optional query: account=<name>. Uses account's env (REAL/SIMULATE) from config."""
    broker = get_futu_broker()
    if not broker or not broker.is_connected:
        return jsonify({"error": "Not connected"}), 503
    acc_id, trd_env = _futu_acc_id_and_env_from_request()
    account_name = _futu_account_name_from_request()
    ok, err = _futu_validate_acc_id(broker, acc_id, account_name, trd_env=trd_env)
    if not ok:
        return err
    orders = broker.get_orders(acc_id=acc_id, trd_env=trd_env)
    return jsonify({"orders": orders or []})


@bp.route("/api/futu/order", methods=["POST"])
def api_futu_order_post():
    """Place Futu order. Optional body: account=<name>. Uses account's env (REAL/SIMULATE) from config."""
    broker = get_futu_broker()
    if not broker or not broker.is_connected:
        return jsonify({"error": "Not connected"}), 503
    data = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "").strip()
    side = (data.get("side") or "buy").lower()
    quantity = int(data.get("quantity", 0))
    price = data.get("price")
    if not symbol or quantity <= 0:
        return jsonify({"error": "Invalid symbol or quantity"}), 400
    acc_id, trd_env = _futu_acc_id_and_env_from_request()
    account_name = _futu_account_name_from_request()
    ok, err = _futu_validate_acc_id(broker, acc_id, account_name, trd_env=trd_env)
    if not ok:
        return err
    from zuilow.components.backtest.types import OrderSide
    order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
    try:
        order_id = broker.place_order(symbol=symbol, side=order_side, quantity=quantity, price=price, acc_id=acc_id, trd_env=trd_env, account_name=account_name)
        if order_id:
            return jsonify({"ok": True, "order_id": order_id})
        if account_name and not broker.is_account_enabled(account_name):
            return jsonify({"error": "Account is disabled for trading. Enable it on the Brokers page."}), 403
        return jsonify({"ok": False, "error": "Place order failed"}), 500
    except Exception as e:
        logger.exception("Place order failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/futu/order/<order_id>", methods=["DELETE"])
def api_futu_order_delete(order_id):
    """Cancel Futu order. Optional query: account=<name>. Uses account's env (REAL/SIMULATE) from config."""
    broker = get_futu_broker()
    if not broker or not broker.is_connected:
        return jsonify({"error": "Not connected"}), 503
    acc_id, trd_env = _futu_acc_id_and_env_from_request()
    account_name = _futu_account_name_from_request()
    ok, err = _futu_validate_acc_id(broker, acc_id, account_name, trd_env=trd_env)
    if not ok:
        return err
    if broker.cancel_order(order_id, acc_id=acc_id, trd_env=trd_env):
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Cancel failed"}), 500


# ========== API: market ==========

@bp.route("/api/market/quote/<path:symbol>")
def api_market_quote(symbol):
    """Get quote for symbol. In sim mode (SIMULATION_TIME_URL): DataSourceManager with current sim time (or as_of override). Else: market first then DataSourceManager."""
    mgr = _get_datasource_manager()

    if ctrl.is_sim_mode():
        as_of_dt = ctrl.get_time_dt()
        if mgr and as_of_dt is not None:
            try:
                quote = mgr.get_quote(symbol, as_of=as_of_dt)
                if quote and (quote.get("price") is not None or quote.get("Close") is not None or quote.get("valid") is True):
                    if "price" not in quote and "Close" in quote:
                        quote = dict(quote)
                        quote["price"] = quote["Close"]
                    return jsonify(quote)
            except Exception as e:
                logger.debug("Datasource manager get_quote %s as_of=%s: %s", symbol, as_of_dt, e)
        else:
            logger.warning("No sim time available for %s", symbol)

        return jsonify({"symbol": symbol, "error": "No data source or sim time unavailable"}), 503

    # Route by account type: paper -> PptGateway (DMS), futu/ibkr -> Futu/IBKR
    account_name = _live_account_from_request()
    acc_type = None
    if account_name:
        acc_cfg = get_account_by_name(account_name)
        if acc_cfg:
            acc_type = (acc_cfg.get("type") or "paper").lower()
    if acc_type == "paper":
        broker = get_ppt_broker()
        if broker and getattr(broker, "is_connected", False):
            try:
                quote = broker.get_quote(symbol)
                if quote and quote.get("error") is None and (quote.get("price") is not None or quote.get("Close") is not None):
                    if "price" not in quote and "Close" in quote:
                        quote = dict(quote)
                        quote["price"] = quote["Close"]
                    return jsonify(quote)
            except Exception as e:
                logger.debug("PptGateway get_quote %s: %s", symbol, e)
        return jsonify({"symbol": symbol, "error": "Paper broker (DMS) no data"}), 503

    broker = None
    if acc_type == "futu":
        broker = get_futu_broker()
    elif acc_type == "ibkr":
        broker = get_ibkr_broker()
    if broker is None:
        broker = get_futu_broker()
        if not (broker and getattr(broker, "is_connected", False)):
            broker = get_ibkr_broker()
    if broker and getattr(broker, "is_connected", False):
        try:
            quote = broker.get_quote(symbol)
            if quote and quote.get("error") is None and (quote.get("price") is not None or quote.get("Close") is not None):
                if "price" not in quote and "Close" in quote:
                    quote = dict(quote)
                    quote["price"] = quote["Close"]
                return jsonify(quote)
        except Exception as e:
            logger.debug("Broker get_quote %s: %s", symbol, e)

    logger.warning("Market service not connected or no data for %s; falling back to DataSourceManager", symbol)
    if mgr:
        try:
            quote = mgr.get_quote(symbol)
            if quote and (quote.get("price") is not None or quote.get("Close") is not None or quote.get("valid") is True):
                if "price" not in quote and "Close" in quote:
                    quote = dict(quote)
                    quote["price"] = quote["Close"]
                return jsonify(quote)
        except Exception as e:
            logger.debug("Datasource manager get_quote %s: %s", symbol, e)
    return jsonify({"symbol": symbol, "error": "No data source"}), 503


@bp.route("/api/market/history")
def api_market_history():
    """Get OHLCV history. In sim mode cap at ctrl time (as_of). Broker first; fallback DataSourceManager."""
    from datetime import datetime as dt
    symbol = (request.args.get("symbol") or "").strip()
    start_s = (request.args.get("start") or "").strip()
    end_s = (request.args.get("end") or "").strip()
    ktype = (request.args.get("ktype") or "K_DAY").strip()
    if not symbol or not start_s or not end_s:
        return jsonify({"error": "missing symbol, start, or end"}), 400
    try:
        start_dt = dt.strptime(start_s[:10], "%Y-%m-%d")
        end_dt = dt.strptime(end_s[:10], "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "start/end must be YYYY-MM-DD"}), 400
    interval_map = {"K_DAY": "1d", "K_1M": "1m", "K_5M": "5m", "K_15M": "15m", "K_30M": "30m", "K_60M": "60m", "K_WEEK": "1wk", "K_MON": "1mo"}
    interval = interval_map.get(ktype, "1d")

    as_of_dt = None
    if ctrl.is_sim_mode():
        as_of_dt = ctrl.get_time_dt()
        if as_of_dt is not None and end_dt.date() > as_of_dt.date():
            end_dt = dt.combine(as_of_dt.date(), dt.min.time())
            end_s = end_dt.strftime("%Y-%m-%d")

    # Paper broker: history from PptGateway (DMS)
    account_name = _live_account_from_request()
    acc_type = None
    if account_name:
        acc_cfg = get_account_by_name(account_name)
        if acc_cfg:
            acc_type = (acc_cfg.get("type") or "paper").lower()
    if acc_type == "paper":
        broker = get_ppt_broker()
        if broker and getattr(broker, "is_connected", False):
            try:
                data = broker.get_history(symbol, start_s, end_s, ktype)
                if data is not None and not data.empty:
                    return jsonify({"data": data.to_dict("records")})
            except Exception as e:
                logger.debug("PptGateway get_history %s: %s", symbol, e)
        return jsonify({"error": "No history data (DMS)"}), 404

    svc = get_market_service()
    if svc and svc.futu_connected:
        data = svc.get_history(symbol, start_s, end_s, ktype)
        if data is not None and not data.empty:
            return jsonify({"data": data.to_dict("records")})

    mgr = _get_datasource_manager()
    if mgr:
        try:
            df = mgr.get_history(symbol, start_dt, end_dt, interval, as_of=as_of_dt)
            if df is not None and not df.empty:
                return jsonify({"data": df.to_dict("records")})
        except Exception as e:
            logger.debug("Datasource manager get_history %s: %s", symbol, e)
    return jsonify({"error": "No history data"}), 404


@bp.route("/api/market/status")
def api_market_status():
    """Market data source status (broker-only)."""
    broker = get_futu_broker()
    futu_connected = broker.is_connected if broker else False
    routing = {"HK.*": "Futu" if futu_connected else "--", "US.*": "Futu" if futu_connected else "--"}
    return jsonify({
        "futu_connected": futu_connected,
        "routing": routing,
    })
