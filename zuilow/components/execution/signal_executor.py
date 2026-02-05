"""
Signal executor: consume pending signals from store and send orders via ZuiLow API.

Order-type: POST /api/order with symbol, side, qty, account; update signal status to executed/failed.
Rebalance-type: GET /api/account for equity/positions; compute target from payload (target_weights or
target_mv); send multiple POST /api/order; update signal status.

Classes:
    SignalExecutor   Execute pending signals for given account/market

SignalExecutor methods:
    .run_once(account: Optional[str] = None, market: Optional[str] = None) -> dict
        Returns {executed, failed, errors}. Fetches pending from store (trigger_at_before=now), executes each.
    ._execute_order(signal, store) -> bool   (order-type: POST /api/order)
    ._execute_rebalance(signal, store) -> bool   (rebalance: target_weights or target_mv -> orders)

SignalExecutor config:
    api_base_url: str = "http://localhost:11180"
    timeout: int = 10

SignalExecutor features:
    - Fetches pending via store.list_pending(account, market, trigger_at_before=now)
    - ORDER: single POST /api/order; updates status to EXECUTED or FAILED
    - REBALANCE: GET /api/account; compute target qty from target_weights or target_mv; send orders; update status
    - Returns summary {executed, failed, errors} from run_once

Functions:
    get_signal_executor(api_base_url="http://localhost:11180") -> SignalExecutor
    set_signal_executor(executor: Optional[SignalExecutor]) -> None
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import requests

from zuilow.components.control.ctrl import get_current_dt
from zuilow.components.signals import (
    TradingSignal,
    SignalKind,
    SignalStatus,
    get_signal_store,
)

logger = logging.getLogger(__name__)


class SignalExecutor:
    """
    Execute pending signals: read from store, send orders via ZuiLow API, update status.

    Supports:
    - ORDER: POST /api/order with symbol, side, qty, account; update signal to EXECUTED/FAILED
    - REBALANCE: GET /api/account; compute target from target_weights or target_mv; send orders; update status

    Features:
    - run_once(account, market) filters pending by account/market and trigger_at_before=now
    - Returns {executed, failed, errors} summary
    """

    def __init__(self, api_base_url: str = "http://localhost:11180", timeout: int = 10):
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = timeout

    def run_once(
        self,
        account: Optional[str] = None,
        market: Optional[str] = None,
        trigger_at: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """
        Run one execution cycle: fetch pending signals, execute each, update status.

        Args:
            account: Optional filter by account
            market: Optional filter by market
            trigger_at: Optional sim time for this run; when set, used as trigger_at_before and sent as X-Simulation-Time to /api/order

        Returns:
            Dict with keys: executed (int), failed (int), errors (list of str)
        """
        store = get_signal_store()
        now = trigger_at if trigger_at is not None else get_current_dt()
        pending = store.list_pending(
            account=account,
            market=market,
            trigger_at_before=now,
        )
        pending_count = len(pending)
        executed = 0
        failed = 0
        errors: list[str] = []
        for sig in pending:
            if sig.kind == SignalKind.ORDER:
                ok = self._execute_order(sig, store, trigger_at=trigger_at)
            elif sig.kind in (SignalKind.REBALANCE, SignalKind.ALLOCATION):
                ok = self._execute_rebalance(sig, store, trigger_at=trigger_at)
            else:
                ok = False
                if sig.id:
                    store.update_status(sig.id, SignalStatus.FAILED)
                logger.warning("Unknown signal kind: %s", sig.kind)
            if ok:
                executed += 1
            else:
                failed += 1
                if sig.id:
                    errors.append(f"signal_id={sig.id} {sig.job_name}")
        return {"pending": pending_count, "executed": executed, "failed": failed, "errors": errors}

    def _execute_order(self, signal: TradingSignal, store: Any, trigger_at: Optional[datetime] = None) -> bool:
        """
        Execute single order-type signal: POST /api/order, update status.

        Args:
            signal: ORDER-type TradingSignal with symbol and payload {side, qty, price?}
            store: SignalStore for update_status
            trigger_at: Optional sim time (for executed_at); X-Simulation-Time is filled by /api/order from tick context.

        Returns:
            True if order succeeded (HTTP 200), else False (status set to FAILED)
        """
        if not signal.symbol or signal.kind != SignalKind.ORDER:
            if signal.id:
                store.update_status(signal.id, SignalStatus.FAILED)
            return False
        payload = signal.payload
        side = (payload.get("side") or "buy").lower()
        qty = float(payload.get("qty", 0))
        if qty <= 0:
            if signal.id:
                store.update_status(signal.id, SignalStatus.FAILED)
            return False
        body: dict[str, Any] = {
            "symbol": signal.symbol,
            "side": side,
            "qty": qty,
            "account": signal.account,
        }
        if payload.get("price") is not None:
            body["price"] = float(payload["price"])
        # X-Simulation-Time: filled by /api/order from tick context (set once at tick entry)
        try:
            url = f"{self.api_base_url}/api/order"
            r = requests.post(url, json=body, timeout=self.timeout)
            if r.status_code == 200:
                if signal.id:
                    store.update_status(signal.id, SignalStatus.EXECUTED, executed_at=trigger_at or get_current_dt())
                logger.info(f"Executed order signal id={signal.id} {signal.symbol} {side} {qty}")
                return True
            logger.warning(f"Order API returned {r.status_code}: {r.text}")
        except Exception as e:
            logger.error(f"Execute order failed: {e}")
        if signal.id:
            store.update_status(signal.id, SignalStatus.FAILED)
        return False

    def _execute_rebalance(self, signal: TradingSignal, store: Any, trigger_at: Optional[datetime] = None) -> bool:
        """
        Execute rebalance/allocation: get account equity/positions, compute target from payload, send orders.

        Supports REBALANCE (target_weights or target_mv) and ALLOCATION (target_weights only).
        Logic: target_value = equity * weight, target_qty = target_value / price; for each symbol
        diff = target_qty - current_qty -> buy or sell to match target weights.

        Args:
            signal: REBALANCE or ALLOCATION TradingSignal with payload {target_weights} or {target_mv}
            store: SignalStore for update_status
            trigger_at: Optional sim time (for executed_at).

        Returns:
            True if all orders succeeded, else False (signal status set to EXECUTED or FAILED)
        """
        if signal.kind not in (SignalKind.REBALANCE, SignalKind.ALLOCATION):
            if signal.id:
                store.update_status(signal.id, SignalStatus.FAILED)
            return False
        payload = signal.payload
        target_weights = payload.get("target_weights")  # { symbol: weight 0..1 }
        target_mv = payload.get("target_mv")            # { symbol: target_market_value }
        if not target_weights and not target_mv:
            logger.warning("Rebalance payload missing target_weights or target_mv")
            if signal.id:
                store.update_status(signal.id, SignalStatus.FAILED)
            return False
        logger.info(
            "Executing rebalance signal job_name=%s account=%s market=%s (target_weights=%s target_mv=%s)",
            signal.job_name, signal.account, signal.market,
            bool(target_weights), bool(target_mv),
        )
        try:
            equity, positions = self._fetch_account_positions(signal.account)
        except Exception as e:
            logger.error(f"Fetch account failed: {e}")
            if signal.id:
                store.update_status(signal.id, SignalStatus.FAILED)
            return False
        if equity <= 0:
            if signal.id:
                store.update_status(signal.id, SignalStatus.FAILED)
            return False
        current = {}
        for p in positions:
            sym = p.get("symbol", "")
            qty = p.get("quantity", p.get("qty", 0))
            price = p.get("current_price", p.get("avg_price", 0))
            current[sym] = {"qty": float(qty), "price": float(price)}
        target_qty: dict[str, float] = {}
        if target_weights:
            for sym, w in target_weights.items():
                target_value = equity * float(w)
                price = current.get(sym, {}).get("price") or 0
                if price <= 0:
                    price = self._fetch_quote_price(sym, signal.account)
                target_qty[sym] = (target_value / price) if price > 0 else 0
        else:
            for sym, mv in target_mv.items():
                price = current.get(sym, {}).get("price") or 0
                if price <= 0:
                    price = self._fetch_quote_price(sym, signal.account)
                target_qty[sym] = (float(mv) / price) if price > 0 else 0
        all_symbols = set(current.keys()) | set(target_qty.keys())
        ok = True
        for symbol in all_symbols:
            cur_q = current.get(symbol, {}).get("qty", 0) or 0
            tgt_q = target_qty.get(symbol, 0) or 0
            diff = tgt_q - cur_q
            if abs(diff) < 1e-6:
                continue
            side = "buy" if diff > 0 else "sell"
            qty = abs(diff)
            body: dict[str, Any] = {
                "symbol": symbol,
                "side": side,
                "qty": round(qty, 4),
                "account": signal.account,
            }
            try:
                r = requests.post(
                    f"{self.api_base_url}/api/order",
                    json=body,
                    timeout=self.timeout,
                )
                if r.status_code != 200:
                    ok = False
                    logger.warning(f"Rebalance order {symbol} {side} {qty} -> {r.status_code}")
            except Exception as e:
                logger.error(f"Rebalance order failed: {e}")
                ok = False
        if signal.id:
            store.update_status(
                signal.id,
                SignalStatus.EXECUTED if ok else SignalStatus.FAILED,
                executed_at=(trigger_at or get_current_dt()) if ok else None,
            )
        return ok

    def _fetch_quote_price(self, symbol: str, account: Optional[str] = None) -> float:
        """GET /api/market/quote/<symbol> -> price. Pass account so quote uses same gateway as order (avoid wrong broker)."""
        try:
            url = f"{self.api_base_url}/api/market/quote/{requests.utils.quote(symbol, safe='')}"
            if account:
                url = f"{url}?account={requests.utils.quote(account, safe='')}"
            r = requests.get(url, timeout=self.timeout)
            if r.status_code != 200:
                return 0.0
            data = r.json()
            if data.get("error"):
                return 0.0
            return float(data.get("price") or data.get("Close") or 0)
        except Exception as e:
            logger.debug("Quote for %s failed: %s", symbol, e)
            return 0.0

    def _fetch_account_positions(self, account: str) -> tuple[float, list[dict]]:
        """GET /api/account?account=... -> (equity, positions list)."""
        url = f"{self.api_base_url}/api/account"
        r = requests.get(url, params={"account": account}, timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"Account API {r.status_code}: {r.text}")
        data = r.json()
        if "error" in data:
            raise RuntimeError(data["error"])
        equity = float(data.get("equity", data.get("total_value", 0)))
        positions = data.get("positions", [])
        return equity, positions


_executor: Optional[SignalExecutor] = None


def get_signal_executor(api_base_url: str = "http://localhost:11180") -> SignalExecutor:
    """
    Get global SignalExecutor instance (singleton).

    Args:
        api_base_url: ZuiLow API base URL (default http://localhost:11180)

    Returns:
        SignalExecutor instance
    """
    global _executor
    if _executor is None:
        _executor = SignalExecutor(api_base_url=api_base_url)
    return _executor


def set_signal_executor(executor: Optional[SignalExecutor]) -> None:
    """
    Set global SignalExecutor (e.g. for tests).

    Args:
        executor: SignalExecutor instance or None to reset
    """
    global _executor
    _executor = executor
