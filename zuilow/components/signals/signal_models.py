"""
Trading signal models (order, rebalance, allocation).

First-class entities for strategy output: single order (buy/sell), rebalance (target_weights / target_mv),
or allocation (target_weights only, 资产配置). Used by scheduler runner (write to store) and execution (consume pending).

Classes:
    SignalKind       Enum: ORDER, REBALANCE, ALLOCATION
    SignalStatus     Enum: PENDING, EXECUTED, FAILED, CANCELLED
    TradingSignal    Dataclass: job_name, account, market, kind, payload, status, symbol?, trigger_at?

TradingSignal factory methods:
    .order(...) -> TradingSignal
    .rebalance(...) -> TradingSignal
    .allocation(job_name, account, market, target_weights, trigger_at=None) -> TradingSignal

TradingSignal payload:
    order:      {side, qty, price?, reason}
    rebalance:  {target_weights} or {target_mv}
    allocation: {target_weights: {symbol: weight 0..1}}  # 资产配置，执行器按账户权益与持仓计算调仓订单
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from zuilow.components.control import ctrl


class SignalKind(Enum):
    """Signal kind: single order, rebalance, or allocation (资产配置)."""
    ORDER = "order"             # Direct buy/sell
    REBALANCE = "rebalance"     # Target weights or target_mv; executor computes orders
    ALLOCATION = "allocation"   # 资产配置: target_weights only; executor computes orders from equity/positions


class SignalStatus(Enum):
    """Signal lifecycle status."""
    PENDING = "pending"
    EXECUTED = "executed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TradingSignal:
    """
    Stored trading signal (first-class entity).

    Supports:
    - ORDER: single symbol, side, qty; optional price and reason
    - REBALANCE: target_weights or target_mv; executor generates orders from account equity/positions
    - ALLOCATION: target_weights only (资产配置); same execution as rebalance by weights

    Attributes:
        id: Optional DB id (set after insert)
        job_name: Scheduler job name that produced this signal
        account: Account name (from config/accounts.yaml)
        market: Market code (e.g. HK, US)
        kind: order | rebalance | allocation
        symbol: Single symbol for order; optional for rebalance (multi-symbol in payload)
        payload: For order: {side, qty, price?}; for rebalance/allocation: {target_weights?}, {target_mv?}
        status: pending | executed | failed | cancelled
        created_at: When signal was produced
        executed_at: When executed (if any)
        trigger_at: Optional desired execution time (e.g. market open)
    """
    job_name: str
    account: str
    market: str
    kind: SignalKind
    payload: dict[str, Any]
    status: SignalStatus = SignalStatus.PENDING
    created_at: datetime = field(default_factory=lambda: ctrl.get_current_dt())
    executed_at: datetime | None = None
    trigger_at: datetime | None = None
    id: int | None = None
    symbol: str | None = None  # For order: required; for rebalance: optional

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize to dict (for API / JSON).

        Returns:
            Dict with id, job_name, account, market, kind, symbol, payload, status,
            created_at, executed_at, trigger_at (ISO strings).
        """
        return {
            "id": self.id,
            "job_name": self.job_name,
            "account": self.account,
            "market": self.market,
            "kind": self.kind.value,
            "symbol": self.symbol,
            "payload": self.payload,
            "status": self.status.value,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,
            "trigger_at": self.trigger_at.isoformat() if self.trigger_at else None,
        }

    @classmethod
    def order(
        cls,
        job_name: str,
        account: str,
        market: str,
        symbol: str,
        side: str,
        qty: float,
        price: float | None = None,
        reason: str = "",
        trigger_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> TradingSignal:
        """Create an order-type signal. created_at: when set (e.g. sim time), used as signal creation time."""
        payload: dict[str, Any] = {"side": side, "qty": qty, "reason": reason}
        if price is not None:
            payload["price"] = price
        return cls(
            job_name=job_name,
            account=account,
            market=market,
            kind=SignalKind.ORDER,
            symbol=symbol,
            payload=payload,
            trigger_at=trigger_at,
            created_at=created_at if created_at is not None else ctrl.get_current_dt(),
        )

    @classmethod
    def rebalance(
        cls,
        job_name: str,
        account: str,
        market: str,
        payload: dict[str, Any],
        trigger_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> TradingSignal:
        """
        Create a rebalance-type signal.

        Args:
            job_name: Scheduler job name
            account: Account name
            market: Market code
            payload: target_weights (symbol -> weight 0..1) or target_mv (symbol -> target market value)
            trigger_at: Optional desired execution time
            created_at: When set (e.g. sim time), used as signal creation time
        """
        return cls(
            job_name=job_name,
            account=account,
            market=market,
            kind=SignalKind.REBALANCE,
            symbol=None,
            payload=payload,
            trigger_at=trigger_at,
            created_at=created_at if created_at is not None else ctrl.get_current_dt(),
        )

    @classmethod
    def allocation(
        cls,
        job_name: str,
        account: str,
        market: str,
        target_weights: dict[str, float],
        trigger_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> TradingSignal:
        """
        Create an allocation-type signal (资产配置).

        Executor uses account equity and positions to compute target qty per symbol
        (target_value = equity * weight, target_qty = target_value / price), then sends
        buy/sell orders to match target weights. Same execution logic as rebalance with target_weights.

        Args:
            job_name: Scheduler job name
            account: Account name
            market: Market code
            target_weights: Symbol -> weight 0..1 (e.g. {"AAPL": 0.2, "GOOGL": 0.4, "AMD": 0.4})
            trigger_at: Optional desired execution time
            created_at: When set (e.g. sim time), used as signal creation time
        """
        return cls(
            job_name=job_name,
            account=account,
            market=market,
            kind=SignalKind.ALLOCATION,
            symbol=None,
            payload={"target_weights": dict(target_weights)},
            trigger_at=trigger_at,
            created_at=created_at if created_at is not None else ctrl.get_current_dt(),
        )
