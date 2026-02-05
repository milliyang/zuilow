"""
Simulated broker: order execution and position management for backtest.

Classes:
    FillMode         Enum: IMMEDIATE (fill at current bar), NEXT_BAR (fill at next bar)
    BrokerConfig     commission_rate, min_commission, slippage, fill_mode
    SimulatedBroker   Place and fill orders; manage positions and account

SimulatedBroker methods:
    .place_order(symbol, side, quantity, price=None) -> Optional[str]   (order_id)
    .get_positions() -> list[Position]
    .get_account_info() -> dict   (cash, total_assets, market_val, ...)
    .get_order_status(order_id) -> Optional[OrderStatus]
    .cancel_order(order_id) -> bool

SimulatedBroker config:
    BrokerConfig: commission_rate=0.001, min_commission=1.0, slippage=0.0, fill_mode=FillMode.IMMEDIATE

"""

from __future__ import annotations

import uuid
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable
from enum import Enum

from zuilow.components.control import ctrl

from .types import (
    Order, OrderSide, OrderType, OrderStatus,
    Position, Trade, Account
)

logger = logging.getLogger(__name__)


class FillMode(Enum):
    """Fill mode."""
    IMMEDIATE = "immediate"   # Fill immediately (simulated)
    NEXT_BAR = "next_bar"     # Fill on next bar


@dataclass
class BrokerConfig:
    """Broker config."""
    commission_rate: float = 0.001      # Commission rate
    min_commission: float = 1.0         # Min commission
    slippage: float = 0.0               # Slippage
    fill_mode: FillMode = FillMode.IMMEDIATE
    allow_short: bool = False           # Allow short
    allow_margin: bool = False          # Allow margin
    margin_rate: float = 0.5            # Margin rate


class SimulatedBroker:
    """
    Simulated broker.

    Order submit/fill and position management for backtest.

    """

    def __init__(
        self,
        initial_capital: float = 100000.0,
        config: BrokerConfig | None = None
    ):
        self.config = config or BrokerConfig()
        self.account = Account(initial_capital=initial_capital)

        self._pending_orders: dict[str, Order] = {}
        self._order_history: list[Order] = []

        self._on_order_filled: Callable[[Order, Trade], None] | None = None
        self._on_order_rejected: Callable[[Order, str], None] | None = None

    # ========== Orders ==========

    def submit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        order_type: OrderType = OrderType.MARKET,
        price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        """
        Submit order.

        Args:
            symbol: Symbol
            side: Buy/sell
            quantity: Quantity
            order_type: Order type
            price: Limit price (required for limit)
            stop_price: Stop price (for stop order)

        Returns:
            Order
        """
        if quantity <= 0:
            raise ValueError("Quantity must be > 0")

        if order_type == OrderType.LIMIT and price is None:
            raise ValueError("Limit order must specify price")

        if side == OrderSide.SELL and not self.config.allow_short:
            position = self.account.get_position(symbol)
            if not position or position.quantity < quantity:
                raise ValueError(f"Insufficient position: need {quantity}, have {position.quantity if position else 0}")

        if side == OrderSide.BUY:
            estimated_cost = (price or 0) * quantity
            if estimated_cost > 0 and estimated_cost > self.account.cash:
                raise ValueError(f"Insufficient cash: need ~${estimated_cost:.2f}, have ${self.account.cash:.2f}")

        order = Order(
            id=str(uuid.uuid4())[:8],
            symbol=symbol.upper(),
            side=side,
            type=order_type,
            quantity=quantity,
            price=price,
            status=OrderStatus.PENDING,
            created_at=ctrl.get_current_dt(),
        )
        
        self._pending_orders[order.id] = order
        self._order_history.append(order)

        logger.info(f"Order submitted: {order.id} {side.value} {quantity} {symbol} @ {price or 'MARKET'}")

        return order

    def cancel_order(self, order_id: str) -> bool:
        """Cancel order."""
        if order_id not in self._pending_orders:
            return False
        
        order = self._pending_orders.pop(order_id)
        order.status = OrderStatus.CANCELLED

        logger.info(f"Order cancelled: {order_id}")
        return True

    def fill_order(
        self,
        order_id: str,
        price: float,
        quantity: float | None = None,
        timestamp: datetime | None = None,
    ) -> Trade | None:
        """
        Fill order.

        Args:
            order_id: Order ID
            price: Fill price
            quantity: Fill quantity (default full)
            timestamp: Fill time

        Returns:
            Trade or None on failure
        """
        if order_id not in self._pending_orders:
            logger.warning(f"Order not found or already processed: {order_id}")
            return None

        order = self._pending_orders[order_id]
        fill_qty = quantity or order.quantity
        fill_price = self._apply_slippage(price, order.side == OrderSide.BUY)

        commission = self._calculate_commission(fill_price, fill_qty)

        order.status = OrderStatus.FILLED
        order.filled_price = fill_price
        order.filled_quantity = fill_qty
        order.commission = commission
        order.filled_at = timestamp or ctrl.get_current_dt()

        trade = self._update_account(order)

        del self._pending_orders[order_id]

        logger.info(
            f"Order filled: {order_id} {order.side.value} {fill_qty} {order.symbol} "
            f"@ ${fill_price:.2f}, commission ${commission:.2f}"
        )

        if self._on_order_filled:
            self._on_order_filled(order, trade)
        
        return trade
    
    def fill_pending_orders(self, symbol: str, price: float) -> list[Trade]:
        """
        Fill all pending orders at given price.

        Args:
            symbol: Symbol
            price: Current price

        Returns:
            List of trades
        """
        trades = []

        pending = list(self._pending_orders.values())

        for order in pending:
            if order.symbol != symbol.upper():
                continue

            should_fill = False

            if order.type == OrderType.MARKET:
                should_fill = True
            elif order.type == OrderType.LIMIT:
                if order.side == OrderSide.BUY and price <= order.price:
                    should_fill = True
                elif order.side == OrderSide.SELL and price >= order.price:
                    should_fill = True
            
            if should_fill:
                trade = self.fill_order(order.id, price)
                if trade:
                    trades.append(trade)
        
        return trades

    # ========== Positions ==========

    def get_position(self, symbol: str) -> Position | None:
        """Get position."""
        return self.account.get_position(symbol.upper())

    def get_positions(self) -> dict[str, Position]:
        """Get all positions."""
        return self.account.positions.copy()

    def has_position(self, symbol: str) -> bool:
        """Whether has position."""
        return self.account.has_position(symbol.upper())

    # ========== Account ==========

    @property
    def cash(self) -> float:
        """Cash balance."""
        return self.account.cash

    @property
    def equity(self) -> float:
        """Total equity."""
        return self.account.equity

    @property
    def buying_power(self) -> float:
        """Buying power."""
        if self.config.allow_margin:
            return self.account.cash / self.config.margin_rate
        return self.account.cash

    def get_pending_orders(self, symbol: str | None = None) -> list[Order]:
        """Get pending orders."""
        orders = list(self._pending_orders.values())
        if symbol:
            orders = [o for o in orders if o.symbol == symbol.upper()]
        return orders

    def get_trades(self, symbol: str | None = None) -> list[Trade]:
        """Get trades."""
        trades = self.account.trades
        if symbol:
            trades = [t for t in trades if t.symbol == symbol.upper()]
        return trades

    # ========== Callbacks ==========

    def on_order_filled(self, callback: Callable[[Order, Trade], None]) -> None:
        """Set order filled callback."""
        self._on_order_filled = callback

    def on_order_rejected(self, callback: Callable[[Order, str], None]) -> None:
        """Set order rejected callback."""
        self._on_order_rejected = callback

    # ========== Internal ==========

    def _calculate_commission(self, price: float, quantity: float) -> float:
        """Compute commission."""
        commission = price * quantity * self.config.commission_rate
        return max(commission, self.config.min_commission)

    def _apply_slippage(self, price: float, is_buy: bool) -> float:
        """Apply slippage."""
        if self.config.slippage == 0:
            return price
        
        slippage = price * self.config.slippage
        return price + slippage if is_buy else price - slippage

    def _update_account(self, order: Order) -> Trade:
        """Update account after fill."""
        price = order.filled_price
        quantity = order.filled_quantity
        commission = order.commission

        if order.side == OrderSide.BUY:
            cost = price * quantity + commission
            self.account.cash -= cost

            if order.symbol in self.account.positions:
                pos = self.account.positions[order.symbol]
                total_qty = pos.quantity + quantity
                pos.avg_price = (pos.cost + price * quantity) / total_qty
                pos.quantity = total_qty
                pos.current_price = price
            else:
                self.account.positions[order.symbol] = Position(
                    symbol=order.symbol,
                    quantity=quantity,
                    avg_price=price,
                    current_price=price,
                    opened_at=order.filled_at or ctrl.get_current_dt(),
                )
            
            pnl = 0.0

        else:  # SELL
            proceeds = price * quantity - commission
            self.account.cash += proceeds

            pos = self.account.positions.get(order.symbol)
            if pos:
                pnl = (price - pos.avg_price) * quantity - commission
                pos.quantity -= quantity
                if pos.quantity <= 0:
                    del self.account.positions[order.symbol]
            else:
                pnl = -commission

        trade = Trade(
            id=str(uuid.uuid4())[:8],
            order_id=order.id,
            symbol=order.symbol,
            side=order.side,
            quantity=quantity,
            price=price,
            commission=commission,
            pnl=pnl,
            timestamp=order.filled_at or ctrl.get_current_dt(),
        )
        
        self.account.trades.append(trade)
        return trade

    def update_prices(self, prices: dict[str, float]) -> None:
        """Update position prices."""
        for symbol, price in prices.items():
            pos = self.account.positions.get(symbol.upper())
            if pos:
                pos.update_price(price)

    def summary(self) -> str:
        """Account summary."""
        lines = [
            "=" * 40,
            "Account Summary",
            "=" * 40,
            f"Cash: ${self.cash:,.2f}",
            f"Positions: ${sum(p.market_value for p in self.account.positions.values()):,.2f}",
            f"Equity: ${self.equity:,.2f}",
            f"P&L: ${self.account.total_pnl:,.2f} ({self.account.total_pnl_pct:+.2f}%)",
            "",
            "Positions:",
        ]

        if self.account.positions:
            for pos in self.account.positions.values():
                lines.append(
                    f"  {pos.symbol}: {pos.quantity:.2f} @ ${pos.avg_price:.2f} "
                    f"(price ${pos.current_price:.2f}, P&L ${pos.pnl:+.2f})"
                )
        else:
            lines.append("  (none)")

        lines.append("=" * 40)
        return "\n".join(lines)
