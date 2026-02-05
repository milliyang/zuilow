"""
Backtest data types: Bar, Signal, Order, Position, Trade, Account.

Classes:
    OrderSide, OrderType, OrderStatus, SignalType   Enums (buy/sell, market/limit, pending/filled, ...)
    Bar       symbol, timestamp, open, high, low, close, volume
    Signal    type, symbol, price, quantity, reason; .buy(), .sell(), .hold()
    Order     id, symbol, side, type, quantity, price, status, filled_price, filled_quantity, ...
    Position  symbol, quantity, avg_price, current_price; .market_value, .pnl, .pnl_pct, .update_price()
    Trade     id, order_id, symbol, side, quantity, price, commission, pnl, timestamp
    Account   initial_capital, cash, positions, equity_curve, trades; .equity, .total_pnl,
              .get_position(symbol), .has_position(symbol), .record_equity(timestamp)

"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from zuilow.components.control import ctrl


class OrderSide(Enum):
    """Order side."""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type."""
    MARKET = "market"       # Market order
    LIMIT = "limit"         # Limit order
    STOP = "stop"           # Stop order
    STOP_LIMIT = "stop_limit"


class OrderStatus(Enum):
    """Order status."""
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class SignalType(Enum):
    """Signal type."""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass
class Bar:
    """
    OHLCV bar data.

    Attributes:
        symbol: Symbol
        timestamp: Timestamp
        open: Open price
        high: High price
        low: Low price
        close: Close price
        volume: Volume
    """
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __repr__(self) -> str:
        return f"Bar({self.symbol} {self.timestamp:%Y-%m-%d} O={self.open:.2f} C={self.close:.2f})"


@dataclass
class Signal:
    """
    Trading signal.

    Attributes:
        type: Signal type (buy/sell/hold)
        symbol: Symbol
        price: Suggested price (optional)
        quantity: Suggested quantity (optional, fraction or fixed)
        reason: Signal reason
        timestamp: Generation time
    """
    type: SignalType
    symbol: str
    price: float | None = None
    quantity: float | None = None
    reason: str = ""
    timestamp: datetime = field(default_factory=lambda: ctrl.get_current_dt())

    @classmethod
    def buy(cls, symbol: str, **kwargs) -> Signal:
        return cls(type=SignalType.BUY, symbol=symbol, **kwargs)

    @classmethod
    def sell(cls, symbol: str, **kwargs) -> Signal:
        return cls(type=SignalType.SELL, symbol=symbol, **kwargs)

    @classmethod
    def hold(cls, symbol: str, **kwargs) -> Signal:
        return cls(type=SignalType.HOLD, symbol=symbol, **kwargs)


@dataclass
class Order:
    """
    Order.

    Attributes:
        id: Order ID
        symbol: Symbol
        side: Buy/sell
        type: Order type
        quantity: Quantity
        price: Price (None for market)
        status: Order status
        filled_price: Fill price
        filled_quantity: Filled quantity
        commission: Commission
        created_at: Created time
        filled_at: Fill time
    """
    id: str
    symbol: str
    side: OrderSide
    type: OrderType
    quantity: float
    price: float | None = None
    status: OrderStatus = OrderStatus.PENDING
    filled_price: float | None = None
    filled_quantity: float = 0
    commission: float = 0.0
    created_at: datetime = field(default_factory=lambda: ctrl.get_current_dt())
    filled_at: datetime | None = None

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def total_cost(self) -> float:
        """Total cost (including commission)."""
        if self.filled_price and self.filled_quantity:
            return self.filled_price * self.filled_quantity + self.commission
        return 0.0


@dataclass
class Position:
    """
    Position.

    Attributes:
        symbol: Symbol
        quantity: Quantity
        avg_price: Average cost
        current_price: Current price
        opened_at: Open time
    """
    symbol: str
    quantity: float
    avg_price: float
    current_price: float = 0.0
    opened_at: datetime = field(default_factory=lambda: ctrl.get_current_dt())

    @property
    def market_value(self) -> float:
        """Market value."""
        return self.quantity * self.current_price

    @property
    def cost(self) -> float:
        """Cost."""
        return self.quantity * self.avg_price

    @property
    def pnl(self) -> float:
        """Unrealized P&L."""
        return self.market_value - self.cost

    @property
    def pnl_pct(self) -> float:
        """Unrealized P&L percentage."""
        if self.cost == 0:
            return 0.0
        return (self.pnl / self.cost) * 100

    def update_price(self, price: float) -> None:
        """Update current price."""
        self.current_price = price


@dataclass
class Trade:
    """
    Trade record.

    Attributes:
        id: Trade ID
        order_id: Order ID
        symbol: Symbol
        side: Buy/sell
        quantity: Filled quantity
        price: Fill price
        commission: Commission
        pnl: Realized P&L (for close only)
        timestamp: Trade time
    """
    id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    commission: float = 0.0
    pnl: float = 0.0
    timestamp: datetime = field(default_factory=lambda: ctrl.get_current_dt())


@dataclass
class Account:
    """
    Account.

    Attributes:
        initial_capital: Initial capital
        cash: Cash balance
        positions: Positions dict {symbol: Position}
        equity_curve: Equity curve [(timestamp, equity), ...]
    """
    initial_capital: float
    cash: float = field(default=0.0)
    positions: dict[str, Position] = field(default_factory=dict)
    equity_curve: list[tuple[datetime, float]] = field(default_factory=list)
    trades: list[Trade] = field(default_factory=list)

    def __post_init__(self):
        if self.cash == 0.0:
            self.cash = self.initial_capital

    @property
    def equity(self) -> float:
        """Total equity = cash + positions value."""
        positions_value = sum(p.market_value for p in self.positions.values())
        return self.cash + positions_value

    @property
    def total_pnl(self) -> float:
        """Total P&L."""
        return self.equity - self.initial_capital

    @property
    def total_pnl_pct(self) -> float:
        """Total return percentage."""
        return (self.total_pnl / self.initial_capital) * 100

    def record_equity(self, timestamp: datetime) -> None:
        """Record current equity."""
        self.equity_curve.append((timestamp, self.equity))

    def get_position(self, symbol: str) -> Position | None:
        """Get position."""
        return self.positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        """Whether has position."""
        pos = self.positions.get(symbol)
        return pos is not None and pos.quantity > 0
