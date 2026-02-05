"""
Backtest executor: process signals and send orders (in-memory backtest or paper via HTTP).

Executor: in-memory; process_signal converts Signal to order and sends to SimulatedBroker.
PaperTrader: sends orders to PPT (Paper Trade) service via HTTP. For live paper use
separate PPT (Paper Trade) service.

Classes:
    ExecutorConfig   initial_capital, broker_config, position_size, max_positions, verbose
    Executor         process_signal(signal), buy(symbol, quantity, price, reason), sell(symbol, ...)
    PaperTraderConfig   api_base_url, webhook_token, timeout
    PaperTrader      process_signal(signal); sends POST to PPT /api/webhook

Executor methods:
    .process_signal(signal: Signal) -> bool
    .buy(symbol, quantity, price=None, reason="") -> bool
    .sell(symbol, quantity, price=None, reason="") -> bool

"""

from __future__ import annotations

import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Any

from .types import OrderSide, OrderType, Signal, SignalType
from .strategy import Strategy, StrategyContext
from .broker import SimulatedBroker, BrokerConfig

logger = logging.getLogger(__name__)


@dataclass
class ExecutorConfig:
    """Backtest executor config."""
    initial_capital: float = 100000.0
    broker_config: BrokerConfig = field(default_factory=BrokerConfig)
    position_size: float = 1.0          # Position size (0-1)
    max_positions: int = 10             # Max positions
    verbose: bool = True


class Executor:
    """
    Backtest executor.

    Connects strategy and simulated broker for backtest order execution.

    """

    def __init__(self, config: ExecutorConfig | None = None):
        self.config = config or ExecutorConfig()

        self.broker = SimulatedBroker(
            initial_capital=self.config.initial_capital,
            config=self.config.broker_config,
        )

        self._strategy: Strategy | None = None
        self._ctx: StrategyContext | None = None

        self._prices: dict[str, float] = {}

        self._on_trade: Callable[[Any], None] | None = None

    def set_strategy(self, strategy: Strategy) -> None:
        """Set strategy."""
        self._strategy = strategy
        self._ctx = StrategyContext(
            account=self.broker.account,
            params=strategy._params,
        )
    
    def on_price_update(self, symbol: str, price: float) -> None:
        """
        Price update callback.

        Args:
            symbol: Symbol
            price: Latest price
        """
        symbol = symbol.upper()
        self._prices[symbol] = price

        self.broker.update_prices({symbol: price})
        self.broker.fill_pending_orders(symbol, price)

    def process_signal(self, signal: Signal) -> bool:
        """
        Process trading signal.

        Args:
            signal: Trading signal

        Returns:
            True if order submitted successfully
        """
        if signal.type == SignalType.HOLD:
            return False
        
        symbol = signal.symbol.upper()
        current_price = self._prices.get(symbol) or signal.price
        
        if not current_price:
            logger.warning(f"Cannot process signal: no price for {symbol}")
            return False

        try:
            if signal.type == SignalType.BUY:
                return self._execute_buy(symbol, current_price, signal)
            elif signal.type == SignalType.SELL:
                return self._execute_sell(symbol, current_price, signal)
        except Exception as e:
            logger.error(f"Signal processing failed: {e}")
            return False

        return False

    def _execute_buy(self, symbol: str, price: float, signal: Signal) -> bool:
        """Execute buy."""
        if len(self.broker.get_positions()) >= self.config.max_positions:
            logger.warning(f"Max positions reached: {self.config.max_positions}")
            return False

        available = self.broker.cash * self.config.position_size
        quantity = signal.quantity or (available / price)

        if quantity <= 0:
            logger.warning("Insufficient cash")
            return False

        order = self.broker.submit_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            order_type=OrderType.MARKET,
        )

        trade = self.broker.fill_order(order.id, price)

        if trade and self._on_trade:
            self._on_trade(trade)

        if self.config.verbose:
            logger.info(f"Buy: {symbol} {quantity:.2f} @ ${price:.2f} | {signal.reason}")

        return True

    def _execute_sell(self, symbol: str, price: float, signal: Signal) -> bool:
        """Execute sell."""
        position = self.broker.get_position(symbol)
        if not position:
            logger.warning(f"No position for {symbol}")
            return False

        quantity = signal.quantity or position.quantity
        quantity = min(quantity, position.quantity)

        order = self.broker.submit_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            order_type=OrderType.MARKET,
        )

        trade = self.broker.fill_order(order.id, price)

        if trade and self._on_trade:
            self._on_trade(trade)

        if self.config.verbose:
            logger.info(f"Sell: {symbol} {quantity:.2f} @ ${price:.2f} | {signal.reason}")

        return True

    def buy(self, symbol: str, quantity: float | None = None, reason: str = "") -> bool:
        """Convenience buy."""
        price = self._prices.get(symbol.upper())
        if not price:
            logger.warning(f"No price for {symbol}")
            return False

        signal = Signal.buy(symbol, quantity=quantity, price=price, reason=reason)
        return self.process_signal(signal)

    def sell(self, symbol: str, quantity: float | None = None, reason: str = "") -> bool:
        """Convenience sell."""
        price = self._prices.get(symbol.upper())
        if not price:
            logger.warning(f"No price for {symbol}")
            return False

        signal = Signal.sell(symbol, quantity=quantity, price=price, reason=reason)
        return self.process_signal(signal)

    def close_position(self, symbol: str, reason: str = "close") -> bool:
        """Close position."""
        return self.sell(symbol, reason=reason)

    def close_all_positions(self, reason: str = "close all") -> int:
        """Close all positions."""
        closed = 0
        for symbol in list(self.broker.get_positions().keys()):
            if self.close_position(symbol, reason):
                closed += 1
        return closed
    
    def on_trade(self, callback: Callable[[Any], None]) -> None:
        """Set trade callback."""
        self._on_trade = callback
    
    @property
    def cash(self) -> float:
        return self.broker.cash
    
    @property
    def equity(self) -> float:
        return self.broker.equity
    
    def summary(self) -> str:
        """Trading summary."""
        return self.broker.summary()

    def get_trades(self) -> list:
        """Get all trades."""
        return self.broker.get_trades()

    def get_positions(self) -> dict:
        """Get all positions."""
        return self.broker.get_positions()


# Backward compatibility aliases
BacktestExecutor = Executor
BacktestExecutorConfig = ExecutorConfig
PaperTrader = Executor
PaperTraderConfig = ExecutorConfig
