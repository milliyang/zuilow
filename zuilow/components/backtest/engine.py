"""
Backtest engine: event-driven bar loop, strategy on_bar, simulated broker.

Classes:
    BacktestConfig   initial_capital, commission_rate, slippage, position_size, allow_short, verbose
    BacktestResult   start_date, end_date, initial_capital, final_equity, equity_curve,
                     trades, metrics (BacktestMetrics)
    BacktestEngine   Event-driven engine; processes bars, strategy.on_bar, SimulatedBroker fills

BacktestEngine methods:
    .run(strategy: Strategy, data: DataFrame, symbol: str = "") -> BacktestResult

BacktestResult fields:
    start_date, end_date, initial_capital, final_equity, total_return, total_return_pct,
    equity_curve (list of (datetime, float)), trades (list of Trade), metrics (BacktestMetrics)

"""

from __future__ import annotations

import uuid
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any
import pandas as pd

from .types import (
    Bar, Signal, SignalType, Order, OrderSide, OrderType, OrderStatus,
    Position, Trade, Account
)
from .strategy import Strategy, StrategyContext
from .metrics import BacktestMetrics, calculate_metrics

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Backtest config."""
    initial_capital: float = 100000.0   # Initial capital
    commission_rate: float = 0.001      # Commission rate (0.1%)
    slippage: float = 0.0               # Slippage (price %)
    position_size: float = 1.0          # Position size (0-1)
    allow_short: bool = False           # Allow short
    verbose: bool = True                # Verbose output


@dataclass
class BacktestResult:
    """Backtest result."""
    strategy_name: str
    symbol: str
    start_date: datetime
    end_date: datetime
    initial_capital: float
    final_equity: float
    total_return: float
    total_return_pct: float
    metrics: BacktestMetrics
    trades: list[Trade]
    equity_curve: list[tuple[datetime, float]]
    config: BacktestConfig
    
    def summary(self) -> str:
        """Generate backtest summary."""
        lines = [
            "=" * 50,
            f"Backtest: {self.strategy_name}",
            "=" * 50,
            f"Symbol: {self.symbol}",
            f"Period: {self.start_date:%Y-%m-%d} ~ {self.end_date:%Y-%m-%d}",
            "",
            "[Returns]",
            f"  Initial: ${self.initial_capital:,.2f}",
            f"  Final: ${self.final_equity:,.2f}",
            f"  P&L: ${self.total_return:,.2f} ({self.total_return_pct:+.2f}%)",
            "",
            "[Metrics]",
            f"  Annual return: {self.metrics.annual_return:.2f}%",
            f"  Max drawdown: {self.metrics.max_drawdown:.2f}%",
            f"  Sharpe: {self.metrics.sharpe_ratio:.2f}",
            f"  Win rate: {self.metrics.win_rate:.1f}%",
            f"  Profit factor: {self.metrics.profit_factor:.2f}",
            "",
            "[Trades]",
            f"  Total: {self.metrics.total_trades}",
            f"  Wins: {self.metrics.winning_trades}",
            f"  Losses: {self.metrics.losing_trades}",
            "=" * 50,
        ]
        return "\n".join(lines)


class BacktestEngine:
    """
    Backtest engine.

    Single-symbol historical backtest with event-driven strategy execution.

    """

    def __init__(self, config: BacktestConfig | None = None):
        self.config = config or BacktestConfig()
        self._account: Account | None = None
        self._strategy: Strategy | None = None
        self._ctx: StrategyContext | None = None
        self._current_data: pd.DataFrame | None = None
    
    def run(
        self,
        strategy: Strategy,
        data: pd.DataFrame,
        symbol: str = "UNKNOWN",
    ) -> BacktestResult:
        """
        Run backtest.

        Args:
            strategy: Strategy instance
            data: History (DataFrame with Open/High/Low/Close/Volume)
            symbol: Symbol

        Returns:
            Backtest result
        """
        # Init
        self._strategy = strategy
        self._account = Account(initial_capital=self.config.initial_capital)
        self._current_data = data.copy()
        
        # Validate columns
        required_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in required_cols:
            if col not in data.columns:
                raise ValueError(f"Data missing required column: {col}")

        # Create context
        self._ctx = StrategyContext(
            account=self._account,
            params=strategy._params,
        )
        
        # Start backtest
        if self.config.verbose:
            logger.info(f"Starting backtest: {strategy.name} on {symbol}")
        
        strategy.on_start(self._ctx)
        
        # Iterate bars
        for i, (timestamp, row) in enumerate(data.iterrows()):
            bar = Bar(
                symbol=symbol,
                timestamp=timestamp,
                open=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"],
                volume=row["Volume"],
            )
            
            # Update context
            self._ctx.current_bar = bar
            self._ctx.history = data.iloc[:i+1]

            # Update position prices
            self._update_positions(bar.close)

            # Run strategy
            signal = strategy.on_bar(bar, self._ctx)

            # Process signal
            if signal:
                self._process_signal(signal, bar)

            # Record equity
            self._account.record_equity(bar.timestamp)

        # End backtest
        strategy.on_end(self._ctx)

        # Compute metrics
        metrics = calculate_metrics(
            equity_curve=self._account.equity_curve,
            trades=self._account.trades,
            initial_capital=self.config.initial_capital,
        )
        
        # Build result
        result = BacktestResult(
            strategy_name=strategy.name,
            symbol=symbol,
            start_date=data.index[0],
            end_date=data.index[-1],
            initial_capital=self.config.initial_capital,
            final_equity=self._account.equity,
            total_return=self._account.total_pnl,
            total_return_pct=self._account.total_pnl_pct,
            metrics=metrics,
            trades=self._account.trades.copy(),
            equity_curve=self._account.equity_curve.copy(),
            config=self.config,
        )
        
        if self.config.verbose:
            logger.info(f"Backtest done: return {result.total_return_pct:+.2f}%")
        
        return result
    
    def _update_positions(self, current_price: float) -> None:
        """Update position prices."""
        for pos in self._account.positions.values():
            pos.update_price(current_price)
    
    def _process_signal(self, signal: Signal, bar: Bar) -> None:
        """Process trading signal."""
        if signal.type == SignalType.BUY:
            self._execute_buy(signal, bar)
        elif signal.type == SignalType.SELL:
            self._execute_sell(signal, bar)
    
    def _execute_buy(self, signal: Signal, bar: Bar) -> None:
        """Execute buy."""
        available_cash = self._account.cash * self.config.position_size
        price = self._apply_slippage(bar.close, is_buy=True)

        quantity = available_cash / (price * (1 + self.config.commission_rate))
        if quantity <= 0:
            return

        order = Order(
            id=str(uuid.uuid4())[:8],
            symbol=signal.symbol,
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=quantity,
            price=price,
        )

        self._fill_order(order, bar)

        if self.config.verbose:
            logger.info(f"Buy: {signal.symbol} @ ${price:.2f} x {quantity:.2f} | {signal.reason}")

    def _execute_sell(self, signal: Signal, bar: Bar) -> None:
        """Execute sell."""
        position = self._account.get_position(signal.symbol)
        if not position or position.quantity <= 0:
            return
        
        price = self._apply_slippage(bar.close, is_buy=False)

        order = Order(
            id=str(uuid.uuid4())[:8],
            symbol=signal.symbol,
            side=OrderSide.SELL,
            type=OrderType.MARKET,
            quantity=position.quantity,
            price=price,
        )

        self._fill_order(order, bar)

        if self.config.verbose:
            logger.info(f"Sell: {signal.symbol} @ ${price:.2f} x {position.quantity:.2f} | {signal.reason}")

    def _fill_order(self, order: Order, bar: Bar) -> None:
        """Fill order."""
        price = order.price or bar.close
        quantity = order.quantity
        commission = price * quantity * self.config.commission_rate

        order.status = OrderStatus.FILLED
        order.filled_price = price
        order.filled_quantity = quantity
        order.commission = commission
        order.filled_at = bar.timestamp

        if order.side == OrderSide.BUY:
            cost = price * quantity + commission
            self._account.cash -= cost

            if order.symbol in self._account.positions:
                pos = self._account.positions[order.symbol]
                total_qty = pos.quantity + quantity
                pos.avg_price = (pos.cost + price * quantity) / total_qty
                pos.quantity = total_qty
            else:
                self._account.positions[order.symbol] = Position(
                    symbol=order.symbol,
                    quantity=quantity,
                    avg_price=price,
                    current_price=price,
                    opened_at=bar.timestamp,
                )
            
            pnl = 0.0
        
        else:  # SELL
            proceeds = price * quantity - commission
            self._account.cash += proceeds

            pos = self._account.positions.get(order.symbol)
            if pos:
                pnl = (price - pos.avg_price) * quantity - commission
                pos.quantity -= quantity
                if pos.quantity <= 0:
                    del self._account.positions[order.symbol]
            else:
                pnl = 0.0

        trade = Trade(
            id=str(uuid.uuid4())[:8],
            order_id=order.id,
            symbol=order.symbol,
            side=order.side,
            quantity=quantity,
            price=price,
            commission=commission,
            pnl=pnl,
            timestamp=bar.timestamp,
        )
        
        self._account.trades.append(trade)

        self._strategy.on_order_filled(order, self._ctx)
        self._strategy.on_trade(trade, self._ctx)
    
    def _apply_slippage(self, price: float, is_buy: bool) -> float:
        """Apply slippage."""
        if self.config.slippage == 0:
            return price

        slippage_amount = price * self.config.slippage
        if is_buy:
            return price + slippage_amount  # Buy slightly higher
        else:
            return price - slippage_amount  # Sell slightly lower
