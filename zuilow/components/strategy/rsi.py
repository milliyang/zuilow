"""
RSI strategy.

- RSI < oversold -> buy
- RSI > overbought -> sell
"""

from __future__ import annotations

import pandas as pd

from zuilow.components.backtest.strategy import Strategy, StrategyContext
from zuilow.components.backtest.types import Bar, Signal


class RSIStrategy(Strategy):
    def __init__(self, period: int = 14, oversold: float = 30, overbought: float = 70):
        super().__init__(name="RSI")
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def _calculate_rsi(self, prices: pd.Series) -> float | None:
        """Compute RSI."""
        if len(prices) < self.period + 1:
            return None

        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(self.period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(self.period).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> Signal | None:
        if ctx.history is None or len(ctx.history) < self.period + 1:
            return None

        rsi = self._calculate_rsi(ctx.history["Close"])
        if rsi is None:
            return None

        has_position = ctx.account.has_position(bar.symbol)

        if rsi < self.oversold and not has_position:
            return Signal.buy(bar.symbol, reason=f"RSI oversold: {rsi:.1f} < {self.oversold}")

        if rsi > self.overbought and has_position:
            return Signal.sell(bar.symbol, reason=f"RSI overbought: {rsi:.1f} > {self.overbought}")

        return None
