"""
Simple moving average strategy.

- Short MA crosses above long MA -> buy
- Short MA crosses below long MA -> sell
"""

from __future__ import annotations

from zuilow.components.backtest.strategy import Strategy, StrategyContext
from zuilow.components.backtest.types import Bar, Signal


class SMAStrategy(Strategy):
    def __init__(self, short_period: int = 5, long_period: int = 20):
        super().__init__(name="SMA")
        self.short_period = short_period
        self.long_period = long_period
        self._prev_short_above = None

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> Signal | None:
        if ctx.history is None or len(ctx.history) < self.long_period:
            return None

        # Compute MAs
        close = ctx.history["Close"]
        short_ma = close.rolling(self.short_period).mean().iloc[-1]
        long_ma = close.rolling(self.long_period).mean().iloc[-1]

        short_above = short_ma > long_ma

        # Detect crossover
        if self._prev_short_above is not None:
            if not self._prev_short_above and short_above:
                # Golden cross
                self._prev_short_above = short_above
                if not ctx.account.has_position(bar.symbol):
                    return Signal.buy(bar.symbol, reason=f"golden cross: SMA{self.short_period} > SMA{self.long_period}")

            elif self._prev_short_above and not short_above:
                # Death cross
                self._prev_short_above = short_above
                if ctx.account.has_position(bar.symbol):
                    return Signal.sell(bar.symbol, reason=f"death cross: SMA{self.short_period} < SMA{self.long_period}")

        self._prev_short_above = short_above
        return None
