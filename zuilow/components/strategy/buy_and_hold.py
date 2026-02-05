"""
Buy and hold strategy (configurable via YAML).

Buy when no position; hold to end.
Config params: reason (str), only_first (bool).
- only_first=True: at most one buy per run (first symbol that triggers).
- only_first=False: buy every symbol that has no position (one buy per symbol).
"""

from __future__ import annotations

import logging

from zuilow.components.backtest.strategy import Strategy, StrategyContext
from zuilow.components.backtest.types import Bar, Signal

logger = logging.getLogger(__name__)


class BuyAndHold(Strategy):
    def __init__(self, reason: str = "buy and hold", only_first: bool = True, **kwargs):
        super().__init__(name="BuyAndHold")
        self._params["reason"] = reason
        self._params["only_first"] = only_first
        self._bought = False

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> Signal | None:
        reason = self.get_param("reason", "buy and hold")
        only_first = self.get_param("only_first", True)
        if only_first and self._bought:
            return None
        if not ctx.account.has_position(bar.symbol):
            if only_first:
                self._bought = True
            logger.info(f"Buying {bar.symbol} at {bar.close}")
            return Signal.buy(bar.symbol, reason=reason)
        return None
