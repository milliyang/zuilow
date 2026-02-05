"""
Rebalance strategy: output target_weights (or target_mv) once per run.
Used after market close; signals are executed at next market open (exec_* job).
Config params: target_weights (symbol -> weight 0..1) or target_mv (symbol -> market value).
"""

from __future__ import annotations

from zuilow.components.backtest.strategy import Strategy, StrategyContext
from zuilow.components.backtest.types import Bar, Signal
import logging

logger = logging.getLogger(__name__)

class RebalanceAfterClose(Strategy):
    def __init__(self, target_weights: dict[str, float] | None = None, target_mv: dict[str, float] | None = None, **kwargs):
        super().__init__(name="RebalanceAfterClose")
        self._target_weights = target_weights or {}
        self._target_mv = target_mv or {}

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> Signal | None:
        logger.info(f"RebalanceAfterClose: on_bar() {self._target_weights} {self._target_mv}")
        return None

    def get_rebalance_output(self) -> dict | None:
        """Return one rebalance signal dict for scheduler (target_weights or target_mv)."""
        if self._target_weights:
            logger.info(f"RebalanceAfterClose: get_rebalance_output() {self._target_weights}")
            return {"kind": "rebalance", "target_weights": self._target_weights}
        if self._target_mv:
            logger.info(f"RebalanceAfterClose: get_rebalance_output() {self._target_mv}")
            return {"kind": "rebalance", "target_mv": self._target_mv}
        return None
