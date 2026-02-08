"""
Strategy base: abstract on_bar and optional hooks for backtest and scheduler.

Classes:
    StrategyContext   account, current_bar, history, params
    Strategy          Abstract base; on_bar(bar, ctx) -> Signal | None (abstract);
                     on_start(ctx), on_end(ctx), on_order_filled(order, ctx);
                     on_market_open(ctx), on_open_bar(ctx, bar), on_time(ctx, t) -> list[Signal] | None (optional)

Concrete strategies live in zuilow.strategies (top-level); backtest re-exports them from there.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import pandas as pd

from .types import Bar, Signal, SignalType, Order, Trade, Account

logger = logging.getLogger(__name__)


@dataclass
class StrategyContext:
    """
    Strategy context.

    Provides information required for strategy execution.
    """
    account: Account
    current_bar: Bar | None = None
    history: pd.DataFrame | None = None
    params: dict[str, Any] = field(default_factory=dict)


class Strategy(ABC):
    """
    Strategy base class.

    All strategies must inherit this and implement on_bar.
    Override init_config() to provide default config (params, etc.) so scheduler does not require a YAML file.
    """

    @classmethod
    def init_config(cls) -> dict:
        """
        Default config for this strategy (params passed to constructor).
        Override in subclass so scheduler can run without a separate config file.
        Returns dict with at least "params" (kwargs for __init__); may include "description", etc.
        """
        return {}

    def __init__(self, name: str = "Strategy"):
        self.name = name
        self._params: dict[str, Any] = {}

    @abstractmethod
    def on_bar(self, bar: Bar, ctx: StrategyContext) -> Signal | None:
        """
        Bar callback - core strategy logic.

        Args:
            bar: Current bar
            ctx: Strategy context

        Returns:
            Trading signal, or None if no signal
        """
        pass

    def on_start(self, ctx: StrategyContext) -> None:
        """Backtest start callback."""
        pass

    def on_end(self, ctx: StrategyContext) -> None:
        """Backtest end callback."""
        pass

    def on_order_filled(self, order: Order, ctx: StrategyContext) -> None:
        """Order filled callback."""
        pass

    def on_market_open(self, ctx: StrategyContext) -> list[Signal] | None:
        """Optional: called at market open; return signals (e.g. rebalance) or None."""
        return None

    def on_open_bar(self, ctx: StrategyContext, bar: Bar) -> list[Signal] | None:
        """Optional: called when a new bar opens; return signals or None."""
        return None

    def on_time(self, ctx: StrategyContext, t: datetime) -> list[Signal] | None:
        """Optional: called at fixed time; return signals or None."""
        return None

    def on_trade(self, trade: Trade, ctx: StrategyContext) -> None:
        """Trade callback."""
        pass

    def set_params(self, **params) -> Strategy:
        """Set parameters."""
        self._params.update(params)
        return self

    def get_param(self, key: str, default: Any = None) -> Any:
        """Get parameter."""
        return self._params.get(key, default)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
