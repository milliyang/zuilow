"""
User strategies: one file per strategy under zuilow/strategies/.
Auto-discovers all Strategy subclasses so you can add new files without editing this file.
Base class Strategy remains in zuilow.components.backtest.strategy.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Type

from zuilow.components.backtest.strategy import Strategy, StrategyContext


def _discover_strategy_classes() -> dict[str, Type[Strategy]]:
    """Import all .py modules in this package and collect Strategy subclasses (including aliases)."""
    result: dict[str, Type[Strategy]] = {}
    this_pkg = __name__
    for _importer, modname, _ispkg in pkgutil.iter_modules(__path__, prefix=this_pkg + "."):
        if modname == this_pkg + ".__init__":
            continue
        try:
            mod = importlib.import_module(modname)
            for attr_name in dir(mod):
                if attr_name.startswith("_"):
                    continue
                obj = getattr(mod, attr_name)
                if (
                    isinstance(obj, type)
                    and issubclass(obj, Strategy)
                    and obj is not Strategy
                ):
                    result[attr_name] = obj
        except Exception:
            continue
    return result


_discovered = _discover_strategy_classes()
globals().update(_discovered)

__all__ = [
    "Strategy",
    "StrategyContext",
    *sorted(_discovered.keys()),
]
