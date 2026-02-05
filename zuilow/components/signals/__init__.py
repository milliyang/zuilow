"""
Trading signals: first-class model and storage (order and rebalance).

Pre-execution writes TradingSignal to store; market execution reads pending and sends orders.
Types: order (direct buy/sell), rebalance (target_weights or target_mv). Storage: SQLite.

Classes:
    TradingSignal, SignalKind, SignalStatus   Models; see signal_models.py
    SignalStore, get_signal_store, set_signal_store   Storage; see signal_store.py

"""

from .signal_models import (
    TradingSignal,
    SignalKind,
    SignalStatus,
)
from .signal_store import SignalStore, get_signal_store, set_signal_store

__all__ = [
    "TradingSignal",
    "SignalKind",
    "SignalStatus",
    "SignalStore",
    "get_signal_store",
    "set_signal_store",
]
