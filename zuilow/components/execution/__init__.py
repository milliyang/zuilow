"""
Market execution: consume pending signals from store and send orders.

Triggered by scheduler (market_open / open_bar / at_time). Reads pending signals by
account/market from SignalStore; executes order-type (single POST /api/order) and
rebalance-type (target_weights/target_mv -> multiple orders); updates signal status.

Classes:
    SignalExecutor, get_signal_executor, set_signal_executor   See signal_executor.py

"""

from .signal_executor import SignalExecutor, get_signal_executor, set_signal_executor

__all__ = [
    "SignalExecutor",
    "get_signal_executor",
    "set_signal_executor",
]
