"""
ZuiLow backtest: strategy backtest, metrics, simulated broker.

Event-driven engine: run strategy on bar data, SimulatedBroker fills orders, BacktestMetrics
computes performance. Paper trading uses separate PPT (Paper Trade) service.

Classes:
    Bar, Signal, SignalType, Order, OrderSide, OrderType, OrderStatus,
    Position, Trade, Account   Types; see types.py
    Strategy, StrategyContext   Strategies; concrete strategies from zuilow.strategies
    BacktestEngine, BacktestConfig, BacktestResult   Engine; see engine.py
    BacktestMetrics, calculate_metrics   Metrics; see metrics.py
    SimulatedBroker, BrokerConfig, FillMode   Simulated broker; see broker.py
    Executor, ExecutorConfig, PaperTrader, PaperTraderConfig   Executor; see executor.py

"""

from .types import (
    Bar,
    Signal,
    SignalType,
    Order,
    OrderSide,
    OrderType,
    OrderStatus,
    Position,
    Trade,
    Account,
)

from .strategy import Strategy, StrategyContext
# Re-export all strategies discovered by zuilow.strategies (no need to list each one)
import zuilow.strategies as _strategy_pkg
_strategy_export = [n for n in getattr(_strategy_pkg, "__all__", []) if n not in ("Strategy", "StrategyContext")]
for _n in _strategy_export:
    globals()[_n] = getattr(_strategy_pkg, _n)

from .engine import (
    BacktestEngine,
    BacktestConfig,
    BacktestResult,
)

from .metrics import (
    BacktestMetrics,
    calculate_metrics,
)

from .broker import (
    SimulatedBroker,
    BrokerConfig,
    FillMode,
)

from .executor import (
    Executor,
    ExecutorConfig,
    # Backward compatibility
    BacktestExecutor,
    BacktestExecutorConfig,
    PaperTrader,
    PaperTraderConfig,
)

__all__ = [
    # Types
    "Bar",
    "Signal",
    "SignalType",
    "Order",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "Position",
    "Trade",
    "Account",
    # Strategy (from zuilow.strategies discovery)
    "Strategy",
    "StrategyContext",
    *_strategy_export,
    # Engine
    "BacktestEngine",
    "BacktestConfig",
    "BacktestResult",
    # Metrics
    "BacktestMetrics",
    "calculate_metrics",
    # Broker
    "SimulatedBroker",
    "BrokerConfig",
    "FillMode",
    # Executor
    "Executor",
    "ExecutorConfig",
    # Backward compatibility
    "BacktestExecutor",
    "BacktestExecutorConfig",
    "PaperTrader",
    "PaperTraderConfig",
]
