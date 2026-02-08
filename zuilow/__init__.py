"""
ZuiLow - Quant trading platform.

Multi data source (yfinance, InfluxDB, DMS), cache/rate-limit (utils),
backtest (engine, strategies, SimulatedBroker), live trading (FutuGateway),
signals (TradingSignal, SignalStore), execution (SignalExecutor), Web UI (web).
Paper trading uses a separate PPT (Paper Trade) service.

Classes (main):
    DataSource, DataSourceConfig, DataSourceType, DataSourceManager, YFinanceSource, InfluxDB1Source
    BacktestEngine, BacktestConfig, Strategy, SimulatedBroker, Executor
    FutuGateway, MarketService
    TradingSignal, SignalKind, SignalStatus, SignalStore
    SignalExecutor
    get_manager, set_manager, get_signal_store, set_signal_store,     get_signal_executor, set_signal_executor
"""

__version__ = "0.1.0"

from .components.datasource import (
    DataSource,
    DataSourceConfig,
    DataSourceType,
    DataSourceManager,
    YFinanceSource,
    InfluxDB1Source,
    get_manager,
    set_manager,
)

from .components.utils import (
    LRUCache,
    cached,
    retry,
    RateLimiter,
)

from .components.backtest import (
    BacktestEngine,
    BacktestConfig,
    Strategy,
    Signal,
    Bar,
    SimulatedBroker,
    Executor,
    PaperTrader,
)

from .components.signals import (
    TradingSignal,
    SignalKind,
    SignalStatus,
    SignalStore,
    get_signal_store,
    set_signal_store,
)

from .components.execution import (
    SignalExecutor,
    get_signal_executor,
    set_signal_executor,
)
