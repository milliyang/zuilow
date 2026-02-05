"""
Scheduler: automated strategy pre-execution and market execution.

Loads config/scheduler.yaml. Auto-adds exec_{market}_open and exec_{market}_bar from
config "markets" (HK, US, BTC by default). Pre-execution writes signals to store;
market execution consumes pending signals at market_open/open_bar/at_time.

Classes:
    Scheduler, JobConfig   Main scheduler and job config; see scheduler.py
    TriggerType, CronTrigger, IntervalTrigger, EventTrigger,
    MarketOpenTrigger, OpenBarTrigger, AtTimeTrigger, EventBus   Triggers; see triggers.py
    StrategyRunner   Run strategies and produce signals; see runner.py
    get_history_db, JobHistory, HistoryDB   Job run history; see history.py
    Notifier, NotificationConfig, get_notifier, set_notifier   Notifications; see notifier.py
    get_event_bus   Event bus for event-triggered jobs

"""

from .triggers import (
    TriggerType,
    CronTrigger,
    IntervalTrigger,
    EventTrigger,
    MarketOpenTrigger,
    OpenBarTrigger,
    AtTimeTrigger,
    get_event_bus,
    EventBus,
)
from .runner import StrategyRunner
from .scheduler import Scheduler, JobConfig
from .history import get_history_db, JobHistory, HistoryDB
from .notifier import Notifier, NotificationConfig, get_notifier, set_notifier

__all__ = [
    "Scheduler",
    "JobConfig",
    "TriggerType",
    "CronTrigger",
    "IntervalTrigger",
    "EventTrigger",
    "MarketOpenTrigger",
    "OpenBarTrigger",
    "AtTimeTrigger",
    "StrategyRunner",
    "get_history_db",
    "JobHistory",
    "HistoryDB",
    "Notifier",
    "NotificationConfig",
    "get_notifier",
    "set_notifier",
    "get_event_bus",
    "EventBus",
]
