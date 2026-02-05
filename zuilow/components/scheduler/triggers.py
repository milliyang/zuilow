"""
Triggers: when strategy or execution jobs run.

Each trigger type has .should_run(now) or .should_run(event); scheduler uses these to decide run time.
Execution jobs use market_open, open_bar, at_time; strategy jobs use cron, interval, event.

Classes:
    TriggerType         Enum: CRON, INTERVAL, EVENT, MARKET_OPEN, OPEN_BAR, AT_TIME
    CronTrigger         .should_run(now) via cron expression (croniter)
    IntervalTrigger     .should_run(now) via minutes/hours; optional start_time, end_time
    MarketOpenTrigger   .should_run(now) at market_open_time (e.g. "09:30")
    OpenBarTrigger      .should_run(now) every open_bar_minutes (scheduler uses job.last_run)
    AtTimeTrigger       .should_run(now) via cron expression
    EventTrigger        .should_run(event) via event_type and condition (op: ==, >, <, in, ...)
    EventBus            .subscribe(event_type, callback), .publish(event), .unsubscribe(event_type, callback)

Trigger config (typical):
    CronTrigger: cron="30 16 * * 1-5", timezone="Asia/Hong_Kong"
    IntervalTrigger: minutes=5, hours=None, start_time, end_time
    MarketOpenTrigger: market="HK", time_str="09:30"
    OpenBarTrigger: open_bar_minutes=5 (scheduler passes last_run)

Trigger features:
    - Cron: standard cron (minute hour day month weekday); requires croniter
    - Interval: fixed minutes/hours with optional time window
    - Event: publish/subscribe; condition match on event payload (op: ==, >, <, in)
    - Market open/bar/at_time: for execution jobs (consume pending signals at market time)

Functions:
    get_event_bus() -> EventBus
"""

from __future__ import annotations

import logging
from enum import Enum
from datetime import datetime, time, timezone
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Trigger type."""
    CRON = "cron"
    INTERVAL = "interval"
    EVENT = "event"
    MARKET_OPEN = "market_open"   # Execute pending signals at market open
    OPEN_BAR = "open_bar"         # Execute at bar boundary (interval)
    AT_TIME = "at_time"          # Execute pending at fixed time (cron-like)


@dataclass
class CronTrigger:
    """
    Cron trigger. Format: "minute hour day month weekday".
    Examples: "30 16 * * 1-5" (Mon-Fri 16:30), "0 9 * * *" (daily 9:00).
    """
    cron: str
    timezone: str = "Asia/Hong_Kong"

    def should_run(self, now: datetime) -> bool:
        """
        Whether to run at current time (cron match).

        Args:
            now: Current datetime

        Returns:
            True if cron expression matches now
        """
        try:
            from croniter import croniter
            return croniter.match(self.cron, now)
        except ImportError:
            logger.error("croniter not installed, cannot use cron trigger")
            return False
        except Exception as e:
            logger.error(f"Cron parse failed: {e}")
            return False


@dataclass
class IntervalTrigger:
    """
    Interval trigger (fixed interval).

    Attributes:
        minutes: Interval in minutes (optional)
        hours: Interval in hours (optional)
        start_time: Optional time window start
        end_time: Optional time window end
    """
    minutes: Optional[int] = None
    hours: Optional[int] = None
    start_time: Optional[time] = None
    end_time: Optional[time] = None

    def __post_init__(self):
        if not self.minutes and not self.hours:
            raise ValueError("Must specify minutes or hours")
        self.interval_seconds = 0
        if self.minutes:
            self.interval_seconds += self.minutes * 60
        if self.hours:
            self.interval_seconds += self.hours * 3600
        
        self._last_run: Optional[datetime] = None

    def should_run(self, now: datetime, last_run: Optional[datetime] = None) -> bool:
        """
        Whether to run at current time (interval elapsed, optional time window).

        Args:
            now: Current datetime
            last_run: When the job last ran (from scheduler); if provided, used for
                      elapsed calculation. If None, uses internal _last_run (first run -> True).

        Returns:
            True if interval has elapsed since last run and within start_time/end_time
        """
        if self.start_time and now.time() < self.start_time:
            return False
        if self.end_time and now.time() > self.end_time:
            return False
        run_time = last_run if last_run is not None else self._last_run
        if run_time is None:
            self._last_run = now
            return True
        elapsed = (now - run_time).total_seconds()
        if elapsed >= self.interval_seconds:
            return True
        return False


@dataclass
class MarketOpenTrigger:
    """
    Execute at market open time. Time and timezone come only from config (market_open_time + market_timezone).

    Attributes:
        market: Market code (HK, US, BTC)
        time_str: Time string (e.g. "09:30")
        timezone: Timezone for comparison (e.g. "America/New_York"). From config; if empty, UTC.
    """
    market: str  # HK, US, BTC
    time_str: str  # "09:30"
    timezone: Optional[str] = None  # From config; if empty, UTC

    def should_run(self, now: datetime) -> bool:
        try:
            from zoneinfo import ZoneInfo
            tz_name = (self.timezone or "").strip() or "UTC"
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            tz = ZoneInfo(tz_name)
            now_local = now.astimezone(tz)
            parts = self.time_str.strip().split(":")
            h = int(parts[0]) if len(parts) > 0 else 9
            m = int(parts[1]) if len(parts) > 1 else 30
            return now_local.hour == h and now_local.minute == m
        except Exception as e:
            logger.error(f"MarketOpenTrigger: {e}")
            return False


@dataclass
class OpenBarTrigger:
    """Execute at bar boundary (e.g. every 5 min). Tracks last run like IntervalTrigger."""
    market: str
    minutes: int = 5
    _last_run: Optional[datetime] = None

    def should_run(self, now: datetime) -> bool:
        if self._last_run is None:
            self._last_run = now
            return True
        elapsed = (now - self._last_run).total_seconds()
        if elapsed >= self.minutes * 60:
            self._last_run = now
            return True
        return False


@dataclass
class AtTimeTrigger:
    """Execute pending signals at fixed time (cron expression)."""
    cron: str
    timezone: str = "Asia/Hong_Kong"

    def should_run(self, now: datetime) -> bool:
        try:
            from croniter import croniter
            return croniter.match(self.cron, now)
        except Exception as e:
            logger.error(f"AtTimeTrigger: {e}")
            return False


@dataclass
class EventTrigger:
    """Event trigger (data_update, price_alert, order_fill, etc.)."""
    event_type: str
    condition: dict

    def should_run(self, event: dict) -> bool:
        """Whether event satisfies trigger condition."""
        if not event or event.get('type') != self.event_type:
            return False
        for key, expected_value in self.condition.items():
            actual_value = event.get(key)
            if isinstance(expected_value, dict):
                operator = expected_value.get('op', '==')
                value = expected_value.get('value')
                
                if operator == '==':
                    if actual_value != value:
                        return False
                elif operator == '>':
                    if not (actual_value and actual_value > value):
                        return False
                elif operator == '<':
                    if not (actual_value and actual_value < value):
                        return False
                elif operator == '>=':
                    if not (actual_value and actual_value >= value):
                        return False
                elif operator == '<=':
                    if not (actual_value and actual_value <= value):
                        return False
                elif operator == 'in':
                    if actual_value not in value:
                        return False
            else:
                if actual_value != expected_value:
                    return False
        return True


class EventBus:
    """Event bus for publish/subscribe; triggers strategy tasks."""

    def __init__(self):
        self._subscribers: dict[str, list] = {}
        self._event_queue = []

    def subscribe(self, event_type: str, callback):
        """Subscribe to event."""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)
        logger.info(f"Subscribed: {event_type}")

    def unsubscribe(self, event_type: str, callback):
        """Unsubscribe."""
        if event_type in self._subscribers:
            self._subscribers[event_type].remove(callback)

    def publish(self, event: dict):
        """Publish event."""
        event_type = event.get('type')
        if not event_type:
            logger.warning("Event missing type")
            return
        logger.debug(f"Publish: {event_type}")
        if event_type in self._subscribers:
            for callback in self._subscribers[event_type]:
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Event handler failed: {e}")
        if '*' in self._subscribers:
            for callback in self._subscribers['*']:
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Wildcard handler failed: {e}")


_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Get global event bus."""
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
    return _event_bus
