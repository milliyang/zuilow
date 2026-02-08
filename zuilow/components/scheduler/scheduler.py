"""
Scheduler: strategy pre-execution and market execution.

Loads config/scheduler.yaml. Auto-adds exec_{market}_open and exec_{market}_bar jobs from
config "markets" section (HK, US, BTC by default) so each market has built-in execution tasks.

Classes:
    JobConfig   Job config dataclass (name, strategy, trigger, account, market, send_immediately, ...)
    Scheduler   Main scheduler class

JobConfig fields:
    name, strategy, config, symbols, trigger, mode, account, market, send_immediately,
    cron, minutes, hours, event_type, event_condition, market_open_time, market_close_time, open_bar_minutes,
    at_time_cron, priority, enabled, last_run, next_run, run_count, error_count, is_running

Scheduler methods:
    .start() -> None
    .stop() -> None
    .get_jobs() -> list[JobConfig]
    .add_job(job_config: JobConfig) -> None
    .remove_job(job_name: str) -> None
    .is_running -> bool

Scheduler config:
    Default config path: config/scheduler.yaml
    DEFAULT_MARKETS: HK (09:30, 5min bar), US (09:30, 5min), BTC (60min bar)

Scheduler features:
    - Pre-execution: cron/interval/event -> run strategy, write signals to store (or send_immediately)
    - Market execution: market_open/open_bar/at_time -> run SignalExecutor for that market
    - Auto-inject exec_HK_open, exec_HK_bar, exec_US_open, exec_US_bar, exec_BTC_bar from markets config
    - Event bus for event-type triggers; croniter for cron/at_time
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone
import yaml

from .triggers import (
    TriggerType,
    CronTrigger,
    IntervalTrigger,
    EventTrigger,
    MarketOpenTrigger,
    OpenBarTrigger,
    AtTimeTrigger,
    get_event_bus,
)
from .runner import StrategyRunner
from .history import get_history_db, JobHistory
from .notifier import get_notifier
import zuilow.components.control.ctrl as ctrl
from zuilow.components.signals import get_signal_store, SignalStatus

logger = logging.getLogger(__name__)

# Per-market execution: each market gets exec_*_open / open_bar jobs that consume pending signals at configured time.
# market_open_time + market_timezone are set in config; trigger only converts and compares.
DEFAULT_MARKETS = {
    "HK": {"market_open_time": "09:30", "market_timezone": "Asia/Hong_Kong", "open_bar_minutes": 5},
    "US": {"market_open_time": "09:30", "market_timezone": "America/New_York", "open_bar_minutes": 5},
    "BTC": {"open_bar_minutes": 60},  # 24/7, run at bar interval
}


@dataclass
class JobConfig:
    """
    Job config for scheduler.

    Fields:
        name: Unique job name
        strategy: Strategy class name (empty for execution-only jobs)
        config: Strategy config path (empty for execution-only)
        symbols: List of symbols for strategy
        trigger: cron | interval | event | market_open | open_bar | at_time
        mode: paper | live
        account: Account name (for signals)
        market: Market code (e.g. HK, US) for signals and execution jobs
        send_immediately: If True, send signals to gateway at once; else write to signal store only
        cron, minutes, hours, event_type, event_condition: For cron/interval/event triggers
        market_open_time, market_close_time, market_timezone, open_bar_minutes, at_time_cron: For market_open/market_close/open_bar/at_time triggers
        priority: Lower = higher priority (default 5)
        enabled: Whether job is enabled
        last_run, next_run, run_count, error_count, is_running: Runtime state
    """
    name: str
    strategy: str = ""      # Empty for execution-only jobs (market_open/open_bar/at_time)
    config: str = ""        # Strategy config path; empty for execution-only
    symbols: list[str] = field(default_factory=list)
    trigger: str = "cron"   # cron / interval / event / market_open / open_bar / at_time
    mode: str = "paper"
    account: Optional[str] = None
    market: Optional[str] = None   # e.g. HK, US (for signal filter and market-time execution)
    send_immediately: bool = False  # If True, send signals to gateway at once; else write to signal store only
    cron: Optional[str] = None
    minutes: Optional[int] = None
    hours: Optional[int] = None
    days: Optional[int] = None
    event_type: Optional[str] = None
    event_condition: Optional[dict] = None
    market_open_time: Optional[str] = None   # "09:30" for market_open trigger
    market_close_time: Optional[str] = None   # "16:00" for market_close trigger
    market_timezone: Optional[str] = None    # e.g. "America/New_York"; used by market_open/market_close (config is source of truth)
    open_bar_minutes: Optional[int] = None  # for open_bar trigger
    at_time_cron: Optional[str] = None      # for at_time trigger
    priority: int = 5        # Lower = higher priority
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0
    is_running: bool = False


class Scheduler:
    """
    Strategy scheduler: pre-execution (strategy -> signals) and market execution (executor).

    Supports:
    - Pre-execution jobs: cron/interval/event -> run StrategyRunner, write signals to store or send_immediately
    - Market execution jobs: market_open/open_bar/at_time -> run SignalExecutor for that market
    - Auto-inject exec_{market}_open, exec_{market}_bar from config "markets" (HK, US, BTC default)

    Features:
    - Load config/scheduler.yaml; JobConfig per job
    - Background thread checks triggers; StrategyRunner and SignalExecutor for execution
    - get_jobs(), add_job(), remove_job(); start()/stop()
    """

    def __init__(self, config_path: Optional[str] = None, max_workers: int = 3):
        self.config_path = config_path or self._default_config_path()
        self.jobs: dict[str, JobConfig] = {}
        self.runner = StrategyRunner()
        self.max_workers = max_workers
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._executor = None
        self._event_bus = get_event_bus()
        self._load_config()
        self._register_event_jobs()

    def _default_config_path(self) -> Path:
        """Default config path."""
        return Path(__file__).parent.parent.parent / "config" / "scheduler" / "scheduler.yaml"

    def _load_config(self):
        """Load scheduler config."""
        if not self.config_path.exists():
            logger.warning(f"Scheduler config not found: {self.config_path}")
            logger.info("Running with empty config")
            return

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}

            # Load markets from config/scheduler/markets.yaml (same dir as scheduler.yaml)
            markets_path = self.config_path.parent / "markets.yaml"
            if markets_path.exists():
                try:
                    with open(markets_path, 'r', encoding='utf-8') as f:
                        markets_data = yaml.safe_load(f) or {}
                    config["markets"] = markets_data.get("markets", config.get("markets"))
                except Exception as e:
                    logger.warning("Load markets.yaml failed: %s", e)

            scheduler_config = config.get("scheduler", {})
            if not scheduler_config.get("enabled", True):
                logger.info("Scheduler disabled in config")
                return

            jobs = scheduler_config.get("jobs", [])
            fields = {f.name for f in (JobConfig.__dataclass_fields__.values())}
            for job_data in jobs:
                job_kw = {k: v for k, v in job_data.items() if k in fields}
                # Normalize enabled: support bool or string "true"/"false"
                if "enabled" in job_kw and isinstance(job_kw["enabled"], str):
                    job_kw["enabled"] = job_kw["enabled"].lower() in ("true", "1", "yes")
                job_config = JobConfig(**job_kw)
                self.jobs[job_config.name] = job_config
                logger.info(f"Loaded job: {job_config.name}" + (" (disabled)" if not job_config.enabled else ""))

            # Auto-add per-market execution jobs (if not in jobs): run executor for that market at configured time.
            # Skip auto execution jobs for markets with enabled: false.
            markets_config = config.get("markets", DEFAULT_MARKETS)
            for market_name, m in (markets_config or DEFAULT_MARKETS).items():
                if not isinstance(m, dict):
                    continue
                market_enabled = m.get("enabled", True)
                if isinstance(market_enabled, str):
                    market_enabled = market_enabled.lower() in ("true", "1", "yes")
                if not market_enabled:
                    logger.info(f"Market {market_name} is disabled, skipping auto execution jobs")
                    continue
                open_time = m.get("market_open_time")
                close_time = m.get("market_close_time")
                bar_minutes = m.get("open_bar_minutes")
                open_job_name = f"exec_{market_name}_open"
                close_job_name = f"exec_{market_name}_close"
                bar_job_name = f"exec_{market_name}_bar"
                if open_time and open_job_name not in self.jobs:
                    self.jobs[open_job_name] = JobConfig(
                        name=open_job_name,
                        trigger="market_open",
                        market=market_name,
                        market_open_time=open_time,
                        market_timezone=m.get("market_timezone") or None,
                        priority=3,
                    )
                    logger.info(f"Auto-added execution job: {open_job_name}")
                if close_time and close_job_name not in self.jobs:
                    self.jobs[close_job_name] = JobConfig(
                        name=close_job_name,
                        trigger="market_close",
                        market=market_name,
                        market_close_time=close_time,
                        market_timezone=m.get("market_timezone") or None,
                        priority=3,
                    )
                    logger.info(f"Auto-added execution job: {close_job_name}")
                if bar_minutes and bar_job_name not in self.jobs:
                    self.jobs[bar_job_name] = JobConfig(
                        name=bar_job_name,
                        trigger="open_bar",
                        market=market_name,
                        open_bar_minutes=bar_minutes,
                        priority=5,
                    )
                    logger.info(f"Auto-added execution job: {bar_job_name}")

            logger.info(f"Loaded {len(self.jobs)} scheduler jobs")

            # Notification config
            notification_config = config.get("notification", {})
            if notification_config.get("enabled", False):
                from .notifier import Notifier, NotificationConfig, set_notifier
                
                notify_cfg = NotificationConfig(
                    enabled=True,
                    types=notification_config.get("types", []),
                    email_smtp_host=notification_config.get("email_smtp_host", ""),
                    email_smtp_port=notification_config.get("email_smtp_port", 465),
                    email_from=notification_config.get("email_from", ""),
                    email_password=notification_config.get("email_password", ""),
                    email_to=notification_config.get("email_to", []),
                    webhook_url=notification_config.get("webhook_url", ""),
                    webhook_headers=notification_config.get("webhook_headers", {}),
                    dingtalk_webhook=notification_config.get("dingtalk_webhook", ""),
                    dingtalk_secret=notification_config.get("dingtalk_secret", ""),
                    notify_on_success=notification_config.get("notify_on_success", False),
                    notify_on_failure=notification_config.get("notify_on_failure", True),
                    notify_on_signal=notification_config.get("notify_on_signal", True),
                )
                
                notifier = Notifier(notify_cfg)
                set_notifier(notifier)
                logger.info(f"Notifier configured: {notify_cfg.types}")

        except Exception as e:
            logger.error(f"Config load failed: {e}")

    def reload_config(self) -> bool:
        """Reload scheduler config from disk (scheduler.yaml). Use after editing yaml so enabled/disabled and job list take effect without restart. Returns True if load succeeded."""
        try:
            self.jobs.clear()
            self._load_config()
            logger.info("Scheduler config reloaded from %s", self.config_path)
            return True
        except Exception as e:
            logger.error(f"Scheduler reload_config failed: {e}")
            return False

    def start(self):
        """Start scheduler."""
        if self._running:
            logger.warning("Scheduler already running")
            return
        self._running = True
        self._stop_event.clear()
        from concurrent.futures import ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"Scheduler started (max_workers: {self.max_workers})")

    def stop(self):
        """Stop scheduler."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        logger.info("Scheduler stopped")

    def _get_now(self) -> datetime:
        """Current time: unified via ctrl (tick or stime fetch)."""
        return ctrl.get_current_dt()

    def _is_execution_job(self, job: JobConfig) -> bool:
        """True if job is market execution (open_bar, market_open, etc.), which consumes pending signals."""
        return job.trigger in ("market_open", "market_close", "open_bar", "at_time")

    def run_one_tick(self) -> int:
        """
        Run one scheduler tick (for replay). Returns number of jobs executed.
        Execution jobs (open_bar, market_open, ...) run after strategy jobs in the same tick
        so they see signals just written by strategy jobs.
        """
        now = self._get_now()
        executed = 0
        # 同一 tick 内先跑策略（产 signal），再跑执行（消费 pending）；key=(是否执行类, priority)
        sorted_jobs = sorted(
            self.jobs.items(),
            key=lambda x: (self._is_execution_job(x[1]), x[1].priority),
        )
        for job_name, job_config in sorted_jobs:
            if not job_config.enabled or job_config.is_running:
                continue
            if self._should_run_job(job_config, now):
                self._execute_job(job_config)
                executed += 1
        return executed

    def _run_loop(self):
        """Scheduler main loop."""
        logger.info("Scheduler main loop started")
        while self._running:
            try:
                now = self._get_now()
                sorted_jobs = sorted(
                    self.jobs.items(),
                    key=lambda x: (self._is_execution_job(x[1]), x[1].priority),
                )
                for job_name, job_config in sorted_jobs:
                    if not job_config.enabled:
                        continue
                    if job_config.is_running:
                        logger.debug(f"Job {job_name} running, skip")
                        continue
                    if self._should_run_job(job_config, now):
                        if self._executor:
                            self._executor.submit(self._execute_job, job_config)
                        else:
                            self._execute_job(job_config)
                if self._stop_event.wait(timeout=10):
                    break
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                time.sleep(10)
        logger.info("Scheduler main loop ended")

    def _should_run_job(self, job: JobConfig, now: datetime) -> bool:
        """Whether job should run."""
        trigger_type = TriggerType(job.trigger)
        if trigger_type == TriggerType.CRON:
            if not job.cron:
                logger.error(f"Job {job.name} missing cron")
                return False
            trigger = CronTrigger(cron=job.cron)
            return trigger.should_run(now)
        elif trigger_type == TriggerType.INTERVAL:
            if not job.minutes and not job.hours and not job.days:
                logger.error(f"Job {job.name} missing interval")
                return False
            trigger = IntervalTrigger(minutes=job.minutes, hours=job.hours, days=job.days)
            return trigger.should_run(now, last_run=job.last_run)
        elif trigger_type == TriggerType.EVENT:
            return False
        elif trigger_type == TriggerType.MARKET_OPEN:
            if not job.market or not job.market_open_time:
                return False
            trigger = MarketOpenTrigger(
                market=job.market,
                time_str=job.market_open_time,
                timezone=(job.market_timezone or "").strip() or None,
            )
            return trigger.should_run(now)
        elif trigger_type == TriggerType.MARKET_CLOSE:
            if not job.market or not job.market_close_time:
                return False
            trigger = MarketOpenTrigger(
                market=job.market,
                time_str=job.market_close_time,
                timezone=(job.market_timezone or "").strip() or None,
            )
            return trigger.should_run(now)
        elif trigger_type == TriggerType.OPEN_BAR:
            if not job.market:
                return False
            tz_str = (job.market_timezone or "").strip()
            if tz_str:
                try:
                    from zoneinfo import ZoneInfo
                    if now.tzinfo is None:
                        now_utc = now.replace(tzinfo=timezone.utc)
                    else:
                        now_utc = now
                    now_local = now_utc.astimezone(ZoneInfo(tz_str))
                    if now_local.weekday() >= 5:
                        return False
                except Exception:
                    pass
            minutes = job.open_bar_minutes or 5
            if job.last_run is None:
                return True
            elapsed = (now - job.last_run).total_seconds()
            return elapsed >= minutes * 60
        elif trigger_type == TriggerType.AT_TIME:
            if not job.at_time_cron:
                return False
            trigger = AtTimeTrigger(cron=job.at_time_cron)
            return trigger.should_run(now)
        return False

    def _execute_market_execution(self, job: JobConfig):
        """Run executor: consume pending signals for this account/market."""
        from zuilow.components.execution import get_signal_executor
        executor = get_signal_executor()
        # When account is empty (e.g. auto-injected exec_* jobs), pass None so list_pending returns all accounts for market
        account = job.account if (job.account and job.account.strip()) else None
        trigger_at = self._get_now()
        result = executor.run_once(account=account, market=job.market, trigger_at=trigger_at)
        logger.info(
            f"Market execution {job.name}: pending={result.get('pending', 0)}, "
            f"executed={result['executed']}, failed={result['failed']}"
        )
        job.last_run = self._get_now()
        job.run_count += 1

    def _register_event_jobs(self):
        """Register event-triggered jobs with event bus."""
        for job_name, job in self.jobs.items():
            if job.trigger == "event" and job.event_type:
                def make_handler(j):
                    def handler(event):
                        trigger = EventTrigger(
                            event_type=j.event_type,
                            condition=j.event_condition or {}
                        )
                        if trigger.should_run(event):
                            logger.info(f"Event-triggered job: {j.name}")
                            if self._executor:
                                self._executor.submit(self._execute_job, j)
                            else:
                                self._execute_job(j)
                    return handler
                handler = make_handler(job)
                self._event_bus.subscribe(job.event_type, handler)
                logger.info(f"Registered event job: {job_name} -> {job.event_type}")

    def _execute_job(self, job: JobConfig):
        """Execute job (strategy pre-exec or market execution)."""
        job.is_running = True
        logger.info(f"Executing job: {job.name} (priority: {job.priority})")
        import json
        if job.trigger in ("market_open", "market_close", "open_bar", "at_time"):
            self._execute_market_execution(job)
            job.is_running = False
            return
        history_db = get_history_db()
        trigger_time = self._get_now()
        history = JobHistory(
            job_name=job.name,
            strategy=job.strategy,
            symbols=json.dumps(job.symbols),
            trigger_time=trigger_time.isoformat(),
            start_time=trigger_time.isoformat(),
            status="running"
        )
        history_id = history_db.add_history(history)
        try:
            strategy_config = self.runner.get_strategy_config(job.strategy, job.config or None)
            strategy = self.runner.create_strategy(job.strategy, strategy_config)
            signals = self.runner.run_strategy(
                strategy, job.symbols, job.mode,
                account=job.account or None,
                job_name=job.name,
                market=job.market or None,
            )
            account = job.account or ""
            if signals:
                for s in signals:
                    s["account"] = account
                    s["mode"] = job.mode
            results = []
            if signals:
                # Always write to SignalStore so signals page and exec_* can consume
                trading_signals = self.runner.signals_dict_to_trading_signals(
                    signals, job.name, account, job.market, trigger_at=self._get_now()
                )
                ids = self.runner.write_signals_to_store(trading_signals)
                if job.send_immediately:
                    results = self.runner.send_signals(signals)
                    store = get_signal_store()
                    for sid in ids:
                        store.update_status(sid, SignalStatus.EXECUTED, executed_at=self._get_now())
                    logger.info(f"Job {job.name} sent {len(signals)} signals (immediate)")
                else:
                    logger.info(f"Job {job.name} wrote {len(signals)} signals to store")
            elif not signals:
                logger.info(f"Job {job.name} produced no signals")
            job.last_run = self._get_now()
            job.run_count += 1
            history_db.update_history(
                history_id,
                end_time=self._get_now().isoformat(),
                status="success",
                signals_count=len(signals),
                signals=json.dumps(signals) if signals else "[]"
            )
            notifier = get_notifier()
            if notifier:
                if signals:
                    notifier.notify(
                        "signal",
                        job.name,
                        f"Job {job.name} produced {len(signals)} trading signals",
                        {"signals": signals, "mode": job.mode}
                    )
                else:
                    notifier.notify(
                        "success",
                        job.name,
                        f"Job {job.name} completed, no signals",
                        {"run_count": job.run_count}
                    )
        except Exception as e:
            logger.error(f"Job failed ({job.name}): {e}")
            job.error_count += 1
            history_db.update_history(
                history_id,
                end_time=self._get_now().isoformat(),
                status="failed",
                error_message=str(e)
            )
            notifier = get_notifier()
            if notifier:
                notifier.notify(
                    "failure",
                    job.name,
                    f"Job {job.name} failed: {str(e)}",
                    {"error": str(e), "error_count": job.error_count, "strategy": job.strategy}
                )
        finally:
            job.is_running = False

    def add_job(self, job_config: JobConfig):
        """Add job."""
        self.jobs[job_config.name] = job_config
        logger.info(f"Added job: {job_config.name}")

    def remove_job(self, job_name: str):
        """Remove job."""
        if job_name in self.jobs:
            del self.jobs[job_name]
            logger.info(f"Removed job: {job_name}")

    def run_job_now(self, job_name: str) -> bool:
        """
        Manually trigger one run of a job. Only allowed for enabled, user-defined (strategy) jobs.
        Returns True if the job was submitted to run, False if not found, disabled, or not a strategy job.
        """
        job = self.jobs.get(job_name)
        if not job:
            return False
        if not job.enabled:
            return False
        # Only user-defined jobs (have strategy); skip auto-injected exec_* jobs
        if not (job.strategy and job.strategy.strip()):
            return False
        if job.is_running:
            return False
        if self._executor:
            self._executor.submit(self._execute_job, job)
        else:
            self._execute_job(job)
        return True

    def get_jobs(self) -> list[JobConfig]:
        """Get all jobs."""
        return list(self.jobs.values())

    @property
    def is_running(self) -> bool:
        """Scheduler running status."""
        return self._running

    def add_job(self, job_config: JobConfig):
        """Add job."""
        self.jobs[job_config.name] = job_config
        logger.info(f"Added job: {job_config.name}")

    def remove_job(self, job_name: str):
        """Remove job."""
        if job_name in self.jobs:
            del self.jobs[job_name]
            logger.info(f"Removed job: {job_name}")

    def get_jobs(self) -> list[JobConfig]:
        """Get all jobs."""
        return list(self.jobs.values())

    @property
    def is_running(self) -> bool:
        """Scheduler running status."""
        return self._running
