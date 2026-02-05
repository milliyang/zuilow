"""
Maintenance scheduler: triggers maintenance tasks (incremental, full_sync, validation) by cron or interval; background thread.

Used for: DMS.start() starts scheduler; tasks run on schedule or via trigger_task.

Classes:
    MaintenanceScheduler  Maintenance task scheduler

MaintenanceScheduler methods:
    .start() -> None                           Start scheduler loop
    .stop() -> None                            Stop scheduler
    .trigger_task(task_name) -> Dict           Manually trigger one task
    .get_task_status(task_name) -> Dict        Task status (idle/running/completed/failed)
    .get_tasks() -> List[Dict]                  Task config list

MaintenanceScheduler features:
    - Task types: incremental (fetch from latest and write), full_sync (history from task config), validation (read-only)
    - croniter required for cron triggers; cron disabled if not installed
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# For UTC naive datetime (croniter compatibility)
def utcnow():
    """Get current UTC time as naive datetime (for croniter compatibility)"""
    return datetime.now(timezone.utc).replace(tzinfo=None)

from ..tasks import MaintenanceTask
from ..tasks.incremental_update import IncrementalUpdateTask
from ..tasks.full_sync import FullSyncTask
from ..tasks.data_validation import DataValidationTask
from ..storage.maintenance_log import MaintenanceLog

logger = logging.getLogger(__name__)

try:
    import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False
    logger.warning("croniter not installed, cron triggers will not work")


class MaintenanceScheduler:
    """
    Maintenance scheduler
    
    Manages task scheduling and execution.
    Supports Cron and Interval triggers.
    """
    
    def __init__(
        self,
        tasks_config: List[Dict[str, Any]],
        fetcher,
        writer,
        reader,
        sync_manager: Optional[Any] = None,
    ):
        """
        Initialize maintenance scheduler
        
        Args:
            tasks_config: List of task configurations
            fetcher: DataFetcher instance
            writer: DataWriter instance
            reader: DataReader instance
            sync_manager: SyncManager instance (optional)
        """
        self.tasks_config = tasks_config
        self.fetcher = fetcher
        self.writer = writer
        self.reader = reader
        self.sync_manager = sync_manager
        
        self.maintenance_log = MaintenanceLog()
        
        self._tasks: Dict[str, MaintenanceTask] = {}
        self._task_status: Dict[str, str] = {}  # idle, running, completed, failed
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Load tasks
        self._load_tasks()
    
    def _load_tasks(self):
        """Load tasks from configuration"""
        for task_config in self.tasks_config:
            task_name = task_config.get("name")
            task_type = task_config.get("type")
            
            if not task_name or not task_type:
                logger.warning(f"Invalid task config: {task_config}")
                continue
            
            try:
                if task_type == "incremental":
                    task = IncrementalUpdateTask(
                        name=task_name,
                        fetcher=self.fetcher,
                        writer=self.writer,
                        sync_manager=self.sync_manager,
                        config=task_config,
                    )
                elif task_type == "full_sync":
                    task = FullSyncTask(
                        name=task_name,
                        fetcher=self.fetcher,
                        writer=self.writer,
                        sync_manager=self.sync_manager,
                        config=task_config,
                    )
                elif task_type == "validation":
                    task = DataValidationTask(
                        name=task_name,
                        reader=self.reader,
                        config=task_config,
                    )
                else:
                    logger.warning(f"Unknown task type: {task_type}")
                    continue
                
                self._tasks[task_name] = task
                self._task_status[task_name] = "idle"
                logger.info(f"Loaded task: {task_name} ({task_type})")
                
            except Exception as e:
                logger.error(f"Failed to load task {task_name}: {e}", exc_info=True)
    
    def start(self):
        """Start scheduler"""
        if self._running:
            logger.warning("Scheduler is already running")
            return
        
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        logger.info("Maintenance scheduler started")
    
    def stop(self):
        """Stop scheduler"""
        self._running = False
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        logger.info("Maintenance scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop"""
        last_check = {}
        
        while self._running:
            try:
                # Use naive UTC datetime for croniter compatibility
                # Cron expressions in config are in UTC time
                current_time = utcnow()
                
                for task_name, task_config in zip(self._tasks.keys(), self.tasks_config):
                    if not self._running:
                        break
                    
                    trigger = task_config.get("trigger", "cron")
                    
                    if trigger == "cron":
                        if not HAS_CRONITER:
                            logger.warning(f"Cron trigger requires croniter, skipping {task_name}")
                            continue
                        
                        cron_expr = task_config.get("cron")
                        if not cron_expr:
                            continue
                        
                        # Check if it's time to run
                        if task_name not in last_check:
                            last_check[task_name] = current_time
                        
                        try:
                            # Use UTC timezone for cron schedule (market times are in UTC)
                            # Cron expressions in config are already in UTC time
                            # croniter works with naive datetime, we use UTC naive datetime
                            cron = croniter.croniter(cron_expr, last_check[task_name])
                            next_run = cron.get_next(datetime)
                            
                            if current_time >= next_run:
                                self._run_task(task_name)
                                last_check[task_name] = current_time
                        except Exception as e:
                            logger.error(f"Error checking cron for {task_name}: {e}")
                    
                    elif trigger == "interval":
                        interval = task_config.get("interval", 3600)  # Default 1 hour
                        
                        if task_name not in last_check:
                            last_check[task_name] = current_time
                        
                        elapsed = (current_time - last_check[task_name]).total_seconds()
                        if elapsed >= interval:
                            self._run_task(task_name)
                            last_check[task_name] = current_time
                
                # Sleep for 60 seconds before next check
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                time.sleep(60)
    
    def _run_task(self, task_name: str):
        """Run a task"""
        if task_name not in self._tasks:
            logger.warning(f"Task not found: {task_name}")
            return
        
        # Check if task is already running
        with self._lock:
            if self._task_status.get(task_name) == "running":
                logger.debug(f"Task {task_name} is already running, skipping")
                return
            self._task_status[task_name] = "running"
        
        # Run task in thread pool
        def run_task_async():
            try:
                task = self._tasks[task_name]
                start_time = datetime.now()
                
                # Add log entry
                log_id = self.maintenance_log.add_log(
                    task_name=task_name,
                    task_type=task.config.get("type"),
                    start_time=start_time,
                    status="running",
                )
                
                # Execute task
                result = task.run()
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # Update log
                status_str = "completed" if result.get("success", True) else "failed"
                self.maintenance_log.update_log(
                    log_id=log_id,
                    end_time=end_time,
                    status=status_str,
                    duration=duration,
                    result_message=result.get("message"),
                    data_count=result.get("data_count", 0),
                    error_message=result.get("error"),
                )
                
                with self._lock:
                    self._task_status[task_name] = status_str
                
                logger.info(f"Task {task_name} completed: {result.get('message')}")
                
            except Exception as e:
                logger.error(f"Error running task {task_name}: {e}", exc_info=True)
                with self._lock:
                    self._task_status[task_name] = "failed"
        
        # Run in background thread
        threading.Thread(target=run_task_async, daemon=True).start()
    
    def trigger_task(self, task_name: str) -> Dict[str, Any]:
        """
        Manually trigger a task (runs asynchronously in background)
        
        Args:
            task_name: Task name
        
        Returns:
            Immediate response indicating task was triggered
        """
        if task_name not in self._tasks:
            raise ValueError(f"Task not found: {task_name}")
        
        # Check if task is already running
        with self._lock:
            if self._task_status.get(task_name) == "running":
                return {
                    "success": False,
                    "message": f"Task {task_name} is already running",
                    "status": "running",
                }
            self._task_status[task_name] = "running"
        
        # Run task in background thread (non-blocking)
        def run_task_async():
            try:
                task = self._tasks[task_name]
                start_time = datetime.now()
                
                # Add log entry
                log_id = self.maintenance_log.add_log(
                    task_name=task_name,
                    task_type=task.config.get("type"),
                    start_time=start_time,
                    status="running",
                )
                
                # Execute task
                result = task.run()
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # Update log
                status_str = "completed" if result.get("success", True) else "failed"
                self.maintenance_log.update_log(
                    log_id=log_id,
                    end_time=end_time,
                    status=status_str,
                    duration=duration,
                    result_message=result.get("message"),
                    data_count=result.get("data_count", 0),
                    error_message=result.get("error"),
                )
                
                with self._lock:
                    self._task_status[task_name] = status_str
                
                logger.info(f"Task {task_name} completed: {result.get('message')}")
                
            except Exception as e:
                logger.error(f"Error running task {task_name}: {e}", exc_info=True)
                with self._lock:
                    self._task_status[task_name] = "failed"
        
        # Start task in background thread
        threading.Thread(target=run_task_async, daemon=True).start()
        
        # Return immediately
        return {
            "success": True,
            "message": f"Task {task_name} triggered successfully, running in background",
            "status": "running",
            "task_name": task_name,
        }

    def _last_run_time_for_task(self, task_name: str) -> Optional[str]:
        """Last run time: from DB (persistent) first, else in-memory task. ISO string or None."""
        db_time = self.maintenance_log.get_last_run_time_iso(task_name)
        if db_time:
            return db_time
        task = self._tasks.get(task_name)
        if task and task.last_run_time:
            return task.last_run_time.isoformat()
        return None

    def _status_for_task(self, task_name: str) -> str:
        """Resolve status from DB first (running/completed/failed), else in-memory, so refresh shows correct state."""
        last_run = self.maintenance_log.get_last_run(task_name)
        if last_run:
            s = last_run.get("status")
            if s == "running":
                return "running"
            if s in ("completed", "failed"):
                return s
        with self._lock:
            return self._task_status.get(task_name, "idle")
    
    def get_task_status(self, task_name: str) -> Dict[str, Any]:
        """
        Get task status
        
        Args:
            task_name: Task name
        
        Returns:
            Status dict
        """
        if task_name not in self._tasks:
            return {"status": "not_found"}
        
        task = self._tasks[task_name]
        stats = self.maintenance_log.get_task_stats(task_name)
        
        # Status from DB when last run is running/completed/failed, so refresh shows correct state
        status = self._status_for_task(task_name)
        
        # Prefer last_run_time from DB (persistent); fallback to in-memory task
        last_run_time = self._last_run_time_for_task(task_name)
        
        return {
            "name": task_name,
            "status": status,
            "last_run_time": last_run_time,
            "last_result": task.last_result,
            "stats": stats,
        }
    
    def get_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks"""
        result = []
        for name, task in self._tasks.items():
            # Find task config to get schedule info
            task_config = None
            for config in self.tasks_config:
                if config.get("name") == name:
                    task_config = config
                    break
            
            schedule_info = None
            if task_config:
                trigger = task_config.get("trigger", "cron")
                if trigger == "cron":
                    cron_expr = task_config.get("cron")
                    if cron_expr:
                        # Format cron expression to readable schedule
                        # cron format: "分 时 日 月 星期"
                        parts = cron_expr.split()
                        if len(parts) >= 2:
                            minute = parts[0]
                            hour = parts[1]
                            day_of_week = parts[4] if len(parts) > 4 else "*"
                            
                            # Convert day of week range (e.g., "1-5" to "Mon-Fri")
                            day_map = {"0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed", 
                                      "4": "Thu", "5": "Fri", "6": "Sat"}
                            if "-" in day_of_week:
                                start, end = day_of_week.split("-")
                                day_range = f"{day_map.get(start, start)}-{day_map.get(end, end)}"
                            elif day_of_week == "*":
                                day_range = "Every day"
                            elif "," in day_of_week:
                                # Handle comma-separated days (e.g., "1,3,5")
                                days = [day_map.get(d.strip(), d.strip()) for d in day_of_week.split(",")]
                                day_range = ", ".join(days)
                            else:
                                day_range = day_map.get(day_of_week, day_of_week)
                            
                            # Format time (only if hour and minute are digits)
                            if hour.isdigit() and minute.isdigit():
                                hour_int = int(hour)
                                minute_int = int(minute)
                                hour_str = hour.zfill(2)
                                minute_str = minute.zfill(2)
                                
                                # Calculate HKT time (UTC+8)
                                hkt_hour = (hour_int + 8) % 24
                                hkt_hour_str = str(hkt_hour).zfill(2)
                                
                                schedule_info = f"{hour_str}:{minute_str} UTC / {hkt_hour_str}:{minute_str} HKT ({day_range})"
                            else:
                                # Fallback to raw cron expression if format is complex
                                schedule_info = f"Cron: {cron_expr}"
                elif trigger == "interval":
                    interval = task_config.get("interval")
                    if interval:
                        schedule_info = f"Every {interval}"
            
            # Notes from task config (e.g. "US market close + 3h (yfinance delay)")
            notes_raw = task_config.get("notes", []) if task_config else []
            notes = " / ".join(notes_raw) if isinstance(notes_raw, list) else (notes_raw or "")

            # Status from DB when last run is running/completed/failed, so refresh shows correct state
            status = self._status_for_task(name)
            # Prefer last_run_time from DB (persistent); fallback to in-memory task
            last_run_time = self._last_run_time_for_task(name)
            result.append({
                "name": name,
                "status": status,
                "last_run_time": last_run_time,
                "schedule": schedule_info,
                "notes": notes,
            })
        return result
