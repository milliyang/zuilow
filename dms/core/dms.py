"""
DMS main class: integrates config, fetcher, writer, reader, sync, scheduler, master-slave; unified entry and HTTP API.

Used for: app startup creates DMS(config), dms.start(); web API and zuilow call into this instance.

Classes:
    DMS  Data maintenance service main class

Functions:
    setup_logging(log_dir, log_level)  Configure logging (file + console, rotating)

DMS methods:
    .start() -> None                                   Start service (scheduler + optional run-once incremental)
    .stop() -> None                                    Stop service
    .is_running() -> bool                              Whether running
    .get_uptime() -> int                               Uptime in seconds
    .get_all_symbols() -> List[str]                    All symbols from tasks
    .get_tasks() -> List[Dict]                         Task list
    .get_task_status(task_name) -> Dict                Single task status
    .trigger_task(task_name) -> Dict                   Manually trigger one task
    .trigger_all_tasks(task_type=None) -> Dict         Manually trigger all (optional filter by type)
    .read_history(symbol, start, end, interval) -> DataFrame  Read history
    .read_batch(symbols, start, end, interval) -> Dict          Batch read
    .get_sync_status() -> Dict                         Sync status
    .trigger_sync(backup_name=None) -> Dict            Trigger sync (optional backup name)
    .get_slaves() / .get_slave_status(name)             Slave list / status
    .sync_to_slave(slave_name, ...)                    Sync to given slave
    .get_master_status() / .request_sync_from_master() Master status / request sync from master (slave)
    .get_maintenance_log(...)                          Maintenance log
    .clear_database() -> Dict                          Clear primary DB (dangerous)

DMS features:
    - Config from YAML; fetcher/writer/reader/sync/scheduler/master_slave created from config
    - Scheduler runs incremental/full_sync/validation by cron or interval
"""

import logging
import logging.handlers
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import requests

from .config import load_config, DMSConfig
from .fetcher import DataFetcher
from .writer import DataWriter
from .reader import DataReader
from .sync_manager import SyncManager
from .scheduler import MaintenanceScheduler
from .master_slave import MasterSlaveManager
from .exporter import DataExporter

logger = logging.getLogger(__name__)


def setup_logging(log_dir: str = "run/logs", log_level: str = "INFO"):
    """
    Setup logging configuration with file and console handlers
    
    Args:
        log_dir: Log directory path
        log_level: Log level (DEBUG, INFO, WARNING, ERROR)
    """
    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Log format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, datefmt=date_format)
    
    # Console handler (always enabled)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (rotating, max 10MB per file, keep 5 backups)
    log_file = log_path / "dms.log"
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    logger.info(f"Logging configured: level={log_level}, file={log_file}")


class DMS:
    """
    Data Maintenance Service
    
    Main class that integrates all components.
    """
    
    def __init__(self, config: Optional[DMSConfig] = None, config_path: Optional[str] = None):
        """
        Initialize DMS
        
        Args:
            config: DMSConfig object (if None, load from file)
            config_path: Path to config file
        """
        if config is None:
            config = load_config(config_path)
        
        self.config = config
        self.role = config.master_slave.role
        self._start_time = time.time()
        self._running = False
        
        # Setup logging with file handler
        setup_logging(
            log_dir=config.service.log_dir,
            log_level=config.service.log_level
        )
        
        # Initialize components
        self.scheduler: Optional[MaintenanceScheduler] = None
        # Convert fetchers config to dict
        fetchers_dict = {name: {
            "enabled": f.enabled,
            "rate_limit": f.rate_limit,
            "retry_times": f.retry_times,
            "cache_enabled": f.cache_enabled,
            "host": f.host,
            "port": f.port,
        } for name, f in config.fetchers.items()}
        
        self.fetcher = DataFetcher(fetchers_dict)
        
        # Writer config
        writer_config = {
            "host": config.primary.host,
            "port": config.primary.port,
            "database": config.primary.database,
            "username": config.primary.username,
            "password": config.primary.password,
            "servers": config.primary.servers,
            "auto_connect": True,
        }
        self.writer = DataWriter(writer_config)
        
        # Reader config
        reader_config = {
            "cache_enabled": config.reader.cache_enabled,
            "cache_size": config.reader.cache_size,
            "cache_ttl": config.reader.cache_ttl,
            "batch_size": config.reader.batch_size,
            "parallel_read": config.reader.parallel_read,
            "max_workers": config.reader.max_workers,
        }
        db_config = {
            "type": "influxdb1",
            "host": config.primary.host,
            "port": config.primary.port,
            "database": config.primary.database,
            "username": config.primary.username,
            "password": config.primary.password,
        }
        self.reader = DataReader(reader_config, db_config)
        
        # Data exporter
        self.exporter = DataExporter(self.reader, export_dir="run/exports")
        
        # Sync manager (only if backups exist)
        if config.backups:
            sync_config_dict = {
                "retry_times": config.sync.retry_times,
                "retry_delay": config.sync.retry_delay,
                "retry_backoff": config.sync.retry_backoff,
                "performance": {
                    "max_workers": config.sync.max_workers,
                    "connection_pool_size": config.sync.connection_pool_size,
                    "enable_compression": config.sync.enable_compression,
                    "compression_threshold": config.sync.compression_threshold,
                },
            }
            self.sync_manager = SyncManager(
                primary_writer=self.writer,
                primary_reader=self.reader,
                backups=[{
                    "name": b.name,
                    "type": b.type,
                    "host": b.host,
                    "port": b.port,
                    "database": b.database,
                    "username": b.username,
                    "password": b.password,
                    "enabled": b.enabled,
                } for b in config.backups],
                sync_config=sync_config_dict,
            )
        else:
            self.sync_manager = None
        
        # Initialize scheduler (only for master node)
        if self.role == "master" and config.tasks:
            self.scheduler = MaintenanceScheduler(
                tasks_config=config.tasks,
                fetcher=self.fetcher,
                writer=self.writer,
                reader=self.reader,
                sync_manager=self.sync_manager,
            )
        
        # Initialize master-slave manager
        master_config = {
            "host": config.master_slave.master_host,
            "port": config.master_slave.master_port,
            "enabled": config.master_slave.master_enabled,
        } if config.master_slave.master_host else {}
        
        slaves_list = config.master_slave.slaves
        self.master_slave = MasterSlaveManager(
            role=self.role,
            master_config=master_config,
            slaves_config=slaves_list,
        )
        
        # Check database health before starting
        self._check_database_health()
        
        # Cache for get_all_symbols (API /symbols): (list, timestamp)
        self._symbols_cache: Optional[List[str]] = None
        self._symbols_cache_ts: float = 0.0
        self._symbols_cache_ttl: int = int(os.getenv("DMS_SYMBOLS_CACHE_TTL", "300"))  # seconds
        
        logger.info(f"DMS initialized (role: {self.role})")
    
    def get_all_symbols(self) -> List[str]:
        """
        Get all symbols from all tasks
        
        Returns:
            List of unique symbols from all configured tasks
        """
        all_symbols = []
        seen = set()
        
        if self.config.tasks:
            for task in self.config.tasks:
                task_symbols = task.get("symbols", [])
                for symbol in task_symbols:
                    if symbol not in seen:
                        seen.add(symbol)
                        all_symbols.append(symbol)
        
        return all_symbols
    
    def get_all_symbols_cached(self, ttl_seconds: Optional[int] = None) -> List[str]:
        """
        Get all symbols with in-memory cache for fast repeated calls (e.g. GET /api/dms/symbols).
        Uses DMS_SYMBOLS_CACHE_TTL env (default 300s); pass ttl_seconds to override.
        """
        ttl = ttl_seconds if ttl_seconds is not None else self._symbols_cache_ttl
        now = time.time()
        if self._symbols_cache is not None and (now - self._symbols_cache_ts) < ttl:
            return self._symbols_cache
        self._symbols_cache = self.get_all_symbols()
        self._symbols_cache_ts = now
        return self._symbols_cache
    
    @property
    def is_running(self) -> bool:
        """Whether DMS is running"""
        return self._running
    
    def _check_database_health(self):
        """Check database connection and health status"""
        logger.info("Database Health Check")
        
        # Check primary database
        db_config = self.config.primary
        logger.info(f"Primary Database: {db_config.type} @ {db_config.host}:{db_config.port}/{db_config.database}")
        
        # Test writer connection
        writer_ok = False
        try:
            if self.writer and hasattr(self.writer, 'writer') and self.writer.writer:
                writer = self.writer.writer
                if hasattr(writer, '_connected'):
                    if writer._connected:
                        if hasattr(writer, 'clients') and writer.clients:
                            logger.info(f"✅ Writer: Connected ({len(writer.clients)} server(s))")
                            writer_ok = True
                        else:
                            logger.warning("⚠️ Writer: No servers available")
                    else:
                        if writer.connect():
                            if hasattr(writer, 'clients') and writer.clients:
                                logger.info(f"✅ Writer: Connected ({len(writer.clients)} server(s))")
                            writer_ok = True
                        else:
                            logger.error("❌ Writer: Connection failed")
                else:
                    logger.warning("⚠️ Writer: Connection status unknown")
            else:
                logger.warning("⚠️ Writer: Not initialized")
        except Exception as e:
            logger.error(f"❌ Writer: Connection error - {e}")
            logger.error(f"  Please check InfluxDB service on {db_config.host}:{db_config.port}")
            raise ConnectionError(f"Failed to connect to primary database: {e}")
        
        # Test reader connection
        reader_ok = False
        try:
            if self.reader and hasattr(self.reader, 'reader') and self.reader.reader:
                reader = self.reader.reader
                if hasattr(reader, '_connected'):
                    if reader._connected:
                        logger.info("✅ Reader: Connected")
                        reader_ok = True
                    else:
                        if reader.connect():
                            logger.info("✅ Reader: Connected")
                            reader_ok = True
                        else:
                            logger.error("❌ Reader: Connection failed")
                else:
                    logger.warning("⚠️ Reader: Connection status unknown")
            else:
                logger.warning("⚠️ Reader: Not initialized")
        except Exception as e:
            logger.error(f"❌ Reader: Connection error - {e}")
            raise ConnectionError(f"Failed to connect to primary database (reader): {e}")
        
        # Test database query (simple test)
        if writer_ok and self.writer and self.writer.writer:
            try:
                if hasattr(self.writer.writer, 'primary_client') and self.writer.writer.primary_client:
                    client = self.writer.writer.primary_client
                    try:
                        client.query("SHOW DATABASES")
                        logger.info("✅ Database Query: Test successful")
                    except Exception as e:
                        logger.warning(f"⚠️ Database Query: Test failed - {e}")
            except Exception as e:
                logger.warning(f"⚠️ Database Query: Could not test - {e}")
        
        # Check backup databases if configured
        if self.config.backups:
            for i, backup in enumerate(self.config.backups, 1):
                if backup.enabled:
                    logger.debug(f"Backup {i}: {backup.name} ({backup.host}:{backup.port}/{backup.database})")
                else:
                    logger.debug(f"Backup {i}: {backup.name} (disabled)")
        
        # Summary
        if writer_ok and reader_ok:
            logger.info("✅ Database health check: PASSED")
            logger.info(f"   Database '{db_config.database}' is ready for use")
        else:
            logger.warning("⚠️ Database health check: PARTIAL")
            if not writer_ok:
                logger.warning("   Writer connection failed")
            if not reader_ok:
                logger.warning("   Reader connection failed")
    
    def start(self):
        """Start DMS service"""
        self._running = True
        
        # Start scheduler if master node
        if self.role == "master" and self.scheduler:
            self.scheduler.start()
            
            # Run all incremental tasks on startup if configured
            if self.config.service.run_on_startup:
                logger.info("⚠️ RUN_ON_STARTUP enabled: Triggering all incremental tasks for initial data fetch")
                self._run_all_incremental_tasks_on_startup()
        
        logger.info("DMS service started")
    
    def _run_all_incremental_tasks_on_startup(self):
        """Run all incremental tasks once on startup (for initial data fetch)"""
        if not self.scheduler:
            return
        
        # Get all incremental tasks
        incremental_tasks = [
            task_name for task_name, task_config in zip(self.scheduler._tasks.keys(), self.config.tasks)
            if task_config.get("type") == "incremental"
        ]
        
        if not incremental_tasks:
            logger.info("No incremental tasks found, skipping startup run")
            return
        
        logger.info(f"Found {len(incremental_tasks)} incremental task(s), triggering on startup...")
        
        # Trigger each task in background (non-blocking)
        def trigger_task_async(task_name: str):
            try:
                logger.info(f"Triggering task '{task_name}' on startup...")
                result = self.scheduler.trigger_task(task_name)
                if result.get("success"):
                    logger.info(f"✅ Task '{task_name}' completed on startup: {result.get('message')}")
                else:
                    logger.warning(f"⚠️ Task '{task_name}' failed on startup: {result.get('message')}")
            except Exception as e:
                logger.error(f"❌ Error triggering task '{task_name}' on startup: {e}", exc_info=True)
        
        # Run all tasks in parallel (non-blocking)
        import threading
        for task_name in incremental_tasks:
            threading.Thread(target=trigger_task_async, args=(task_name,), daemon=True).start()
        
        logger.info(f"Started {len(incremental_tasks)} task(s) in background for initial data fetch")
    
    def stop(self):
        """Stop DMS service"""
        self._running = False
        
        # Stop scheduler
        if self.scheduler:
            self.scheduler.stop()
        
        if self.writer and self.writer.writer:
            self.writer.writer.disconnect()
        if self.reader and self.reader.reader:
            self.reader.reader.disconnect()
        logger.info("DMS service stopped")
    
    def get_uptime(self) -> int:
        """Get uptime in seconds"""
        return int(time.time() - self._start_time)
    
    def get_tasks(self) -> List[Dict[str, Any]]:
        """Get task list (master only)"""
        if self.role != "master":
            return []
        if self.scheduler:
            return self.scheduler.get_tasks()
        return []
    
    def get_all_nodes_status(self) -> Dict[str, Any]:
        """Get all nodes status"""
        nodes = []
        
        # Add current node
        nodes.append({
            "name": "local",
            "role": self.role,
            "host": self.config.service.host,
            "port": self.config.service.port,
            "status": "running" if self._running else "stopped",
            "uptime": self.get_uptime(),
            "tasks_count": len(self.config.tasks) if self.role == "master" else 0,
        })
        
        # Query other nodes
        if self.role == "master":
            # Query all slaves
            slaves = self.config.master_slave.slaves
            for slave in slaves:
                slave_dict = slave if isinstance(slave, dict) else slave.__dict__ if hasattr(slave, "__dict__") else {}
                if slave_dict.get("enabled", True):
                    try:
                        response = requests.get(
                            f"http://{slave_dict['host']}:{slave_dict['port']}/api/dms/status",
                            timeout=3
                        )
                        if response.status_code == 200:
                            slave_status = response.json()
                            nodes.append({
                                "name": slave_dict.get("name", slave_dict["host"]),
                                "role": "slave",
                                "host": slave_dict["host"],
                                "port": slave_dict["port"],
                                "status": "online" if slave_status.get("running") else "offline",
                                "uptime": slave_status.get("uptime", 0),
                            })
                        else:
                            nodes.append({
                                "name": slave_dict.get("name", slave_dict["host"]),
                                "role": "slave",
                                "host": slave_dict["host"],
                                "port": slave_dict["port"],
                                "status": "offline",
                            })
                    except Exception as e:
                        nodes.append({
                            "name": slave_dict.get("name", slave_dict["host"]),
                            "role": "slave",
                            "host": slave_dict["host"],
                            "port": slave_dict["port"],
                            "status": "offline",
                            "error": str(e),
                        })
        elif self.role == "slave":
            # Query master
            master = self.config.master_slave
            if master.master_enabled and master.master_host:
                try:
                    response = requests.get(
                        f"http://{master.master_host}:{master.master_port}/api/dms/status",
                        timeout=3
                    )
                    if response.status_code == 200:
                        master_status = response.json()
                        nodes.append({
                            "name": "master",
                            "role": "master",
                            "host": master.master_host,
                            "port": master.master_port,
                            "status": "online" if master_status.get("running") else "offline",
                            "uptime": master_status.get("uptime", 0),
                            "tasks_count": master_status.get("tasks_count", 0),
                        })
                    else:
                        nodes.append({
                            "name": "master",
                            "role": "master",
                            "host": master.master_host,
                            "port": master.master_port,
                            "status": "offline",
                        })
                except Exception as e:
                    nodes.append({
                        "name": "master",
                        "role": "master",
                        "host": master.master_host,
                        "port": master.master_port,
                        "status": "offline",
                        "error": str(e),
                    })
        
        return {
            "nodes": nodes,
            "total_nodes": len(nodes),
            "online_nodes": sum(1 for n in nodes if n["status"] in ["running", "online"]),
        }
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get sync status"""
        if not self.sync_manager:
            return {"backups": [], "total_backups": 0}
        
        backups_status = []
        for backup in self.config.backups:
            backup_name = backup.name
            last_sync = self.sync_manager.sync_history.get_last_sync_time(backup_name)
            backups_status.append({
                "name": backup_name,
                "host": backup.host,
                "port": backup.port,
                "last_sync": last_sync.isoformat() if last_sync else None,
            })
        
        return {
            "backups": backups_status,
            "total_backups": len(backups_status),
        }
    
    def get_sync_history(
        self,
        backup_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get sync history"""
        if not self.sync_manager:
            return []
        return self.sync_manager.sync_history.get_history(backup_name, limit, offset)
    
    def read_history(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ):
        """Read history data"""
        return self.reader.read_history(symbol, start_date, end_date, interval)
    
    def read_batch(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> Dict[str, Any]:
        """Batch read data"""
        return self.reader.read_batch(symbols, start_date, end_date, interval)
    
    # Master node methods
    def trigger_task(self, task_name: str):
        """Trigger a task (master only)"""
        if self.role != "master":
            raise ValueError("Only master node can trigger tasks")
        if not self.scheduler:
            raise ValueError("Scheduler not initialized")
        return self.scheduler.trigger_task(task_name)
    
    def trigger_all_tasks(self, task_type: Optional[str] = None):
        """
        Trigger all tasks (master only)
        
        Args:
            task_type: Optional task type filter (e.g., "incremental", "full_sync")
                      If None, trigger all tasks
        
        Returns:
            Dict with results for each task
        """
        if self.role != "master":
            raise ValueError("Only master node can trigger tasks")
        if not self.scheduler:
            raise ValueError("Scheduler not initialized")
        
        # Get tasks to trigger
        tasks_to_trigger = []
        for task_name, task_config in zip(self.scheduler._tasks.keys(), self.config.tasks):
            if task_type is None or task_config.get("type") == task_type:
                tasks_to_trigger.append(task_name)
        
        if not tasks_to_trigger:
            return {
                "success": True,
                "message": f"No tasks found (filter: {task_type or 'all'})",
                "triggered_count": 0,
                "results": {},
            }
        
        logger.info(f"Triggering {len(tasks_to_trigger)} task(s) (filter: {task_type or 'all'})...")
        
        results = {}
        for task_name in tasks_to_trigger:
            try:
                result = self.scheduler.trigger_task(task_name)
                results[task_name] = result
            except Exception as e:
                logger.error(f"Error triggering task {task_name}: {e}", exc_info=True)
                results[task_name] = {
                    "success": False,
                    "error": str(e),
                }
        
        success_count = sum(1 for r in results.values() if r.get("success", False))
        
        return {
            "success": success_count == len(tasks_to_trigger),
            "message": f"Triggered {len(tasks_to_trigger)} task(s), {success_count} succeeded",
            "triggered_count": len(tasks_to_trigger),
            "success_count": success_count,
            "results": results,
        }
    
    def get_task_status(self, task_name: str):
        """Get task status (master only)"""
        if self.role != "master":
            raise ValueError("Only master node can get task status")
        if not self.scheduler:
            raise ValueError("Scheduler not initialized")
        return self.scheduler.get_task_status(task_name)
    
    def trigger_sync(self, backup_name: Optional[str] = None):
        """Trigger sync (master only)"""
        if self.role != "master":
            raise ValueError("Only master node can trigger sync")
        if not self.sync_manager:
            raise ValueError("No backup nodes configured")
        
        if backup_name:
            result = self.sync_manager.sync_incremental(backup_name)
            return {"success": result, "backup_name": backup_name}
        else:
            result = self.sync_manager.sync_to_all_backups()
            return result
    
    def get_slaves(self) -> List[Dict[str, Any]]:
        """Get slaves list (master only)"""
        if self.role != "master":
            raise ValueError("Only master node can list slaves")
        slaves = self.config.master_slave.slaves
        return [s if isinstance(s, dict) else s.__dict__ for s in slaves if (s if isinstance(s, dict) else s.__dict__).get("enabled", True)]
    
    def get_slave_status(self, slave_name: str) -> Dict[str, Any]:
        """Get slave status (master only)"""
        if self.role != "master":
            raise ValueError("Only master node can get slave status")
        
        return self.master_slave.check_slave_status(slave_name)
    
    def sync_to_slave(
        self,
        slave_name: str,
        symbol: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        """Sync to specific slave (master only)"""
        if self.role != "master":
            raise ValueError("Only master node can sync to slaves")
        if not self.sync_manager:
            raise ValueError("No sync manager available")
        
        return self.sync_manager.sync_incremental(slave_name, symbol)
    
    def get_maintenance_log(
        self,
        task_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get maintenance log (master only)"""
        if self.role != "master":
            raise ValueError("Only master node can get maintenance log")
        if not self.scheduler:
            return []
        return self.scheduler.maintenance_log.get_logs(task_name, limit, offset)
    
    # Slave node methods
    def request_sync_from_master(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """Request sync from master (slave only)"""
        if self.role != "slave":
            raise ValueError("Only slave node can request sync")
        
        master = self.config.master_slave
        if not master.master_enabled or not master.master_host:
            raise ValueError("Master node not configured")
        
        # Get local node name (use hostname or IP)
        local_name = self._get_local_node_name()
        # Try to find matching slave name in master's config
        # For now, use a simple identifier
        slave_identifier = local_name.replace(":", "_")
        
        try:
            response = requests.post(
                f"http://{master.master_host}:{master.master_port}/api/dms/slaves/{slave_identifier}/sync",
                json={
                    "symbol": symbol,
                    "start_date": start_date,
                    "end_date": end_date,
                },
                timeout=30
            )
            if response.status_code == 200:
                return response.json()
            else:
                raise ValueError(f"Sync request failed: HTTP {response.status_code}")
        except Exception as e:
            raise ValueError(f"Failed to request sync: {e}")
    
    def get_master_status(self) -> Dict[str, Any]:
        """Get master status (slave only)"""
        if self.role != "slave":
            raise ValueError("Only slave node can get master status")
        
        master = self.config.master_slave
        if not master.master_enabled or not master.master_host:
            return {"status": "not_configured"}
        
        try:
            response = requests.get(
                f"http://{master.master_host}:{master.master_port}/api/dms/status",
                timeout=3
            )
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "offline", "error": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"status": "offline", "error": str(e)}
    
    def clear_database(self) -> Dict[str, Any]:
        """
        Clear all data from database
        
        WARNING: This is a destructive operation that will delete all data!
        
        Returns:
            Result dict with success status and message
        """
        if self.role != "master":
            raise ValueError("Only master node can clear database")
        
        logger.warning("⚠️ DATABASE CLEAR OPERATION REQUESTED - This will delete all data!")
        
        try:
            success = self.writer.clear_database()
            if success:
                message = "Database cleared successfully"
                logger.warning(f"⚠️ {message}")
                return {
                    "success": True,
                    "message": message,
                }
            else:
                message = "Failed to clear database"
                logger.error(f"❌ {message}")
                return {
                    "success": False,
                    "message": message,
                }
        except Exception as e:
            error_msg = f"Error clearing database: {e}"
            logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "message": error_msg,
            }
    
    def _get_local_node_name(self) -> str:
        """Get local node name"""
        # Try to get from config, or use default
        host = self.config.service.host
        if host == "0.0.0.0":
            import socket
            host = socket.gethostname()
        return f"{host}:{self.config.service.port}"
