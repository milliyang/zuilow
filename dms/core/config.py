"""
DMS configuration: load and validate config from YAML (dms.yaml, task.yaml, sync.yaml) and symbol directories.

Used for: app and DMS constructor; load_config() once at startup, get_config() for singleton.

Classes:
    ServiceConfig      Service (host, port, log_level, log_dir, run_on_startup)
    FetcherConfig      Fetcher (enabled, rate_limit, retry_times, initial_days)
    PrimaryDBConfig    Primary DB (type, host, port, database, username, password, servers)
    ReaderConfig       Reader (cache_enabled, cache_size, batch_size, max_workers)
    BackupConfig       Backup node (name, type, host, port, database, enabled)
    SyncConfig         Sync (default_mode, retry_*, performance)
    MasterSlaveConfig  Master-slave (role, master_host, master_port, slaves, debug_mode)
    DMSConfig          Full DMS config (aggregates above)

Functions:
    load_config(config_path: Optional[str] = None) -> DMSConfig  Load config from YAML
    get_config() -> DMSConfig                                    Get global config singleton

DMSConfig features:
    - dms.yaml: service, fetchers, primary, reader, backups, sync, master_slave
    - task.yaml: tasks (name, type, symbols_file/symbols_dir, trigger)
    - sync.yaml: sync (default_mode, retry_*, performance)
    - symbols_dir: all *.yaml under directory, each with symbols list
"""

import yaml
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ServiceConfig:
    """Service configuration"""
    host: str = "0.0.0.0"
    port: int = 11183
    log_level: str = "INFO"
    log_dir: str = "run/logs"
    run_on_startup: bool = False  # Run all incremental tasks once on startup (for initial data fetch)


@dataclass
class FetcherConfig:
    """Fetcher configuration"""
    enabled: bool = True
    rate_limit: float = 0.5
    retry_times: int = 3
    cache_enabled: bool = False
    host: Optional[str] = None
    port: Optional[int] = None
    initial_days: int = 1825  # Default history range (5 years)


@dataclass
class PrimaryDBConfig:
    """Primary database configuration"""
    type: str = "influxdb1"
    host: str = "localhost"
    port: int = 8086
    database: str = "stock_data"
    username: str = ""
    password: str = ""
    servers: list[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReaderConfig:
    """Reader configuration"""
    cache_enabled: bool = True
    cache_size: int = 1000
    cache_ttl: int = 3600
    batch_size: int = 100
    parallel_read: bool = True
    max_workers: int = 5


@dataclass
class BackupConfig:
    """Backup node configuration"""
    name: str = ""
    type: str = "influxdb1"
    host: str = ""
    port: int = 8086
    database: str = "stock_data"
    username: str = ""
    password: str = ""
    enabled: bool = True


@dataclass
class SyncConfig:
    """Sync configuration"""
    default_mode: str = "incremental"
    retry_times: int = 3
    retry_delay: int = 5
    retry_backoff: str = "exponential"
    initial_days: int = 1825  # Default first sync history range (5 years)
    
    # Performance
    max_workers: int = 5
    connection_pool_size: int = 5
    enable_compression: bool = False
    compression_threshold: int = 100000


@dataclass
class MasterSlaveConfig:
    """Master-Slave configuration"""
    role: str = "master"
    master_host: Optional[str] = None
    master_port: int = 11183
    master_enabled: bool = False
    slaves: list[Dict[str, Any]] = field(default_factory=list)
    debug_mode: bool = False  # Debug mode: only use first 10 symbols for testing


@dataclass
class DMSConfig:
    """DMS complete configuration"""
    service: ServiceConfig
    fetchers: Dict[str, FetcherConfig]
    primary: PrimaryDBConfig
    reader: ReaderConfig
    backups: list[BackupConfig]
    tasks: list[Dict[str, Any]]
    sync: SyncConfig
    master_slave: MasterSlaveConfig


def load_config(config_path: Optional[str] = None) -> DMSConfig:
    """
    Load DMS configuration from YAML files
    
    Args:
        config_path: Path to user config file. If None, use default path.
    
    Returns:
        DMSConfig object
    """
    # Get config directory
    dms_dir = Path(__file__).parent.parent
    config_dir = dms_dir / "config"
    
    # Load dms.yaml (user config)
    if config_path is None:
        config_path = config_dir / "dms.yaml"
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    logger.info(f"Loading configuration from: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}
    
    dms_data = data.get("dms", {})
    
    # Read debug_mode early to apply it when loading symbols
    debug_mode = dms_data.get("debug_mode", False)
    debug_symbol_limit = dms_data.get("debug_symbol_limit", 10)  # Default: 10 symbols per file
    
    # Service config
    service_data = dms_data.get("service", {})
    service = ServiceConfig(
        host=service_data.get("host", "0.0.0.0"),
        port=service_data.get("port", 11183),
        log_level=service_data.get("log_level", "INFO"),
        log_dir=service_data.get("log_dir", "run/logs"),
        run_on_startup=service_data.get("run_on_startup", False),
    )
    
    # Fetchers config
    fetchers_data = dms_data.get("fetchers", {})
    fetchers = {}
    for name, config in fetchers_data.items():
        fetchers[name] = FetcherConfig(
            enabled=config.get("enabled", True),
            rate_limit=config.get("rate_limit", 0.5),
            retry_times=config.get("retry_times", 3),
            cache_enabled=config.get("cache_enabled", False),
            host=config.get("host"),
            port=config.get("port"),
            initial_days=config.get("initial_days", 1825),
        )
    
    # Primary DB config
    primary_data = dms_data.get("primary", {})
    primary = PrimaryDBConfig(
        type=primary_data.get("type", "influxdb1"),
        host=primary_data.get("host", "localhost"),
        port=primary_data.get("port", 8086),
        database=primary_data.get("database", "stock_data"),
        username=primary_data.get("username", ""),
        password=primary_data.get("password", ""),
        servers=primary_data.get("servers", []),
    )
    
    # Reader config
    reader_data = dms_data.get("reader", {})
    reader = ReaderConfig(
        cache_enabled=reader_data.get("cache_enabled", True),
        cache_size=reader_data.get("cache_size", 1000),
        cache_ttl=reader_data.get("cache_ttl", 3600),
        batch_size=reader_data.get("batch_size", 100),
        parallel_read=reader_data.get("parallel_read", True),
        max_workers=reader_data.get("max_workers", 5),
    )
    
    # Backups config
    backups_data = dms_data.get("backups", [])
    backups = []
    for backup_data in backups_data:
        backups.append(BackupConfig(
            name=backup_data.get("name", ""),
            type=backup_data.get("type", "influxdb1"),
            host=backup_data.get("host", ""),
            port=backup_data.get("port", 8086),
            database=backup_data.get("database", "stock_data"),
            username=backup_data.get("username", ""),
            password=backup_data.get("password", ""),
            enabled=backup_data.get("enabled", True),
        ))
    
    # Tasks config - load from task.yaml
    task_path = config_dir / "task.yaml"
    tasks = []
    if task_path.exists():
        logger.info(f"Loading tasks configuration from: {task_path}")
        try:
            with open(task_path, 'r', encoding='utf-8') as f:
                task_data = yaml.safe_load(f) or {}
                tasks = task_data.get("tasks", [])
        except Exception as e:
            logger.warning(f"Failed to load tasks from {task_path}: {e}")
    else:
        # Fallback to dms.yaml if task.yaml doesn't exist
        tasks = dms_data.get("tasks", [])
        if tasks:
            logger.info("Loading tasks from dms.yaml (task.yaml not found)")
    
    # Load symbols from files or directories if specified
    for task in tasks:
        symbols_file = task.get("symbols_file")
        symbols_dir = task.get("symbols_dir")
        
        symbols = []
        
        # Load from directory (scan all YAML files in the directory)
        if symbols_dir:
            symbols_dir_path = config_dir / symbols_dir
            if symbols_dir_path.exists() and symbols_dir_path.is_dir():
                try:
                    # Scan all YAML files in the directory
                    yaml_files = sorted(symbols_dir_path.glob("*.yaml"))
                    for yaml_file in yaml_files:
                        try:
                            with open(yaml_file, 'r', encoding='utf-8') as f:
                                symbols_data = yaml.safe_load(f) or {}
                                file_symbols = symbols_data.get("symbols", [])
                                if file_symbols:
                                    # Apply debug_mode limit per file
                                    if debug_mode and len(file_symbols) > debug_symbol_limit:
                                        original_count = len(file_symbols)
                                        file_symbols = file_symbols[:debug_symbol_limit]
                                        logger.debug(f"Debug mode: Limited {yaml_file.name} from {original_count} to {len(file_symbols)} symbols")
                                    symbols.extend(file_symbols)
                                    logger.debug(f"Loaded {len(file_symbols)} symbols from {yaml_file.name}")
                        except Exception as e:
                            logger.warning(f"Failed to load symbols from {yaml_file.name}: {e}")
                    # Remove duplicates while preserving order
                    if symbols:
                        seen = set()
                        unique_symbols = []
                        for symbol in symbols:
                            if symbol not in seen:
                                seen.add(symbol)
                                unique_symbols.append(symbol)
                        task["symbols"] = unique_symbols
                        logger.info(f"Loaded {len(unique_symbols)} unique symbols from {len(yaml_files)} files in {symbols_dir} for task {task.get('name')}")
                except Exception as e:
                    logger.warning(f"Failed to scan symbols directory {symbols_dir}: {e}")
            else:
                logger.warning(f"Symbols directory not found: {symbols_dir_path}")
        
        # Load from single file or multiple files (backward compatibility)
        elif symbols_file:
            # Support both single file (string) and multiple files (list)
            files_to_load = []
            if isinstance(symbols_file, str):
                files_to_load = [symbols_file]
            elif isinstance(symbols_file, list):
                files_to_load = symbols_file
            else:
                logger.warning(f"Invalid symbols_file format for task {task.get('name')}: {symbols_file}")
                continue
            
            # Load symbols from each file
            for file_path in files_to_load:
                symbols_path = config_dir / file_path
                if symbols_path.exists():
                    if symbols_path.is_dir():
                        # If it's a directory, scan it
                        try:
                            yaml_files = sorted(symbols_path.glob("*.yaml"))
                            for yaml_file in yaml_files:
                                try:
                                    with open(yaml_file, 'r', encoding='utf-8') as f:
                                        symbols_data = yaml.safe_load(f) or {}
                                        file_symbols = symbols_data.get("symbols", [])
                                        if file_symbols:
                                            # Apply debug_mode limit per file
                                            if debug_mode and len(file_symbols) > debug_symbol_limit:
                                                original_count = len(file_symbols)
                                                file_symbols = file_symbols[:debug_symbol_limit]
                                                logger.debug(f"Debug mode: Limited {yaml_file.name} from {original_count} to {len(file_symbols)} symbols")
                                            symbols.extend(file_symbols)
                                            logger.debug(f"Loaded {len(file_symbols)} symbols from {yaml_file.name}")
                                except Exception as e:
                                    logger.warning(f"Failed to load symbols from {yaml_file.name}: {e}")
                        except Exception as e:
                            logger.warning(f"Failed to scan symbols directory {file_path}: {e}")
                    else:
                        # Single file
                        try:
                            with open(symbols_path, 'r', encoding='utf-8') as f:
                                symbols_data = yaml.safe_load(f) or {}
                                file_symbols = symbols_data.get("symbols", [])
                                if file_symbols:
                                    # Apply debug_mode limit per file
                                    if debug_mode and len(file_symbols) > debug_symbol_limit:
                                        original_count = len(file_symbols)
                                        file_symbols = file_symbols[:debug_symbol_limit]
                                        logger.debug(f"Debug mode: Limited {file_path} from {original_count} to {len(file_symbols)} symbols")
                                    symbols.extend(file_symbols)
                                    logger.debug(f"Loaded {len(file_symbols)} symbols from {file_path}")
                        except Exception as e:
                            logger.warning(f"Failed to load symbols from {file_path}: {e}")
                else:
                    logger.warning(f"Symbols file not found: {symbols_path}")
            
            # Remove duplicates while preserving order
            if symbols:
                seen = set()
                unique_symbols = []
                for symbol in symbols:
                    if symbol not in seen:
                        seen.add(symbol)
                        unique_symbols.append(symbol)
                task["symbols"] = unique_symbols
                file_count = len(files_to_load)
                logger.info(f"Loaded {len(unique_symbols)} unique symbols from {file_count} file(s) for task {task.get('name')}")
    
    # Log debug_mode status (already applied during symbol loading)
    if debug_mode:
        logger.warning(f"⚠️ DEBUG MODE ENABLED: Limited to first {debug_symbol_limit} symbols per file")
    
    # Sync config - load from sync.yaml
    sync_path = config_dir / "sync.yaml"
    sync_data = {}
    if sync_path.exists():
        logger.info(f"Loading sync configuration from: {sync_path}")
        try:
            with open(sync_path, 'r', encoding='utf-8') as f:
                sync_file_data = yaml.safe_load(f) or {}
                sync_data = sync_file_data.get("sync", {})
        except Exception as e:
            logger.warning(f"Failed to load sync from {sync_path}: {e}")
    
    # Fallback to dms.yaml if sync.yaml doesn't exist or is empty
    if not sync_data:
        sync_data = dms_data.get("sync", {})
        if sync_data:
            logger.info("Loading sync from dms.yaml (sync.yaml not found or empty)")
    
    performance_data = sync_data.get("performance", {})
    incremental_data = sync_data.get("incremental", {})
    sync = SyncConfig(
        default_mode=sync_data.get("default_mode", "incremental"),
        retry_times=sync_data.get("retry_times", 3),
        retry_delay=sync_data.get("retry_delay", 5),
        retry_backoff=sync_data.get("retry_backoff", "exponential"),
        initial_days=incremental_data.get("initial_days", 1825),
        max_workers=performance_data.get("max_workers", 5),
        connection_pool_size=performance_data.get("connection_pool_size", 5),
        enable_compression=performance_data.get("enable_compression", False),
        compression_threshold=performance_data.get("compression_threshold", 100000),
    )
    
    # Master-Slave config
    role = dms_data.get("role", "master")
    master_data = dms_data.get("master", {})
    slaves_data = dms_data.get("slaves", [])
    
    # Convert slaves to list of dicts
    slaves_list = []
    for slave in slaves_data:
        if isinstance(slave, dict):
            slaves_list.append(slave)
        else:
            slaves_list.append(slave.__dict__ if hasattr(slave, "__dict__") else slave)
    
    master_slave = MasterSlaveConfig(
        role=role,
        master_host=master_data.get("host") if master_data else None,
        master_port=master_data.get("port", 11183) if master_data else 11183,
        master_enabled=master_data.get("enabled", False) if master_data else False,
        slaves=slaves_list,
        debug_mode=debug_mode,
    )
    
    return DMSConfig(
        service=service,
        fetchers=fetchers,
        primary=primary,
        reader=reader,
        backups=backups,
        tasks=tasks,
        sync=sync,
        master_slave=master_slave,
    )


def get_config() -> DMSConfig:
    """Get global configuration instance"""
    global _config
    if _config is None:
        _config = load_config()
    return _config


# Global config instance
_config: Optional[DMSConfig] = None
