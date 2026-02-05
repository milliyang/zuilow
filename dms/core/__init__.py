"""
DMS Core (Data Maintenance Service Core)

Core components for data maintenance: config loading, fetch/write/read, master-slave sync, and scheduled tasks.

Modules:
    config          Config: load and validate DMS config from YAML
    dms             DMS main class: integrates all components
    fetcher         Data fetcher manager: unified interface for multiple sources
    writer          Data writer manager: write to primary DB
    reader          Data reader manager: read from primary DB
    sync_manager    Sync manager: incremental/full sync from primary to backup nodes
    scheduler       Scheduler: Cron/Interval triggers for maintenance tasks
    master_slave    Master-slave: role detection and communication
    exporter        Exporter: export from DB to CSV/ZIP

Usage:
    from dms.core import load_config
    from dms.core import DMS

    config = load_config("config/dms.yaml")
    dms = DMS(config=config)
    dms.start()
"""
