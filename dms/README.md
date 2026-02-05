# DMS - Data Maintenance Service

Standalone data maintenance service for fetching, maintaining, syncing, and backing up historical market data.

> Independent project; does not depend on `zuilow`. Can be deployed and run on its own.

Sub-project of **ZuiLow** all-in-one trading platform.

---

## Core Features

| Feature | Description |
|---------|-------------|
| Data maintenance | Fetch historical data from external sources (YFinance, Futu, etc.) |
| Efficient sync | Incremental sync to backup nodes; only new data is synced |
| Master-slave | Master-slave mode; slave nodes act as full backups |
| Backtest-friendly | Efficient batch read, LRU cache, parallel queries |
| Task scheduling | Cron and Interval triggers; automatic maintenance tasks |
| Web UI | View node status and control tasks |

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
# Edit config/dms.yaml for database, data sources, tasks, etc.
# Config files: config/dms.yaml, config/task.yaml, config/sync.yaml

# 3. Start service
./start_dms.sh      # Linux/Mac
.\start_dms.ps1     # Windows
# Or run directly: python app.py
```

**Access**: http://localhost:11183

---

## Documentation

| Document | Description |
|----------|-------------|
| [Doc index](doc/README.md) | Index of all documentation |
| [Architecture](doc/architecture.md) | System architecture overview |
| [Data quality](doc/data_quality.md) | Auto-repair and data quality |
| [API reference](doc/api_reference.md) | HTTP API documentation |
| [Config](config/) | Config files (dms.yaml, task.yaml, sync.yaml) |
| [Sync strategy](doc/sync_strategy.md) | Sync design and strategy |
| [Master-slave](doc/master_slave.md) | Master-slave architecture |

---

## Tech Stack

Flask / InfluxDB / yfinance / SQLite

### License

No License
