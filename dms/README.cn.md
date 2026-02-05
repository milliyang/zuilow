# DMS - Data Maintenance Service

完全独立的数据维护服务，专门用于历史数据的获取、维护、同步和备份。

> 独立项目，不依赖 `zuilow`，可单独部署运行。

🚀 ***ZuiLow** 一站式交易平台子项目，敬请期待！*

---

## 核心功能

| 功能 | 说明 |
|------|------|
| 数据维护 | 从外部数据源（YFinance、Futu等）获取历史数据 |
| 高效同步 | 增量同步到备份节点，只同步新数据 |
| 主从架构 | 支持主从模式，从节点作为完全备份角色 |
| 回测优化 | 高效批量读取，LRU 缓存，并行查询 |
| 任务调度 | 支持 Cron 和 Interval 触发，自动执行维护任务 |
| WebUI   | 显示节点状态 |

---

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置
# 编辑 config/dms.yaml 配置数据库、数据源、任务等
# 配置文件：config/dms.yaml, config/task.yaml, config/sync.yaml

# 3. 启动服务
./start_dms.sh      # Linux/Mac
.\start_dms.ps1     # Windows
# 或直接运行: python app.py
```

**访问**: http://localhost:11183

---

## 文档

| 文档 | 说明 |
|------|------|
| [文档索引](doc/README.md) | 所有文档的索引 |
| [架构设计](doc/architecture.md) | 系统架构概览 |
| [首次运行](doc/first_run_guide.md) | **如何触发第一次更新所有股票** ⭐ |
| [数据质量](doc/data_quality.md) | 自动恢复、数据质量保证 |
| [数据导出](doc/export_guide.md) | 导出数据到 CSV/ZIP |
| [Debug Mode](doc/debug_mode_guide.md) | 调试模式快速测试 |
| [API 参考](doc/api_reference.md) | HTTP API 完整文档 |
| [配置文件](config/) | 配置文件（dms.yaml, task.yaml, sync.yaml） |
| [同步策略](doc/sync_strategy.md) | 同步方案详细设计 |
| [主从模式](doc/master_slave.md) | 主从架构设计 |

---

## 技术栈

Flask / InfluxDB / yfinance / SQLite

### Licence

No License
