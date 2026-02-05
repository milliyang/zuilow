# ZuiLow

策略信号与市场执行解耦的任务调度系统：定义任务（策略 + 账号 + 市场），在指定时间预执行策略产生交易信号，在对应市场开盘/到点时执行信号（直接单或调仓单）。

---

## 核心概念

- **任务**：由策略、账号、市场、通知等组成；每个任务可指定使用的 **Market** 与 **Account**。
- **策略预执行**：按 cron/interval/盘前/盘后等触发，运行策略生成**交易信号**（直接买卖 或 调仓比例），写入信号存储。
- **市场执行**：按策略指定的市场，在 **市场开盘 / open_bar / 指定时间** 触发执行器，从存储取出该市场的 pending 信号，下单并更新状态。
- **解耦原因**：策略可能计算量大、跨市场或依赖 GPU，提前算好信号，到点再执行，避免到盘才算导致延迟。

### 交易信号类型

- **直接买卖**：symbol、side、qty、（price）等。
- **调仓 / 换股**：目标权重或目标市值，执行器根据账户资产与持仓计算买卖单并下单。

---

## 工作流程

1. **预执行**（cron / interval / 盘前盘后等）→ 跑策略 → 产出 TradingSignal → 写入 SignalStore（或 `send_immediately` 直发）。
2. **市场执行**（market_open / open_bar / at_time）→ 执行器按 account + market 拉取 pending 信号 → 调用 `/api/order`（带 account）→ 直接单或调仓单执行 → 更新信号状态。

详见 [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md)。

---

## 网页管理

| 功能       | 说明 |
|------------|------|
| 系统状态   | 数据源、账号、Broker 连接状态 |
| 账户       | 多账户（Paper / Futu / IBKR），按 account 查资产与持仓 |
| Brokers    | Futu 连接、IBKR 连接、Paper Trade |
| 任务调度器 | 策略任务配置、trigger、account、market、send_immediately |
| 交易信号   | 信号列表与状态 |
| 策略       | 策略视角信息 |

---

## 配置

- **config/accounts.yaml**：账户抽象（name、type、futu_acc_id / ibkr_account_id / paper_account）。
- **config/brokers/**：Futu（futu.yaml）、IBKR（ibkr.yaml）。
- **config/scheduler.yaml**：调度任务、市场开盘时间与时区、jobs（strategy、account、market、trigger）。
- **config/strategies/**：各策略参数 YAML。
- **config/brokers/ppt.yaml**、**config/users.yaml**：PPT 地址/Token、用户等。

---

## 全栈模拟（回放/仿真）

使用项目内 **stime/**（Simulation Time Service）+ ZuiLow tick + PPT tick）做统一仿真时间与回放，详见 [stime/doc/ARCH.md](../stime/doc/ARCH.md)（若存在）或项目根目录下的仿真文档。

---

## 文档与启动

- 架构与数据流：[doc/ARCHITECTURE.md](doc/ARCHITECTURE.md)
- 回测/测试：[doc/TESTING.md](doc/TESTING.md)
- Futu / IBKR 配置：[doc/brokers/futu_setup.md](doc/brokers/futu_setup.md)、[doc/brokers/ibkr_setup.md](doc/brokers/ibkr_setup.md)
- 启动：`./start_zuilow.sh` 或 `python -m zuilow.web.app`（见 `env.example` 与 `requirements.txt`）
