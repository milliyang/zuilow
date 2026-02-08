

# 用最简短的话介绍系统

  ZuiLow：做最低频、最慢、稳定的自动交易平台。

### 主要原因

```
┌─────────────────────────────────────────────────────────────┐
│                    交易延迟级别金字塔                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│                        ▲                                    │
│                       /\                                    │
│                      /  \     FPGA + Colocation             │
│                     / <1μs\    (专业HFT机构)                 │
│                    /______\                                 │
│                   /        \   Colocation + Direct Feed     │
│                  / 10-50μs  \  (量化基金)                    │
│                 /____________\                              │
│                /              \  DMA / Sponsored Access     │
│               /  100μs - 1ms   \ (机构交易)                  │
│              /__________________\                           │
│             /                    \ 专业券商 API              │
│            /     1ms - 50ms       \ (IB/Alpaca)             │
│           /________________________\                        │
│          /                          \ 零售券商               │
│         /       50ms - 500ms         \ (富途/老虎)           │
│        /______________________________\                     │
│       /                                \   ← ZuiLow 定位    │
│      /     !!! 我们在这里,不要挣扎 !!!    \                   │
│     /         (低频·慢·稳定·自动)          \                  │
│    /______________________________________\                 │
│                                                             │
│  延迟越低 → 成本越高 → 策略要求越高                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 项目介绍

Sai 由五块组成：

- **DMS** — 历史数据维护与同步
- **PPT** — 模拟交易与 Webhook
- **stime** — 仿真时间服务
- **ZuiLow** — 策略调度与多券商执行
- **simulate** — 全栈仿真编排（stime + ZuiLow + PPT + DMS）

### 对券商想说

- **富途牛牛** — 差劲！！！没有子账户，美股也没有仿真账户。
- **盈透 IBKR** — 也查经！！！无纯终端Gateway。

本项目：从回测、仿真到实盘，一条龙搞定！！！

### 使用界面

- **DMS**  
  ![DMS](doc/img/dms.jpg)

- **PPT**  
  ![PPT](doc/img/ppt.jpg)

- **stime / simulate**  
  ![仿真时间](doc/img/time_service_simulate.jpg)

- **ZuiLow**（账户、券商、任务、实盘、状态）  
  ![ZuiLow 账户](doc/img/zuilow.account.jpg)  
  ![ZuiLow 券商](doc/img/zuilow.broker.jpg)  
  ![ZuiLow 任务](doc/img/zuilow.jobs.jpg)  
  ![ZuiLow 实盘](doc/img/zuilow.live.jpg)  
  ![ZuiLow 状态](doc/img/zuilow.status.jpg)

### Docker 快速体验

**环境要求：** 已安装 Docker 与 Docker Compose。DMS 为可选（外部服务）；若需对接 DMS，请在 `docker/.env` 中设置 `DMS_BASE_URL`。

**首次运行**（在仓库根目录下）：

```bash
cd simlulate
./deploy_sim.sh up
```

将构建并启动 **stime**、**ZuiLow**、**PPT**。启动后可访问：

| 服务 | 默认地址 |
|------|----------|
| stime（设置/推进时间） | http://localhost:11185 |
| ZuiLow | http://localhost:11180 |
| PPT | http://localhost:11182 |

### 各子项目独立运行

在仓库根目录下，各组件可单独启动（需在各项目内安装依赖并配置，详见各项目 README）。

| 项目 | 命令（进入项目目录后执行） | 默认地址 |
|------|----------------------------|----------|
| **DMS** | `cd dms && ./start_dms.sh` | http://localhost:11183 |
| **stime** | `cd stime && python app.py` | http://localhost:11185 |
| **PPT** | `cd ppt && ./start_ppt.sh` | http://localhost:11182 |
| **ZuiLow** | `cd zuilow && ./start_zuilow.sh` | http://localhost:11180 |

也可在各项目目录下直接执行 `python app.py`（需先 `pip install -r requirements.txt` 并配置环境）。DMS、PPT 可能需要配置 `DMS_BASE_URL` 或数据库；ZuiLow 可能需要配置数据源与券商。**simulate** 整栈（见上方）通过 Docker 一次性运行 stime + ZuiLow + PPT。
