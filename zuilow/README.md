# ZuiLow

A task scheduler that decouples strategy signals from market execution: define jobs (strategy + account + market), run strategies at scheduled times to produce trading signals, and execute those signals at market open or at configured times (single orders or rebalance orders).

---

## Core Concepts

- **Job**: Composed of strategy, account, market, notifications, etc.; each job specifies a **Market** and **Account**.
- **Pre-execution**: Triggered by cron, interval, pre/post market, etc.; runs the strategy to generate **trading signals** (single buy/sell or target weights), which are written to the signal store.
- **Market execution**: At **market open / open_bar / at_time** for the job’s market, the executor pulls pending signals for that account + market, calls `/api/order` (with account), executes single or rebalance orders, and updates signal status.
- **Why decouple**: Strategies can be heavy, cross-market, or GPU-bound; computing signals ahead of time and executing at the bell avoids latency from computing at market open.

### Signal Types

- **Single order**: symbol, side, qty, (price), etc.
- **Rebalance / rotation**: Target weights or target notional; the executor derives buy/sell orders from account equity and positions, then places orders.

---

## Workflow

1. **Pre-execution** (cron / interval / pre/post market, etc.) → run strategy → produce TradingSignal → write to SignalStore (or send via `send_immediately`).
2. **Market execution** (market_open / open_bar / at_time) → executor fetches pending signals by account + market → calls `/api/order` (with account) → executes single or rebalance orders → updates signal status.

See [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md) for details.

---

## Web UI

| Feature | Description |
|---------|-------------|
| System status | Data sources, accounts, broker connection status |
| Account | Multi-account (Paper / Futu / IBKR); view assets and positions by account |
| Brokers | Futu, IBKR, Paper Trade connection and config |
| Scheduler | Job config: strategy, trigger, account, market, send_immediately |
| Signals | Signal list and status |
| Strategies | Strategy-centric view |

---

## Configuration

- **config/accounts/** (paper.yaml, futu.yaml, ibkr.yaml): Account abstraction (name, type, futu_acc_id / ibkr_account_id / paper_account).
- **config/brokers/**: Futu (futu.yaml), IBKR (ibkr.yaml), PPT (ppt.yaml).
- **config/scheduler.yaml**: Jobs, market open times and timezones (strategy, account, market, trigger).
- **config/strategies/**: Per-strategy parameter YAML.
- **config/brokers/ppt.yaml**, **config/users.yaml**: PPT base_url/token, web users.

---

## Full-Stack Simulation (Replay)

Uses **stime/** (Simulation Time Service) + ZuiLow tick + PPT tick for unified sim time and replay. See [stime/doc/ARCH.md](../stime/doc/ARCH.md) (if present) or the repo root simulation docs.

---

## Docs and Run

- Architecture and data flow: [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md)
- Backtest and testing: [doc/TESTING.md](doc/TESTING.md)
- Futu / IBKR setup: [doc/brokers/futu_setup.md](doc/brokers/futu_setup.md), [doc/brokers/ibkr_setup.md](doc/brokers/ibkr_setup.md)
- Run: `./start_zuilow.sh` or `python -m zuilow.app` (see `env.example` and `requirements.txt`)
