# Simulation Time Service

The **single source of “current time”** in the full-stack simulation environment. ZuiLow, PPT, etc. read the current time from this service in simulation mode for repeatable, step-driven replay.

---

## Purpose

- Provides a unified **sim-time** (simulation time) for ZuiLow scheduler, PPT order/trade timestamps, and related components.
- Supports **set time**, **advance by step**, and **advance then trigger one ZuiLow scheduler tick**.
- All times are stored and returned in **UTC** (ISO 8601).

---

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/now` | Returns current sim time, e.g. `{"now": "2024-01-15T09:35:00Z"}` |
| POST | `/set` | Set current time. Body: `{"now": "2024-01-15T09:35:00Z"}` |
| POST | `/advance` | Advance time. Body: one of `{"days": 1}`, `{"hours": 1}`, `{"minutes": 5}`, `{"seconds": 300}` |
| POST | `/advance-and-tick` | **Background** job: advance N steps and trigger one ZuiLow tick per step (requires `ZUILOW_TICK_URL`). Body same as `/advance`, e.g. `{"days": 5}`. Returns **202** `{"status": "started", "steps": N}`; 409 if a job is already running. Per-step tick timeout: `ZUILOW_TICK_TIMEOUT` (default 600 s). |
| GET | `/advance-and-tick/status` | Job status: `running`, `steps_done`, `steps_total`, `executed_total`, `cancelled`, `error`, `now`. |
| POST | `/advance-and-tick/cancel` | Cancel the running job; the current step may still finish, the next step will not run. |
| GET | `/config` | Returns UI config (e.g. `zuilow_tick_url`, `zuilow_tick_timeout`). |
| POST | `/config` | Override config. Body: `{"zuilow_tick_url": "http://...", "zuilow_tick_timeout": 3600}` (optional fields). |

---

### 60-minute step and market open/close

When using fine-grained step with **60 min** and snap-to-boundary, set env `MARKET_OPEN_TIME` (e.g. `09:30`), `MARKET_CLOSE_TIME` (e.g. `16:00`), `MARKET_TIMEZONE` (e.g. `America/New_York`) to insert one extra tick at open and one at close when a 60-min step would skip them. If unset, no extra ticks.

---

### Serial wait (important)

Each step **waits for ZuiLow’s tick response** before the next step, so:

- When ZuiLow handles a tick, sim-time is still the **current step’s** time;
- Sim-time is not advanced multiple times while ZuiLow is still on the first tick;
- ZuiLow’s `/api/scheduler/tick` runs that tick’s jobs to completion and returns 200; this service then advances time and sends the next tick.

---
