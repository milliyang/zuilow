# 模拟时间服务（Simulation Time Service）

全栈模拟环境下**唯一的「当前时间」来源**。ZuiLow、PPT 等在模拟模式下从本服务获取当前时间，实现可重复、按步推进的回放。

---

## 作用

- 提供统一的 **sim-time**（模拟时间），供 ZuiLow scheduler、PPT 订单/成交时间等使用。
- 支持 **设置时间**、**按步推进**，以及 **推进后触发 ZuiLow 一次 scheduler tick**。
- 所有时间以 **UTC** 存储和返回（ISO 8601）。

---

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/now` | 返回当前模拟时间，如 `{"now": "2024-01-15T09:35:00Z"}` |
| POST | `/set` | 设置当前时间，请求体 `{"now": "2024-01-15T09:35:00Z"}` |
| POST | `/advance` | 推进时间，请求体任选其一：`{"days": 1}`、`{"hours": 1}`、`{"minutes": 5}`、`{"seconds": 300}` |
| POST | `/advance-and-tick` | **后台**推进 N 步并每步触发一次 ZuiLow tick（需设 `ZUILOW_TICK_URL`）。请求体同 `/advance`，如 `{"days": 5}`。返回 **202** `{"status": "started", "steps": N}`；若已在运行返回 409。单次 tick 超时由 `ZUILOW_TICK_TIMEOUT` 控制（默认 600 秒）。 |
| GET | `/advance-and-tick/status` | 查询当前任务状态：`running`, `steps_done`, `steps_total`, `executed_total`, `cancelled`, `error`, `now`。 |
| POST | `/advance-and-tick/cancel` | 请求取消正在运行的任务；下一步将不再执行，当前步可能仍会跑完。 |
| GET | `/config` | 返回 UI 所需配置（如 `zuilow_tick_url`） |

---

### 串行等待说明（重要）

每一步都会 **等待 ZuiLow 的 tick 响应** 后才进行下一步，因此：

- ZuiLow 每次处理 tick 时，sim-time 仍是**当前这一步**的时间；
- 不会出现「sim-time 已连续推进 5 次、而 ZuiLow 还在跑第一次」导致时间错位；
- ZuiLow 的 `/api/scheduler/tick` 会同步执行完该次 tick 的所有作业后才返回 200，时间服务收到响应后才推进下一单位并发下一次 tick。

---
