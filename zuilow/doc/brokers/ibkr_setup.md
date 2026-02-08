# Interactive Brokers (IBKR) TWS / Gateway 配置说明

## 一、架构概述

ZuiLow 通过 **ib_insync** 连接 Interactive Brokers 的 TWS（Trader Workstation）或 IB Gateway，实现行情与交易接口。

| 组件 | 说明 |
|------|------|
| **TWS / IB Gateway** | 官方桌面程序或无界面网关，需在本机或服务器运行并开启 API |
| **ib_insync** | Python 异步封装，用于调用行情、账户、下单等 |

### 端口约定

**同一套 TWS/Gateway，端口由当前登录的账户类型决定**（模拟登录用模拟端口，实盘登录用实盘端口）。

| 环境 | TWS 端口 | IB Gateway 端口 |
|------|----------|-----------------|
| 模拟 (Paper) | 7497 | 4002 |
| 实盘 (Live) | 7496 | 4001 |

---

## 二、安装并启动 TWS / IB Gateway

1. 从 [Interactive Brokers 官网](https://www.interactivebrokers.com/en/trading/ib-api.php) 下载并安装 **TWS** 或 **IB Gateway**。
2. 登录对应环境（模拟或实盘）。
3. 在 TWS：**编辑 → 全局配置 → API → 设置** 中：
   - 勾选 **启用 ActiveX 和 Socket 客户端**
   - 勾选 **只读 API**（若仅需行情、不需下单）
   - 端口：TWS 模拟 7497，实盘 7496；Gateway 模拟 4002，实盘 4001
   - 信任的 IP：`127.0.0.1` 或运行 ZuiLow 的机器 IP
4. 在 IB Gateway 中同样在 **配置 → API → 设置** 里开启 Socket 客户端并设置端口。

---

## 三、Python 依赖

```bash
pip install ib_insync
```

无其他必选依赖。

---

## 四、ZuiLow 配置

### 1. 经纪商配置 `config/brokers/ibkr.yaml`

```yaml
# config/brokers/ibkr.yaml
ibkr:
  host: "127.0.0.1"
  port: 7497          # TWS 模拟=7497, 实盘=7496; Gateway 模拟=4002, 实盘=4001
  client_id: 1        # 连接 ID (1–32)；若报错 "client id is already in use"，改用 2 等或关闭其他占用该端口的程序
  read_only: false    # true 时仅行情/历史，不下单
  account: ""         # 留空使用默认账户；多账户时可填 DU123456 等
  timeout: 30         # 连接超时（秒）
```

### 2. 账户配置 `config/accounts/ (e.g. ibkr.yaml)`

在 `accounts` 列表中加入 IBKR 账户，供 Live 页与统一下单使用：

```yaml
accounts:
  - name: ibkr-main
    type: ibkr
    ibkr_account_id: ""   # 填 TWS/Gateway 中显示的账户 ID（如 DU123456），留空用默认

  - name: ibkr-paper
    type: ibkr
    ibkr_account_id: ""   # 模拟账户 ID，留空用默认
```

---

## 五、在 ZuiLow 中的使用

- **Brokers 页**：当前 IBKR 为 “Coming soon”，后续会在此页提供连接/断开与状态。
- **Live 页**：连接 IBKR 后，可在 Gateway 中选择 IB，并选择上述账户进行行情、持仓、下单。
- **统一下单**：`POST /api/order` 传入 `account=ibkr-main`（或你配置的 name）时，会经 IBKR 网关下单。

### 合约与代码格式

- 美股：`US.AAPL`、`US.TSLA`（交易所 SMART，货币 USD）
- 港股：`HK.00700`、`HK.09988`（交易所 SEHK，货币 HKD）

---

## 六、市场数据订阅

### 6.1 怎么知道有没有订阅？

- **看报错**：请求实时行情时若出现 **Error 10089**（“请求的市场数据对于API来说需要额外订阅”“延迟市场数据可用”），说明该市场**没有**实时行情订阅；此时可改用延迟数据或到 IB 开通订阅。
- **在 TWS 里看**：**账户 → 管理账户 → 设置** 中找 **市场数据订阅**（Market Data Subscriptions），可查看已订阅的市场及是否实时。
- **在 IBKR 官网看**：登录 [Account Management](https://www.interactivebrokers.com/portal) → **设置 → 用户设置 → 市场数据订阅**，可查看各市场订阅状态与费用。

### 6.2 在哪里订阅？

订阅在 **IBKR 的 TWS 或官网** 操作，不在 ZuiLow 代码里：

| 方式 | 路径 |
|------|------|
| **TWS** | **账户 → 管理账户 → 设置 → 市场数据订阅**（或 **Edit → Global Configuration → API** 附近相关入口） |
| **官网** | [Account Management](https://www.interactivebrokers.com/portal) → **设置 → 用户设置 → 市场数据订阅** |

选择需要的市场（如 US Securities、HK 等），勾选并接受费用条款即可开通。开通后，同一账户通过 API（含 ZuiLow）请求该市场行情即为实时。

### 6.3 ZuiLow 中的用法（API）

`IbkrGateway` 提供两个封装，便于按是否有订阅选择行情类型：

| 方法 | 说明 |
|------|------|
| `use_live_market_data()` | 使用**实时**行情（需在 IB 已订阅对应市场）；否则可能触发 10089。 |
| `use_delayed_market_data()` | 使用**延迟**行情（约 15 分钟延迟，免费、无需订阅），可避免 10089。 |

底层为 `set_market_data_type(type_id)`：`1`=Live，`2`=Frozen，`3`=Delayed，`4`=Delayed frozen。

- **未订阅实时行情时**：先调用 `gateway.use_delayed_market_data()`，再 `get_quote` 等，可正常取价且不报 10089。
- **已订阅实时行情时**：调用 `gateway.use_live_market_data()`，再 `get_quote`，获得实时价格。
- `get_quote` 返回的 `data_type` 会为 `"realtime"` 或 `"delayed"`，与当前设置一致。

---

## 七、参考链接

- [IB API 文档](https://www.interactivebrokers.com/en/trading/ib-api.php)
- [ib_insync 文档](https://ib-insync.readthedocs.io/)
