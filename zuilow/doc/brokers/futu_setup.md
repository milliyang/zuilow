# 富途牛牛 OpenD SDK 安装配置指南

## 一、架构概述

富途 OpenAPI 包含两个核心组件：

1. **OpenD（网关程序）**：负责与富途服务器通信，将数据暴露给本地或远程程序
2. **Futu API SDK**：语言封装层，用于调用行情与交易接口

### 支持平台

- **OpenD**: Windows、macOS、CentOS、Ubuntu
- **SDK**: Python、Java、C#、C++、JavaScript 等

### 当前环境

| 配置项 | 值 |
|-------|-----|
| OpenD 地址 | `10.147.17.99:11111` |
| 环境 | 模拟 / 实盘 |
| 支持市场 | 港股 (HK)、美股 (US) |

---

## 二、下载安装 OpenD

### 下载地址

官方下载页面：https://openapi.futunn.com/futu-api-doc/quick/opend-base.html

### 方式一：可视化 OpenD（GUI）

1. 下载对应系统的可视化版本
2. 解压后执行安装程序
3. 启动后配置监听地址、端口等

### 方式二：命令行 OpenD（CLI）

1. 下载命令行版本压缩包
2. 解压得到：
   - `FutuOpenD` 可执行文件
   - `FutuOpenD.xml` 配置文件
   - `Appdata.dat` 数据文件（必需）

3. 编辑 `FutuOpenD.xml` 配置：

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<root>
    <!-- 登录账号（牛牛号/邮箱/手机号） -->
    <login_account>your_account</login_account>
    
    <!-- 登录密码（二选一） -->
    <login_pwd>your_password</login_pwd>
    <!-- <login_pwd_md5>md5_of_password</login_pwd_md5> -->
    
    <!-- API 服务配置 -->
    <ip>127.0.0.1</ip>
    <api_port>11111</api_port>
    
    <!-- 可选配置 -->
    <log_level>info</log_level>
    <lang>chs</lang>
</root>
```

4. 启动：

```bash
# Linux/macOS
./FutuOpenD

# Windows
FutuOpenD.exe

# 或通过命令行参数
./FutuOpenD -login_account=xxx -login_pwd=xxx
```

---

## 三、Python SDK 安装

### 环境要求

- Python 3.6+（推荐 3.8+）
- 支持系统：Windows 7+、macOS 10.11+、Ubuntu 16.04+、CentOS 7+

### 安装

```bash
# 安装
pip install futu-api

# 更新
pip install futu-api --upgrade
```

### 可选依赖

```bash
# 技术分析库（如需指标计算）
pip install TA-Lib
```

---

## 四、快速上手

### 前置条件

1. OpenD 已启动并登录成功
2. 默认监听地址：`127.0.0.1:11111`

### 示例代码

```python
from futu import *

# ============================================================
# 行情接口
# ============================================================
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

# 获取快照
ret, data = quote_ctx.get_market_snapshot(['HK.00700'])
if ret == RET_OK:
    print(data)
else:
    print('error:', data)

# 获取 K 线
ret, data, _ = quote_ctx.request_history_kline('HK.00700', 
                                                start='2025-01-01',
                                                end='2025-01-20',
                                                ktype=KLType.K_DAY)
if ret == RET_OK:
    print(data)

quote_ctx.close()

# ============================================================
# 交易接口（模拟环境）
# ============================================================
trade_ctx = OpenHKTradeContext(host='127.0.0.1', port=11111)

# 解锁交易（需要交易密码）
ret, data = trade_ctx.unlock_trade(password='your_trade_password')
if ret != RET_OK:
    print('unlock failed:', data)

# 模拟下单
ret, data = trade_ctx.place_order(
    price=500.0,
    qty=100,
    code="HK.00700",
    trd_side=TrdSide.BUY,
    order_type=OrderType.NORMAL,
    trd_env=TrdEnv.SIMULATE  # 模拟环境
)
print(ret, data)

trade_ctx.close()
```

### 市场代码前缀

| 市场 | 前缀 | 示例 |
|------|------|------|
| 港股 | HK | HK.00700 |
| 美股 | US | US.AAPL |
| A股沪 | SH | SH.600000 |
| A股深 | SZ | SZ.000001 |

---

## 五、实时订阅示例

```python
from futu import *

class QuoteHandler(StockQuoteHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret, data = super().on_recv_rsp(rsp_pb)
        if ret == RET_OK:
            print(data)
        return ret, data

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
quote_ctx.set_handler(QuoteHandler())

# 订阅报价
ret, data = quote_ctx.subscribe(['HK.00700'], [SubType.QUOTE])
print(ret, data)

# 保持运行
import time
time.sleep(60)

quote_ctx.close()
```

---

## 六、常见问题

### 1. 连接失败

- 确认 OpenD 已启动并登录成功
- 检查 host/port 配置是否正确
- 检查防火墙设置

### 2. 权限不足

- 部分行情/交易功能需要相应权限
- 登录 OpenD 后可在界面确认权限状态
- 可能需要开通相应的行情订阅

### 3. 端口被占用

```bash
# 检查端口占用
lsof -i :11111

# 修改配置使用其他端口
```

### 4. 交易密码

- 真实交易需要交易密码解锁
- 模拟交易部分功能也需要解锁

---

## 七、ZuiLow Broker 接入方式

### 7.1 使用 FutuBroker 类

`FutuBroker` 是对富途 API 的封装，位于 `zuilow/live/futu_broker.py`。

#### 初始化配置

```python
from zuilow.live.futu_broker import FutuBroker, FutuConfig, FutuMarket

# 创建配置 (连接远程 OpenD)
config = FutuConfig(
    host="10.147.17.99",      # OpenD 地址
    port=11111,                # OpenD 端口
    market=FutuMarket.HK,      # 市场: HK/US/CN/SG
    env="SIMULATE",            # 环境: SIMULATE=模拟, REAL=实盘
    unlock_password="",        # 交易密码 (实盘必填)
    acc_id=None,               # 账户ID (None=自动选择)
)

# 创建 broker 实例
broker = FutuBroker(config=config)
```

#### 连接与断开

```python
# 方式1: 手动管理
broker.connect()
# ... 执行操作
broker.disconnect()

# 方式2: 上下文管理器 (推荐)
with FutuBroker(config=config) as broker:
    # 自动连接和断开
    print(broker.get_account_info())
```

#### 行情接口

```python
# 获取实时报价
quote = broker.get_quote("HK.00700")
print(f"腾讯: {quote['price']}")

# 获取历史K线
df = broker.get_history(
    symbol="HK.00700",
    start="2025-01-01",
    end="2025-01-20",
    ktype="K_DAY"  # K_DAY, K_1M, K_5M, K_15M, K_30M, K_60M
)
```

#### 交易接口

```python
from zuilow.backtest.types import OrderSide, OrderType

# 下单
order_id = broker.place_order(
    symbol="HK.00700",
    side=OrderSide.BUY,
    quantity=100,
    price=350.0,
    order_type=OrderType.LIMIT
)

# 便捷方法
order_id = broker.buy("HK.00700", 100, price=350.0)   # 买入
order_id = broker.sell("HK.00700", 100, price=360.0)  # 卖出
broker.close_position("HK.00700")                      # 平仓

# 撤单
broker.cancel_order(order_id)
```

#### 账户与持仓

```python
# 账户信息
info = broker.get_account_info()
print(f"总资产: {info['total_assets']}")
print(f"现金: {info['cash']}")
print(f"购买力: {info['power']}")

# 持仓列表
positions = broker.get_positions()
for pos in positions:
    print(f"{pos['symbol']}: {pos['quantity']}股, 盈亏 {pos['pnl']}")

# 单个持仓
pos = broker.get_position("HK.00700")

# 订单列表
orders = broker.get_orders(status="PENDING")

# 成交记录
deals = broker.get_deals()
```

#### 多账户管理

```python
# 获取账户列表
accounts = broker.get_account_list()
for acc in accounts:
    print(f"账户: {acc['acc_id']}, 类型: {acc['acc_type']}, 环境: {acc['trd_env']}")

# 切换账户
broker.switch_account(acc_id=12345678)

# 当前账户ID
print(broker.current_account_id)
```

---

## 八、Web API 接口

### 8.1 启动 Web 服务

```bash
cd /home/leo/work/quant/sai

# 方式1: 直接运行
python -m zuilow.web.app

# 方式2: uvicorn (推荐，支持热重载)
uvicorn zuilow.web.app:app --reload --host 0.0.0.0 --port 8000
```

### 8.2 可用 API 端点

#### 系统状态

```bash
GET /api/status

# 响应示例
{
    "status": "running",
    "timestamp": "2026-01-21T10:30:00",
    "version": "0.1.0"
}
```

#### 账户信息

```bash
GET /api/account

# 响应示例
{
    "cash": 85000.00,
    "equity": 102500.00,
    "market_value": 17500.00,
    "pnl": 2500.00,
    "pnl_pct": 2.5,
    "positions": [...],
    "updated_at": "2026-01-21T10:30:00"
}
```

#### 交易记录

```bash
GET /api/trades?limit=20

# 响应示例
{
    "trades": [
        {
            "id": "t001",
            "symbol": "AAPL",
            "side": "buy",
            "quantity": 50,
            "price": 150.0,
            "timestamp": "2026-01-21 10:30:00"
        }
    ],
    "total": 1
}
```

#### 策略列表

```bash
GET /api/strategies

# 响应示例
{
    "strategies": [
        {"id": "buyhold", "name": "买入持有", ...},
        {"id": "sma", "name": "均线策略", ...},
        {"id": "rsi", "name": "RSI策略", ...}
    ]
}
```

#### 运行回测

```bash
POST /api/backtest
Content-Type: application/json

{
    "symbol": "SPY",
    "strategy": "sma",
    "initial_capital": 100000,
    "short_period": 5,
    "long_period": 20
}

# 响应示例
{
    "success": true,
    "summary": {
        "strategy": "sma",
        "symbol": "SPY",
        "period": "2025-01-01 ~ 2025-12-31",
        "total_return_pct": 15.5
    },
    "metrics": {
        "sharpe_ratio": 1.2,
        "max_drawdown": -8.5,
        "win_rate": 55.0
    },
    "equity_curve": [...],
    "trades": [...]
}
```

### 8.3 计划扩展的 Futu 相关 API

以下接口计划在后续版本添加：

| 端点 | 方法 | 描述 |
|-----|------|-----|
| `/api/futu/connect` | POST | 连接 OpenD |
| `/api/futu/disconnect` | POST | 断开连接 |
| `/api/futu/quote/{symbol}` | GET | 获取实时报价 |
| `/api/futu/account` | GET | 获取富途账户信息 |
| `/api/futu/positions` | GET | 获取持仓 |
| `/api/futu/orders` | GET | 获取订单列表 |
| `/api/futu/order` | POST | 下单 |
| `/api/futu/order/{id}` | DELETE | 撤单 |

---

## 九、模拟账号接口

### 9.1 使用富途模拟环境

富途提供免费的模拟交易环境，无需真实资金即可测试策略。

#### 连接模拟账户

```python
from zuilow.live.futu_broker import FutuBroker, FutuConfig, FutuMarket

# 模拟港股
config_hk = FutuConfig(
    host="10.147.17.99",
    port=11111,
    market=FutuMarket.HK,
    env="SIMULATE",  # 关键: 设置为模拟环境
)

# 模拟美股
config_us = FutuConfig(
    host="10.147.17.99",
    port=11111,
    market=FutuMarket.US,
    env="SIMULATE",
)

with FutuBroker(config=config_hk) as broker:
    # 模拟交易，使用虚拟资金
    info = broker.get_account_info()
    print(f"模拟账户资金: {info['cash']}")
    
    # 模拟下单 (不会真正执行)
    order_id = broker.buy("HK.00700", 100, price=350.0)
```

#### 模拟 vs 实盘对比

| 功能 | 模拟环境 | 实盘环境 |
|-----|---------|---------|
| 资金 | 虚拟 100万 | 真实资金 |
| 行情 | 延迟15分钟 | 实时 |
| 成交 | 即时模拟成交 | 真实撮合 |
| 交易密码 | 不需要 | 需要解锁 |
| 风险 | 无 | 真实亏损 |

### 9.2 使用本地模拟券商

对于完全离线的测试，可以使用 `SimulatedBroker`：

```python
from zuilow.trading.broker import SimulatedBroker, BrokerConfig
from zuilow.backtest.types import OrderSide

# 创建模拟券商
config = BrokerConfig(
    commission_rate=0.001,  # 0.1% 手续费
    min_commission=5.0,     # 最低 5 元
    slippage=0.001,         # 0.1% 滑点
)

broker = SimulatedBroker(
    initial_capital=100000.0,
    config=config
)

# 提交订单
order = broker.submit_order(
    symbol="AAPL",
    side=OrderSide.BUY,
    quantity=100,
    price=150.0
)

# 模拟成交 (需要手动提供价格)
trade = broker.fill_order(order.id, price=150.0)

# 查看账户
print(broker.summary())
```

### 9.3 完整示例：模拟交易策略

```python
#!/usr/bin/env python3
"""模拟交易示例"""

from zuilow.live.futu_broker import FutuBroker, FutuConfig, FutuMarket
from zuilow.backtest.types import OrderSide
import time

def simple_strategy():
    """简单的模拟交易策略"""
    
    config = FutuConfig(
        host="10.147.17.99",
        port=11111,
        market=FutuMarket.HK,
        env="SIMULATE",
    )
    
    with FutuBroker(config=config) as broker:
        # 1. 查看账户
        info = broker.get_account_info()
        print(f"初始资金: ${info['cash']:,.2f}")
        
        # 2. 获取报价
        quote = broker.get_quote("HK.00700")
        if not quote:
            print("获取报价失败")
            return
        
        current_price = quote['price']
        print(f"腾讯当前价: ${current_price}")
        
        # 3. 简单策略：如果价格低于 350，买入
        if current_price < 350:
            print("价格低于 350，执行买入...")
            order_id = broker.buy("HK.00700", 100, price=current_price)
            if order_id:
                print(f"下单成功: {order_id}")
        
        # 4. 查看持仓
        time.sleep(2)  # 等待成交
        positions = broker.get_positions()
        for pos in positions:
            print(f"持仓: {pos['symbol']} {pos['quantity']}股")
        
        # 5. 最终账户状态
        info = broker.get_account_info()
        print(f"最终资金: ${info['cash']:,.2f}")


if __name__ == "__main__":
    simple_strategy()
```

---

## 十、参考链接

- 官方文档：https://openapi.futunn.com/futu-api-doc/
- Python SDK GitHub：https://github.com/FutunnOpen/py-futu-api
- API 接口列表：https://openapi.futunn.com/futu-api-doc/api-ref/
- 模拟交易说明：https://openapi.futunn.com/futu-api-doc/trade/overview.html
