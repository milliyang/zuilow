"""
IBKR gateway: quotes and trading via TWS/Gateway. Requires ib_insync.

Classes:
    IbkrConfig   host, port, client_id, read_only, account, timeout
    IbkrGateway  Connect to TWS/Gateway; quote/history + account/orders (same interface as FutuGateway)

IbkrGateway methods:
    .connect() -> bool
    .disconnect()
    .is_connected -> bool
    .set_market_data_type(type_id: int) -> None  1=Live, 2=Frozen, 3=Delayed, 4=Delayed frozen
    .use_live_market_data() -> None   same as set_market_data_type(1)
    .use_delayed_market_data() -> None   same as set_market_data_type(3), avoids 10089 when no real-time subscription
    .get_quote(symbol: str) -> dict | None
    .get_history(symbol, start, end, ktype) -> Optional[DataFrame]
    .get_account_info(account=None) -> Optional[dict]
    .get_positions(account=None) -> list[dict]
    .place_order(symbol, side, quantity, price=None, order_type=..., account=None) -> Optional[str]
    .cancel_order(order_id, account=None) -> bool
    .get_orders(account=None) -> list[dict]
    .get_deals(account=None) -> list[dict]
"""

from __future__ import annotations

import asyncio
import logging
import math
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _safe_float(v: Any, default: float = 0.0) -> float:
    """Coerce to float; return default if None or NaN."""
    if v is None:
        return default
    try:
        f = float(v)
        return f if not math.isnan(f) else default
    except (TypeError, ValueError):
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    """Coerce to int; return default if None or NaN."""
    if v is None:
        return default
    try:
        f = float(v)
        if math.isnan(f):
            return default
        return int(f)
    except (TypeError, ValueError):
        return default

from zuilow.components.control import ctrl

logger = logging.getLogger(__name__)

try:
    from ib_insync import IB, Stock, util, MarketOrder, LimitOrder
    HAS_IB_INSYNC = True
except ImportError:
    IB = None
    Stock = None
    util = None
    MarketOrder = None
    LimitOrder = None
    HAS_IB_INSYNC = False
    logger.warning("ib_insync not installed: pip install ib_insync")


@dataclass
class IbkrConfig:
    """IBKR TWS/Gateway connection config."""
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 1
    read_only: bool = False
    account: str = ""
    timeout: int = 30

    @classmethod
    def from_yaml(cls, path: str | None = None) -> "IbkrConfig":
        """Load config from YAML (default: config/brokers/ibkr.yaml)."""
        if path is None:
            path = Path(__file__).parent.parent.parent / "config" / "brokers" / "ibkr.yaml"
        else:
            path = Path(path)
        if not path.exists():
            logger.warning("Config file not found: %s, using defaults", path)
            return cls()
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        ibkr = data.get("ibkr", {})
        return cls(
            host=ibkr.get("host", "127.0.0.1"),
            port=int(ibkr.get("port", 7497)),
            client_id=int(ibkr.get("client_id", 1)),
            read_only=bool(ibkr.get("read_only", False)),
            account=str(ibkr.get("account", "") or ""),
            timeout=int(ibkr.get("timeout", 30)),
        )


def _symbol_to_contract(symbol: str) -> Any:
    """
    Convert ZuiLow symbol (e.g. US.AAPL, HK.00700) to IB Contract.
    US.* -> Stock(symbol, 'SMART', 'USD'), HK.* -> Stock(symbol, 'SEHK', 'HKD').
    """
    if not HAS_IB_INSYNC:
        return None
    s = (symbol or "").strip().upper()
    if not s:
        return None
    if s.startswith("US."):
        ticker = s[3:].strip()
        return Stock(ticker, "SMART", "USD")
    if s.startswith("HK."):
        ticker = s[3:].strip().lstrip("0") or "0"
        return Stock(ticker, "SEHK", "HKD")
    return Stock(s, "SMART", "USD")


def _contract_to_symbol(contract: Any) -> str:
    """Convert IB Contract to ZuiLow symbol (e.g. US.AAPL, HK.00700)."""
    if contract is None:
        return ""
    sym = getattr(contract, "symbol", "") or ""
    exc = getattr(contract, "exchange", "") or ""
    curr = getattr(contract, "currency", "") or ""
    if exc == "SMART" and curr == "USD":
        return f"US.{sym}"
    if exc == "SEHK" or curr == "HKD":
        return f"HK.{sym.zfill(5)}"
    return sym


# Ktype (Futu-style) -> (barSizeSetting, durationStr for 1 year)
_KTYPE_TO_IB = {
    "K_DAY": ("1 day", "365 D"),
    "K_WEEK": ("1 week", "52 W"),
    "K_MON": ("1 month", "12 M"),
    "K_1M": ("1 min", "1 D"),
    "K_5M": ("5 mins", "5 D"),
    "K_15M": ("15 mins", "5 D"),
    "K_30M": ("30 mins", "5 D"),
    "K_60M": ("1 hour", "5 D"),
}


class IbkrGateway:
    """
    IBKR gateway: quotes (get_quote), history (get_history) via TWS/Gateway.
    Same interface as FutuGateway for use with MarketService.
    """

    def __init__(self, config: IbkrConfig | None = None):
        if not HAS_IB_INSYNC:
            raise ImportError("Install ib_insync: pip install ib_insync")
        self.config = config or IbkrConfig()
        self._ib: Any = None
        self._connected = False
        self._connection_loop: asyncio.AbstractEventLoop | None = None  # runs in _connection_thread; other threads submit work via _run_on_connection_loop
        self._connection_thread: threading.Thread | None = None
        # reqPositionsAsync uses key 'positions'; serialize to avoid concurrent overwrite.
        self._positions_lock: threading.Lock = threading.Lock()
        self._market_data_type: int = 1  # 1=Live, 3=Delayed; used for get_quote data_type in response

    def _run_connection_thread(self, host: str, port: int, ready_event: threading.Event, connect_result: list) -> None:
        """Run in background thread: create loop, connect IB with connectAsync on this loop, then run_forever.
        Using connectAsync (not sync connect) ensures the IB client is bound to this loop; sync connect()
        uses util.run() which may create and close a different loop, leaving accountSummaryAsync etc. never completing.
        """
        loop = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def do_connect() -> None:
                ib = IB()
                await ib.connectAsync(
                    host=host,
                    port=port,
                    clientId=self.config.client_id,
                    timeout=self.config.timeout or 4,
                    readonly=self.config.read_only,
                    account=self.config.account or "",
                )
                return ib

            ib = loop.run_until_complete(do_connect())
            self._ib = ib
            self._connected = True
            self._connection_loop = loop
            logger.info("Connected to IBKR %s:%s", host, port)
            # Connection probe: verify API responds (not just TCP). If this fails, TWS/Gateway may not be ready.
            try:
                t = loop.run_until_complete(asyncio.wait_for(ib.reqCurrentTimeAsync(), timeout=5))
                logger.info("IBKR connection probe: reqCurrentTime ok -> %s", t)
            except Exception as ex:
                logger.warning("IBKR connection probe: reqCurrentTime failed (API not responding): %s", ex)
            connect_result.append(True)
        except Exception as e:
            logger.exception("IBKR connect failed in thread: %s", e)
            connect_result.append(e)
            if loop:
                loop.close()
        finally:
            ready_event.set()
        if loop and self._connected:
            try:
                loop.run_forever()
            finally:
                loop.close()
            self._ib = None
            self._connected = False
            self._connection_loop = None
            logger.info("IBKR connection thread stopped")

    def connect(self, host: str | None = None, port: int | None = None) -> bool:
        """Connect to TWS or IB Gateway in a dedicated thread; event loop stays running for account/orders. Returns True on success."""
        if self._connection_thread is not None and self._connection_thread.is_alive():
            return self._connected
        h = host if host is not None else self.config.host
        p = port if port is not None else self.config.port
        ready_event = threading.Event()
        connect_result: list = []
        self._connection_thread = threading.Thread(
            target=self._run_connection_thread,
            args=(h, p, ready_event, connect_result),
            daemon=True,
        )
        self._connection_thread.start()
        if not ready_event.wait(timeout=self.config.timeout + 5):
            logger.warning("IBKR connect timed out waiting for thread")
            return False
        if not connect_result:
            return False
        if isinstance(connect_result[0], Exception):
            self._connected = False
            self._connection_loop = None
            self._connection_thread = None
            return False
        return True  # connect_result[0] is True

    def disconnect(self) -> None:
        """Disconnect from TWS/Gateway and stop the connection thread."""
        loop = self._connection_loop
        if loop is not None and loop.is_running():

            def _shutdown() -> None:
                if self._ib and self._ib.isConnected():
                    try:
                        self._ib.disconnect()
                    except Exception as e:
                        logger.debug("IBKR disconnect: %s", e)
                self._ib = None
                self._connected = False
                loop.stop()

            loop.call_soon_threadsafe(_shutdown)
        if self._connection_thread is not None and self._connection_thread.is_alive():
            self._connection_thread.join(timeout=5)
        self._ib = None
        self._connected = False
        self._connection_loop = None
        self._connection_thread = None
        logger.info("Disconnected from IBKR")

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ib is not None and self._ib.isConnected()

    def _get_connection_loop(self) -> asyncio.AbstractEventLoop | None:
        """Event loop used by the IB connection (must run calls from other threads on this loop)."""
        return self._connection_loop

    def _run_on_connection_loop(self, coro, timeout_sec: float = 25, label: str = "") -> Any:
        """Run a coroutine on the connection's event loop (for use from Flask request thread).
        Uses wait_for so a stuck coroutine is cancelled and does not block the loop for other requests.
        label: optional name (e.g. 'accountSummaryAsync') for timeout logs.
        """
        loop = self._get_connection_loop()
        if loop is None:
            logger.warning("IBKR _run_on_connection_loop: no connection loop")
            return None
        if not loop.is_running():
            logger.warning("IBKR _run_on_connection_loop: loop not running")
            return None

        op_name = label or "coroutine"

        async def _run_with_timeout(c):
            try:
                return await asyncio.wait_for(c, timeout=timeout_sec)
            except asyncio.TimeoutError:
                logger.warning("IBKR _run_on_connection_loop: %s timed out after %ss (cancelled)", op_name, timeout_sec)
                return None

        try:
            future = asyncio.run_coroutine_threadsafe(_run_with_timeout(coro), loop)
            return future.result(timeout=timeout_sec + 5)
        except Exception as e:
            logger.warning("IBKR _run_on_connection_loop: %s: %s", type(e).__name__, e)
            return None

    def set_market_data_type(self, type_id: int) -> None:
        """Set market data type for subsequent requests. Runs on connection loop.
        type_id: 1=Live, 2=Frozen, 3=Delayed (free, ~15 min), 4=Delayed frozen.
        """
        if not self.is_connected or self._ib is None:
            logger.warning("IBKR set_market_data_type: not connected")
            return

        async def _set():
            self._ib.reqMarketDataType(type_id)

        self._run_on_connection_loop(_set(), timeout_sec=5, label="reqMarketDataType")
        self._market_data_type = type_id

    def use_live_market_data(self) -> None:
        """Use live (real-time) market data. Requires subscription; otherwise use use_delayed_market_data()."""
        self.set_market_data_type(1)

    def use_delayed_market_data(self) -> None:
        """Use delayed market data (~15 min). Free, avoids Error 10089 when real-time is not subscribed."""
        self.set_market_data_type(3)

    def get_quote(self, symbol: str) -> dict | None:
        """Get snapshot quote. Uses current market data type (call use_delayed_market_data() to avoid 10089)."""
        if not self.is_connected:
            logger.error("Not connected")
            return None
        contract = _symbol_to_contract(symbol)
        if contract is None:
            return None
        try:
            async def _fetch_quote():
                tickers = await self._ib.reqTickersAsync(contract)
                if not tickers:
                    return None
                t = tickers[0]
                last = _safe_float(t.last) or _safe_float(t.close)
                return {
                    "symbol": symbol,
                    "name": getattr(contract, "symbol", "") or "",
                    "price": last,
                    "open": _safe_float(t.open),
                    "high": _safe_float(t.high),
                    "low": _safe_float(t.low),
                    "prev_close": _safe_float(t.close),
                    "volume": _safe_int(t.volume),
                    "turnover": 0,
                    "timestamp": ctrl.get_current_time_iso(),
                    "source": "ibkr",
                    "data_type": "delayed" if self._market_data_type == 3 else "realtime",
                }
            out = self._run_on_connection_loop(
                _fetch_quote(), timeout_sec=15, label="get_quote"
            )
            return out
        except Exception as e:
            logger.warning("IBKR get_quote %s: %s", symbol, e)
            return None

    def get_history(
        self,
        symbol: str,
        start: str,
        end: str,
        ktype: str = "K_DAY",
    ) -> Any:
        """
        Get history OHLCV. Args: symbol, start (YYYY-MM-DD), end, ktype (K_DAY, K_1M, ...).
        Returns DataFrame with Open, High, Low, Close, Volume (and time index).
        """
        if not self.is_connected:
            logger.error("Not connected")
            return None
        contract = _symbol_to_contract(symbol)
        if contract is None:
            return None
        bar_setting, duration = _KTYPE_TO_IB.get(ktype, ("1 day", "365 D"))
        try:
            import pandas as pd
            end_dt = end + " 23:59:59" if (end and " " not in end) else (end or "")
            bars = self._ib.reqHistoricalData(
                contract,
                endDateTime=end_dt,
                durationStr=duration,
                barSizeSetting=bar_setting,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
                timeout=60,
            )
            if not bars:
                return None
            df = util.df(bars)
            if df.empty:
                return None
            col_map = {"date": "time", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            if "time" not in df.columns and "date" in df.columns:
                df["time"] = df["date"]
            cols = [c for c in ["time", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
            df = df[cols].copy()
            df = df.set_index("time")
            if start or end:
                try:
                    if start:
                        df = df[df.index >= pd.Timestamp(start)]
                    if end:
                        df = df[df.index <= pd.Timestamp(end + " 23:59:59" if len(end) <= 10 else end)]
                except Exception:
                    pass
            return df
        except Exception as e:
            logger.warning("IBKR get_history %s: %s", symbol, e)
            return None

    # ========== Account ==========

    def _default_account(self) -> str:
        """Return default account (config or first managed)."""
        if self.config.account and self.config.account.strip():
            return self.config.account.strip()
        if self._ib and self._ib.managedAccounts():
            return self._ib.managedAccounts()[0]
        return ""

    def get_account_info(self, account: str | None = None) -> dict | None:
        """Get account info. account: optional IB account id (e.g. DU123456); when None, use default."""
        if not self.is_connected:
            logger.error("Not connected")
            return None
        acc = (account or "").strip() or self._default_account()
        if not acc:
            logger.error("No account specified and no default account")
            return None
        try:
            summary = self._run_on_connection_loop(
                self._ib.accountSummaryAsync(account=acc), label="accountSummaryAsync"
            )
            if summary is None:
                logger.warning(
                    "IBKR get_account_info: accountSummaryAsync returned None (timeout or loop not running). "
                    "If connection probe (reqCurrentTime) succeeded, check TWS/Gateway: Read-Only API, account id, or account not loaded."
                )
                return None
            # Prefer BASE currency when multiple currencies present (e.g. NetLiquidation;HKD and NetLiquidation;BASE)
            by_tag: dict[str, str] = {}
            for av in summary:
                tag = getattr(av, "tag", "") or ""
                curr = getattr(av, "currency", "") or ""
                val = getattr(av, "value", "") or ""
                if not tag:
                    continue
                if tag in by_tag and curr != "BASE":
                    continue  # keep existing (prefer BASE)
                by_tag[tag] = val
            net_liq = float(by_tag.get("NetLiquidation", 0) or 0)
            cash = float(by_tag.get("TotalCashValue", 0) or 0)
            gross_pos = float(by_tag.get("GrossPositionValue", 0) or 0)
            return {
                "cash": cash,
                "total_assets": net_liq,
                "market_value": gross_pos,
                "frozen_cash": 0,
                "available_funds": float(by_tag.get("AvailableFunds", 0) or 0),
                "currency": by_tag.get("Currency", "USD") or "USD",
                "power": net_liq,
            }
        except Exception as e:
            logger.warning("IBKR get_account_info %s: %s", acc, e)
            return None

    def get_positions(self, account: str | None = None) -> list[dict]:
        """Get positions. account: optional IB account id; when None, use default.
        Uses reqPositionsAsync() (one-shot, completes on positionEnd) per ib_insync docs;
        reqAccountUpdates is for startup subscription and often does not complete in request context.
        """
        if not self.is_connected:
            logger.error("Not connected")
            return []
        acc = (account or "").strip() or self._default_account()
        if not acc:
            logger.error("No account specified and no default account")
            return []
        try:
            # One-shot request: reqPositionsAsync() returns when TWS sends positionEnd()
            async def _fetch_positions():
                await self._ib.reqPositionsAsync()
            with self._positions_lock:
                self._run_on_connection_loop(
                    _fetch_positions(), timeout_sec=15, label="reqPositionsAsync"
                )
            # positions() returns list filtered by account from wrapper cache
            pos_list = self._ib.positions(account=acc)
            # Optional: merge portfolio (market value, PnL) if already in cache from connection
            portfolio_by_conid = {}
            for item in self._ib.portfolio(account=acc):
                c = getattr(item, "contract", None)
                if c is not None:
                    portfolio_by_conid[getattr(c, "conId", None)] = item
            positions = []
            for p in pos_list:
                qty = int(getattr(p, "position", 0) or 0)
                if qty == 0:
                    continue
                contract = getattr(p, "contract", None)
                sym = _contract_to_symbol(contract)
                avg_cost = float(getattr(p, "avgCost", 0) or 0)
                port_item = portfolio_by_conid.get(getattr(contract, "conId", None)) if contract else None
                if port_item:
                    mkt_val = float(getattr(port_item, "marketValue", 0) or 0)
                    mkt_price = float(getattr(port_item, "marketPrice", 0) or 0) or avg_cost
                    unreal_pnl = float(getattr(port_item, "unrealizedPNL", 0) or 0)
                else:
                    mkt_val = avg_cost * abs(qty)
                    mkt_price = avg_cost
                    unreal_pnl = 0.0
                positions.append({
                    "symbol": sym,
                    "name": getattr(contract, "symbol", "") or "" if contract else "",
                    "quantity": qty,
                    "available": qty,
                    "avg_price": avg_cost,
                    "current_price": mkt_price,
                    "market_value": mkt_val,
                    "pnl": unreal_pnl,
                    "pnl_pct": (unreal_pnl / (avg_cost * abs(qty)) * 100) if (avg_cost and qty) else 0.0,
                })
            return positions
        except Exception as e:
            logger.warning("IBKR get_positions %s: %s", acc, e)
            return []

    # ========== Trading ==========

    def place_order(
        self,
        symbol: str,
        side: Any,
        quantity: int,
        price: float | None = None,
        order_type: Any = None,
        account: str | None = None,
    ) -> str | None:
        """Place order. account: optional IB account id; when None, use default. Returns order_id or None on failure."""
        if not self.is_connected:
            logger.error("Not connected")
            return None
        if self.config.read_only:
            logger.error("IBKR read_only: orders disabled")
            return None
        acc = (account or "").strip() or self._default_account()
        if not acc:
            logger.error("No account specified and no default account")
            return None
        contract = _symbol_to_contract(symbol)
        if contract is None:
            return None
        side_val = getattr(side, "value", side) if hasattr(side, "value") else str(side)
        action = "BUY" if (side_val or "").lower() == "buy" else "SELL"
        type_val = getattr(order_type, "value", str(order_type or "")) if order_type is not None else "limit"
        is_market = (type_val or "").lower() == "market"
        try:
            # Round limit price to valid tick size (IBKR rejects excess decimals, e.g. 259.48001...)
            limit_price = round(float(price or 0), 2) if not is_market else 0.0
            async def _do_place_order():
                await self._ib.qualifyContractsAsync(contract)
                if is_market:
                    order = MarketOrder(action, quantity)
                else:
                    order = LimitOrder(action, quantity, limit_price)
                order.tif = "DAY"  # avoid IB error 10349: TIF set to DAY by preset (market and limit)
                order.account = acc
                trade = self._ib.placeOrder(contract, order)
                return str(trade.order.orderId) if trade and trade.order else None
            order_id = self._run_on_connection_loop(
                _do_place_order(), timeout_sec=25, label="place_order"
            )
            if order_id:
                logger.info("IBKR order placed: %s %s %s @ %s -> %s", order_id, action, quantity, symbol, price)
            return order_id
        except Exception as e:
            logger.warning("IBKR place_order %s: %s", symbol, e)
            return None

    def cancel_order(self, order_id: str, account: str | None = None) -> bool:
        """Cancel order. account: optional; filter by account. Returns True on success."""
        if not self.is_connected:
            logger.error("Not connected")
            return False
        if self.config.read_only:
            logger.error("IBKR read_only: orders disabled")
            return False
        try:
            oid = int(order_id)
            for trade in self._ib.trades():
                if trade.order.orderId != oid:
                    continue
                if account and (trade.order.account or "").strip() and (trade.order.account or "").strip() != (account or "").strip():
                    continue
                self._ib.cancelOrder(trade.order)
                logger.info("IBKR order cancelled: %s", order_id)
                return True
            logger.warning("IBKR cancel_order: order %s not found", order_id)
            return False
        except Exception as e:
            logger.warning("IBKR cancel_order %s: %s", order_id, e)
            return False

    def get_orders(self, status: str | None = None, account: str | None = None) -> list[dict]:
        """Get order list. status: optional filter. account: optional IB account id."""
        if not self.is_connected:
            logger.error("Not connected")
            return []
        acc = (account or "").strip() or None
        try:
            orders = []
            for trade in self._ib.trades():
                if acc and (trade.order.account or "").strip() and (trade.order.account or "").strip() != acc:
                    continue
                order_status = (trade.orderStatus.status if trade.orderStatus else "") or ""
                if status and status.upper() not in order_status.upper():
                    continue
                contract = trade.contract
                sym = _contract_to_symbol(contract) if contract else ""
                lmt = float(trade.order.lmtPrice or 0)
                if lmt >= 1e15 or lmt != lmt:
                    lmt = 0.0
                orders.append({
                    "order_id": str(trade.order.orderId),
                    "symbol": sym,
                    "name": getattr(contract, "symbol", "") if contract else "",
                    "side": "buy" if (trade.order.action or "").upper() == "BUY" else "sell",
                    "quantity": int(trade.order.totalQuantity or 0),
                    "price": round(lmt, 2),
                    "filled_qty": int(trade.orderStatus.filled if trade.orderStatus else 0),
                    "filled_avg_price": float(trade.orderStatus.avgFillPrice or 0) if trade.orderStatus else 0,
                    "status": order_status,
                    "created_at": str(trade.log[0].time) if trade.log else "",
                    "updated_at": str(trade.log[-1].time) if trade.log else "",
                })
            return orders
        except Exception as e:
            logger.warning("IBKR get_orders: %s", e)
            return []

    def get_deals(self, account: str | None = None) -> list[dict]:
        """Get fills/executions. account: optional IB account id."""
        if not self.is_connected:
            logger.error("Not connected")
            return []
        acc = (account or "").strip() or None
        try:
            deals = []
            for fill in self._ib.fills():
                if acc and fill.trade and (getattr(fill.trade.order, "account", "") or "").strip() != acc:
                    continue
                exec_ = fill.execution
                contract = fill.contract
                sym = _contract_to_symbol(contract) if contract else ""
                deals.append({
                    "deal_id": str(exec_.execId) if exec_ else "",
                    "order_id": str(fill.trade.order.orderId) if fill.trade else "",
                    "symbol": sym,
                    "name": getattr(contract, "symbol", "") if contract else "",
                    "side": "buy" if (exec_.side or "").upper() == "BUY" else "sell" if exec_ else "buy",
                    "quantity": int(exec_.shares or 0) if exec_ else 0,
                    "price": float(exec_.price or 0) if exec_ else 0,
                    "timestamp": str(exec_.time) if exec_ and getattr(exec_, "time", None) else "",
                })
            return deals
        except Exception as e:
            logger.warning("IBKR get_deals: %s", e)
            return []
