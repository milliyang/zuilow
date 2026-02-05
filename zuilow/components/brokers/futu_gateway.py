"""
Futu gateway: quotes and trading via Futu OpenD.

Requires: pip install futu-api, FutuOpenD running, API enabled in Futu app.
Docs: https://openapi.futunn.com/futu-api-doc/

Classes:
    FutuConfig       host, port, unlock_password, acc_id, rsa_file
    FutuMarket       Enum: HK, US
    FutuGateway      Connect to OpenD; quotes and trading

FutuGateway methods:
    .connect(host=None, port=None) -> bool
    .disconnect()
    .is_connected -> bool
    .get_quote(symbol: str) -> dict
    .get_history(symbol, start, end, interval) -> Optional[DataFrame]
    .place_order(symbol, side, quantity, price=None, acc_id=None, account_name=None) -> Optional[str]   (order_id)
    .get_account_info(acc_id=None) -> Optional[dict]
    .get_positions(acc_id=None) -> list[dict]
    .switch_account(acc_id: int) -> bool   (optional; prefer acc_id per call)
    .get_market_snapshot(symbols: list[str]) -> list[dict]

FutuGateway config:
    FutuConfig: host, port, unlock_password, acc_id, rsa_file. Trading env is per-account (accounts.yaml).

FutuGateway features:
    - HK and US markets
    - Real and simulated trading (TrdEnv)
    - Multi-account: pass acc_id per call (get_account_info, get_positions, place_order, etc.)

"""

from __future__ import annotations

import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Any
from enum import Enum

from zuilow.components.control import ctrl

from ..backtest.types import (
    Order, OrderSide, OrderType, OrderStatus,
    Position, Trade, Account
)

logger = logging.getLogger(__name__)

try:
    from futu import (
        OpenQuoteContext, OpenSecTradeContext,
        TrdSide, TrdEnv, OrderType as FutuOrderType,
        OrderStatus as FutuOrderStatus, TrdMarket,
        RET_OK, RET_ERROR,
        SysConfig,
    )
    HAS_FUTU = True
except ImportError:
    HAS_FUTU = False
    logger.warning("futu-api not installed: pip install futu-api")


class FutuMarket(Enum):
    """Futu market."""
    HK = "HK"
    US = "US"
    CN = "CN"
    SG = "SG"


@dataclass
class FutuConfig:
    """Futu config. Trading env is per-account (accounts.yaml), not here."""
    host: str = "10.147.17.99"
    port: int = 11111
    unlock_password: str = ""
    acc_id: int | None = None
    rsa_file: str = ""

    @classmethod
    def from_yaml(cls, path: str = None) -> "FutuConfig":
        """Load config from YAML."""
        import yaml
        from pathlib import Path
        
        if path is None:
            # Default config path
            path = Path(__file__).parent.parent.parent / "config" / "brokers" / "futu.yaml"
        else:
            path = Path(path)
        
        if not path.exists():
            logger.warning(f"Config file not found: {path}, using defaults")
            return cls()
        
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        futu_cfg = data.get('futu', {})
        return cls(
            host=futu_cfg.get('host', '10.147.17.99'),
            port=futu_cfg.get('port', 11111),
            unlock_password=futu_cfg.get('unlock_password', ''),
            acc_id=futu_cfg.get('acc_id'),
            rsa_file=futu_cfg.get('rsa_file', ''),
        )


class FutuGateway:
    """
    Futu gateway: quotes (get_quote, get_history), account (get_account_info, get_positions),
    trading (place_order, cancel_order). Requires futu-api and FutuOpenD.
 
    """
    
    def __init__(self, config: FutuConfig | None = None):
        if not HAS_FUTU:
            raise ImportError("Install futu-api: pip install futu-api")
        
        self.config = config or FutuConfig()
        
        self._quote_ctx: Any = None   # OpenQuoteContext
        self._trade_ctx: Any = None   # OpenSecTradeContext
        self._connected = False

        # Real account enabled for trading (name -> bool). Set at connect via set_real_account_names(); default disabled.
        self._acc_enabled: dict[str, bool] = {}
        
        self._acc_id: int | None = None
        self._acc_list: Any = None
        self._on_order_update: Callable[[dict], None] | None = None
        self._on_deal: Callable[[dict], None] | None = None

    # ========== Connection ==========

    def connect(self) -> bool:
        """Connect to FutuOpenD. Returns True on success."""
        try:
            if self.config.rsa_file:
                import os
                if os.path.exists(self.config.rsa_file):
                    SysConfig.set_init_rsa_file(self.config.rsa_file)
                    logger.info(f"RSA connect: {self.config.rsa_file}")
                else:
                    logger.warning(f"RSA file not found: {self.config.rsa_file}")
            self._quote_ctx = OpenQuoteContext(
                host=self.config.host,
                port=self.config.port,
            )
            self._trade_ctx = OpenSecTradeContext(
                host=self.config.host,
                port=self.config.port,
            )
            ret, data = self._trade_ctx.get_acc_list()
            if ret != RET_OK:
                logger.error(f"get_acc_list failed: {data}")
                return False
            self._acc_list = data
            print("=" * 50)
            print("Account list:")
            for _, row in data.iterrows():
                print(f"  - acc_id={row['acc_id']}, type={row.get('acc_type','')}, env={row.get('trd_env','')}")
            print("=" * 50)
            if self.config.acc_id is not None:
                acc_ids = [int(x) for x in data['acc_id'].values]
                if self.config.acc_id in acc_ids:
                    self._acc_id = int(self.config.acc_id)
                    logger.info(f"Using account: {self._acc_id}")
                else:
                    logger.error(f"Account ID {self.config.acc_id} not found. Valid: {acc_ids}")
                    return False
            else:
                # Prefer SIMULATE; trading env is per-account (accounts.yaml), not from futu.yaml
                want_env = "SIMULATE"
                connect_env = None
                for _, row in data.iterrows():
                    trd_env_raw = str(row.get('trd_env', ''))
                    trd_env = trd_env_raw.upper().replace('TRDENV.', '')
                    acc_type = str(row.get('acc_type', ''))
                    if want_env == trd_env or want_env in trd_env_raw:
                        self._acc_id = int(row['acc_id'])
                        connect_env = trd_env_raw
                        print(f"Auto-selected account: {self._acc_id} (env={trd_env_raw}, type={acc_type})")
                        break
                if self._acc_id is None:
                    for _, row in data.iterrows():
                        if 'SIMULATE' in str(row.get('trd_env', '')):
                            self._acc_id = int(row['acc_id'])
                            connect_env = str(row.get('trd_env', ''))
                            print(f"Using first SIMULATE account: {self._acc_id}")
                            break
                if self._acc_id is None:
                    first_row = data.iloc[0]
                    self._acc_id = int(first_row['acc_id'])
                    connect_env = str(first_row.get('trd_env', ''))
                    print(f"Using first account: {self._acc_id}")
                if connect_env and "REAL" in connect_env.upper() and self.config.unlock_password:
                    ret, data = self._trade_ctx.unlock_trade(self.config.unlock_password)
                    if ret != RET_OK:
                        logger.error(f"Unlock failed: {data}")
                        return False
            self._connected = True
            logger.info(f"Connected FutuOpenD ({self.config.host}:{self.config.port})")
            logger.info(f"Account: {self._acc_id}")
            return True
        except Exception as e:
            logger.error(f"Connect failed: {e}")
            return False

    def set_real_account_names(self, names: list[str]) -> None:
        """Set list of Real account names (from config). Enabled state is initialized to False for each; use set_account_enabled to allow trading."""
        self._acc_enabled = {n.strip(): False for n in names if (n or "").strip()}

    def set_account_enabled(self, name: str, enabled: bool) -> None:
        """Enable or disable a Real account for placing orders."""
        key = (name or "").strip()
        if key:
            self._acc_enabled[key] = bool(enabled)

    def is_account_enabled(self, name: str) -> bool:
        """True if account is enabled for trading. If name not in Real list, returns True (e.g. SIMULATE)."""
        key = (name or "").strip()
        if not key:
            return True
        return self._acc_enabled.get(key, True)

    def get_account_list(self) -> list[dict]:
        """Get all accounts (acc_id, acc_type, trd_env, card_num)."""
        if not self._connected:
            logger.error("Not connected")
            return []
        if not hasattr(self, '_acc_list') or self._acc_list is None:
            ret, data = self._trade_ctx.get_acc_list()
            if ret != RET_OK:
                return []
            self._acc_list = data
        accounts = []
        for _, row in self._acc_list.iterrows():
            accounts.append({
                "acc_id": int(row.get("acc_id")),
                "acc_type": str(row.get("acc_type", "")),
                "trd_env": str(row.get("trd_env", "")),
                "card_num": str(row.get("card_num", "")),
            })
        return accounts

    def switch_account(self, acc_id: int) -> bool:
        """Set default account for subsequent calls. Optional; prefer passing acc_id per call (place_order, get_account_info, etc.). Returns True on success."""
        if not self._connected:
            logger.error("Not connected")
            return False
        accounts = self.get_account_list()
        valid_ids = [a["acc_id"] for a in accounts]
        if acc_id not in valid_ids:
            logger.error(f"Account ID {acc_id} not found. Valid: {valid_ids}")
            return False
        self._acc_id = acc_id
        logger.info(f"Switched to account: {acc_id}")
        return True

    @property
    def current_account_id(self) -> int | None:
        """Current account ID."""
        return int(self._acc_id) if self._acc_id is not None else None

    def disconnect(self) -> None:
        """Disconnect."""
        if self._quote_ctx:
            self._quote_ctx.close()
        if self._trade_ctx:
            self._trade_ctx.close()
        self._connected = False
        logger.info("Disconnected FutuOpenD")

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ========== Quote ==========

    def get_quote(self, symbol: str) -> dict | None:
        """Get real-time quote. Returns dict with data_type: realtime."""
        if not self._connected:
            logger.error("Not connected")
            return None
        from futu import SubType
        ret, data = self._quote_ctx.subscribe([symbol], [SubType.QUOTE])
        if ret != RET_OK:
            logger.warning(f"Subscribe failed: {data}")
        ret, data = self._quote_ctx.get_stock_quote([symbol])
        if ret != RET_OK:
            logger.error(f"get_quote failed: {data}")
            return None
        
        if data.empty:
            return None
        
        row = data.iloc[0]
        return {
            "symbol": symbol,
            "name": str(row.get("name", "")),
            "price": float(row.get("last_price", 0)),
            "open": float(row.get("open_price", 0)),
            "high": float(row.get("high_price", 0)),
            "low": float(row.get("low_price", 0)),
            "prev_close": float(row.get("prev_close_price", 0)),
            "volume": int(row.get("volume", 0)),
            "turnover": float(row.get("turnover", 0)),
            "timestamp": ctrl.get_current_time_iso(),
            "source": "futu",
            "data_type": "realtime",
        }
    
    def get_history(
        self,
        symbol: str,
        start: str,
        end: str,
        ktype: str = "K_DAY",
    ) -> Any:
        """
        Get history OHLCV. Args: symbol, start (YYYY-MM-DD), end, ktype (K_DAY, K_WEEK, ...).
            
        Returns:
            DataFrame
        """
        if not self._connected:
            logger.error("Not connected")
            return None
        
        from futu import KLType
        ktype_map = {
            "K_DAY": KLType.K_DAY,
            "K_WEEK": KLType.K_WEEK,
            "K_MON": KLType.K_MON,
            "K_1M": KLType.K_1M,
            "K_5M": KLType.K_5M,
            "K_15M": KLType.K_15M,
            "K_30M": KLType.K_30M,
            "K_60M": KLType.K_60M,
        }
        
        ret, data, _ = self._quote_ctx.request_history_kline(
            symbol,
            start=start,
            end=end,
            ktype=ktype_map.get(ktype, KLType.K_DAY),
        )
        
        if ret != RET_OK:
            logger.error(f"get_history failed: {data}")
            return None
        
        return data
    
    # ========== Account ==========

    def _resolve_trd_env(self, trd_env=None):
        """Resolve trd_env for API call. trd_env from account (accounts.yaml); when None use default SIMULATE."""
        if trd_env is None:
            return TrdEnv.SIMULATE
        if HAS_FUTU and isinstance(trd_env, str):
            return TrdEnv.SIMULATE if trd_env.upper() == "SIMULATE" else TrdEnv.REAL
        return trd_env

    def get_account_info(self, acc_id: int | None = None, trd_env=None) -> dict | None:
        """Get account info. acc_id/trd_env: optional; trd_env from account config (REAL/SIMULATE)."""
        if not self._connected:
            logger.error("Not connected")
            return None
        acc = acc_id if acc_id is not None else self._acc_id
        if acc is None:
            logger.error("No account specified and no default account")
            return None
        env = self._resolve_trd_env(trd_env)
        ret, data = self._trade_ctx.accinfo_query(
            trd_env=env,
            acc_id=acc,
        )
        
        if ret != RET_OK:
            logger.error(f"get_account_info failed: {data}")
            return None
        
        if data.empty:
            return None
        
        row = data.iloc[0]
        return {
            "cash": float(row.get("cash", 0)),
            "total_assets": float(row.get("total_assets", 0)),
            "market_value": float(row.get("market_val", 0)),
            "frozen_cash": float(row.get("frozen_cash", 0)),
            "available_funds": float(row.get("avl_withdrawal_cash", 0)),
            "currency": str(row.get("currency", "HKD")),
            "power": float(row.get("power", 0)),
        }
    
    def get_positions(self, acc_id: int | None = None, trd_env=None) -> list[dict]:
        """Get positions. acc_id/trd_env: optional; trd_env from account config (REAL/SIMULATE)."""
        if not self._connected:
            logger.error("Not connected")
            return []
        acc = acc_id if acc_id is not None else self._acc_id
        if acc is None:
            logger.error("No account specified and no default account")
            return []
        env = self._resolve_trd_env(trd_env)
        ret, data = self._trade_ctx.position_list_query(
            trd_env=env,
            acc_id=acc,
        )
        
        if ret != RET_OK:
            logger.error(f"get_positions failed: {data}")
            return []
        
        positions = []
        for _, row in data.iterrows():
            # pl_ratio from Futu is already % (e.g. -4.90)
            pl_ratio = row.get("pl_ratio", 0)
            positions.append({
                "symbol": str(row.get("code", "")),
                "name": str(row.get("stock_name", "")),
                "quantity": int(row.get("qty", 0)),
                "available": int(row.get("can_sell_qty", 0)),
                "avg_price": float(row.get("cost_price", 0)),
                "current_price": float(row.get("nominal_price", 0)),
                "market_value": float(row.get("market_val", 0)),
                "pnl": float(row.get("pl_val", 0)),
                "pnl_pct": float(pl_ratio) if pl_ratio else 0.0,
            })
        
        return positions
    
    def get_position(self, symbol: str, acc_id: int | None = None, trd_env=None) -> dict | None:
        """Get single position. acc_id/trd_env: optional."""
        positions = self.get_positions(acc_id=acc_id, trd_env=trd_env)
        for pos in positions:
            if pos["symbol"] == symbol:
                return pos
        return None
    
    # ========== Trading ==========

    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: int,
        price: float | None = None,
        order_type: OrderType = OrderType.LIMIT,
        acc_id: int | None = None,
        trd_env=None,
        account_name: str | None = None,
    ) -> str | None:
        """Place order. acc_id/trd_env: optional; account_name used to check enabled (Real accounts). Returns order_id or None."""
        if not self._connected:
            logger.error("Not connected")
            return None
        if account_name and not self.is_account_enabled(account_name):
            logger.error("Account %s is disabled for trading", account_name)
            return None
        acc = acc_id if acc_id is not None else self._acc_id
        if acc is None:
            logger.error("No account specified and no default account")
            return None
        trd_side = TrdSide.BUY if side == OrderSide.BUY else TrdSide.SELL
        
        futu_order_type = FutuOrderType.NORMAL
        if order_type == OrderType.MARKET:
            futu_order_type = FutuOrderType.MARKET
        
        env = self._resolve_trd_env(trd_env)
        ret, data = self._trade_ctx.place_order(
            price=price or 0,
            qty=quantity,
            code=symbol,
            trd_side=trd_side,
            order_type=futu_order_type,
            trd_env=env,
            acc_id=acc,
        )
        
        if ret != RET_OK:
            msg = str(data) if data is not None else "Order failed"
            logger.error("place_order failed: %s", msg)
            raise RuntimeError(msg)
        
        order_id = str(data.iloc[0]['order_id'])
        logger.info(f"Order placed: {order_id} {side.value} {quantity} {symbol} @ {price}")
        
        return order_id
    
    def cancel_order(self, order_id: str, acc_id: int | None = None, trd_env=None) -> bool:
        """Cancel order. acc_id/trd_env: optional; trd_env from account config (REAL/SIMULATE). Returns True on success."""
        if not self._connected:
            logger.error("Not connected")
            return False
        acc = acc_id if acc_id is not None else self._acc_id
        if acc is None:
            logger.error("No account specified and no default account")
            return False
        from futu import ModifyOrderOp
        env = self._resolve_trd_env(trd_env)
        ret, data = self._trade_ctx.modify_order(
            modify_order_op=ModifyOrderOp.CANCEL,
            order_id=order_id,
            qty=0,
            price=0,
            trd_env=env,
            acc_id=acc,
        )
        
        if ret != RET_OK:
            logger.error(f"cancel_order failed: {data}")
            return False
        
        logger.info(f"Order cancelled: {order_id}")
        return True
    
    def get_orders(self, status: str | None = None, acc_id: int | None = None, trd_env=None) -> list[dict]:
        """Get order list. status: filter. acc_id/trd_env: optional; trd_env from account config (REAL/SIMULATE)."""
        if not self._connected:
            logger.error("Not connected")
            return []
        acc = acc_id if acc_id is not None else self._acc_id
        if acc is None:
            logger.error("No account specified and no default account")
            return []
        env = self._resolve_trd_env(trd_env)
        ret, data = self._trade_ctx.order_list_query(
            trd_env=env,
            acc_id=acc,
        )
        
        if ret != RET_OK:
            logger.error(f"get_orders failed: {data}")
            return []
        
        orders = []
        for _, row in data.iterrows():
            order_status = str(row.get("order_status", ""))
            
            if status and status not in order_status:
                continue
            
            orders.append({
                "order_id": str(row.get("order_id", "")),
                "symbol": str(row.get("code", "")),
                "name": str(row.get("stock_name", "")),
                "side": "buy" if str(row.get("trd_side")) == "BUY" else "sell",
                "quantity": int(row.get("qty", 0)),
                "price": float(row.get("price", 0)),
                "filled_qty": int(row.get("dealt_qty", 0)),
                "filled_avg_price": float(row.get("dealt_avg_price", 0)),
                "status": order_status,
                "created_at": str(row.get("create_time", "")),
                "updated_at": str(row.get("updated_time", "")),
            })
        
        return orders
    
    def get_order(self, order_id: str, acc_id: int | None = None, trd_env=None) -> dict | None:
        """Get single order. acc_id/trd_env: optional."""
        orders = self.get_orders(acc_id=acc_id, trd_env=trd_env)
        for order in orders:
            if order["order_id"] == order_id:
                return order
        return None
    
    def get_deals(self, acc_id: int | None = None, trd_env=None) -> list[dict]:
        """Get deals. acc_id/trd_env: optional; trd_env from account config (REAL/SIMULATE)."""
        if not self._connected:
            logger.error("Not connected")
            return []
        acc = acc_id if acc_id is not None else self._acc_id
        if acc is None:
            logger.error("No account specified and no default account")
            return []
        env = self._resolve_trd_env(trd_env)
        ret, data = self._trade_ctx.deal_list_query(
            trd_env=env,
            acc_id=acc,
        )
        
        if ret != RET_OK:
            logger.error(f"get_deals failed: {data}")
            return []
        
        deals = []
        for _, row in data.iterrows():
            deals.append({
                "deal_id": str(row.get("deal_id", "")),
                "order_id": str(row.get("order_id", "")),
                "symbol": str(row.get("code", "")),
                "name": str(row.get("stock_name", "")),
                "side": "buy" if str(row.get("trd_side")) == "BUY" else "sell",
                "quantity": int(row.get("qty", 0)),
                "price": float(row.get("price", 0)),
                "timestamp": str(row.get("create_time", "")),
            })
        
        return deals
    
    # ========== Convenience ==========

    def buy(
        self,
        symbol: str,
        quantity: int,
        price: float | None = None,
    ) -> str | None:
        """Buy."""
        return self.place_order(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            price=price,
            order_type=OrderType.LIMIT if price else OrderType.MARKET,
        )
    
    def sell(
        self,
        symbol: str,
        quantity: int,
        price: float | None = None,
    ) -> str | None:
        """Sell."""
        return self.place_order(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            price=price,
            order_type=OrderType.LIMIT if price else OrderType.MARKET,
        )
    
    def close_position(self, symbol: str, price: float | None = None) -> str | None:
        """Close position."""
        pos = self.get_position(symbol)
        if not pos or pos["available"] <= 0:
            logger.warning(f"No sellable position: {symbol}")
            return None
        
        return self.sell(symbol, int(pos["available"]), price)
    
    # ========== Callbacks ==========

    def on_order_update(self, callback: Callable[[dict], None]) -> None:
        """Set order update callback."""
        self._on_order_update = callback

    def on_deal(self, callback: Callable[[dict], None]) -> None:
        """Set deal callback."""
        self._on_deal = callback

    # ========== Context manager ==========
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


# Backward compatibility
FutuBroker = FutuGateway
