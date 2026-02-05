"""
PPT gateway: quote/history from DMS (independent DmsSource from ppt.yaml), trading (account/positions/order) from PPT.

Same interface as FutuGateway and IbkrGateway so it can be used as a broker in Live.

Classes:
    PptConfig   base_url, webhook_token, dms_config (from ppt.yaml dms section)
    PptGateway  connect to PPT (health check); get_quote/get_history via own DmsSource; account/positions/order via PPT

PptGateway methods:
    .connect() -> bool   True only when both PPT and DMS are reachable (full broker connected).
    .disconnect()        Really disconnects; no auto-reconnect until connect() is called again.
    .is_connected -> bool
    .get_quote(symbol: str) -> dict | None
    .get_history(symbol, start, end, ktype) -> Optional[DataFrame]
    .get_account_info(account: str | None = None) -> dict | None
    .get_positions(account: str | None = None) -> list[dict]
    .get_orders(account: str | None = None, limit: int = 50) -> list[dict]
    .place_order(symbol, side, quantity, price=None, order_type=None, account=None) -> str | None
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests

from zuilow.components.control import ctrl

logger = logging.getLogger(__name__)


@dataclass
class PptConfig:
    """PPT broker config (base_url, webhook_token, dms_config). Load from config/brokers/ppt.yaml."""
    base_url: str = "http://localhost:11182"
    webhook_token: str = ""
    dms_config: dict | None = None  # dms section from ppt.yaml for independent DmsSource

    @classmethod
    def from_yaml(cls, path: str | None = None) -> "PptConfig":
        """Load from config/brokers/ppt.yaml (ppt + dms sections)."""
        if path is None:
            path = Path(__file__).resolve().parent.parent.parent / "config" / "brokers" / "ppt.yaml"
        else:
            path = Path(path)
        if not path.exists():
            logger.warning("Config file not found: %s, using defaults", path)
            return cls()
        try:
            import yaml
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            ppt_cfg = data.get("ppt", {})
            base = (ppt_cfg.get("base_url") or "").strip().rstrip("/") or "http://localhost:11182"
            import os
            if os.environ.get("PAPER_TRADE_URL"):
                base = (os.environ.get("PAPER_TRADE_URL") or "").strip().rstrip("/")
            token = (ppt_cfg.get("webhook_token") or "").strip()
            if os.environ.get("WEBHOOK_TOKEN"):
                token = (os.environ.get("WEBHOOK_TOKEN") or "").strip()
            dms_cfg = data.get("dms")
            if isinstance(dms_cfg, dict):
                dms_config = dict(dms_cfg)
            else:
                dms_config = None
            return cls(base_url=base, webhook_token=token, dms_config=dms_config)
        except Exception as e:
            logger.debug("PptConfig from_yaml: %s", e)
            return cls()


# ktype (Futu-style) -> interval for DataSourceManager
_KTYPE_TO_INTERVAL = {
    "K_DAY": "1d",
    "K_WEEK": "1wk",
    "K_MON": "1mo",
    "K_1M": "1m",
    "K_5M": "5m",
    "K_15M": "15m",
    "K_30M": "30m",
    "K_60M": "60m",
}


class PptGateway:
    """
    PPT broker: quote/history from own DmsSource (ppt.yaml dms section), account/positions/order from PPT service.
    Same interface as FutuGateway / IbkrGateway. Connected only when both PPT and DMS are reachable.
    """

    def __init__(self, config: PptConfig | None = None):
        self.config = config or PptConfig.from_yaml()
        self._connected = False
        self._ppt_ok = False  # PPT service reachable
        self._dms_ok = False  # DMS (data source) reachable
        self._timeout = 10
        # PPT base URL normalized once (strip, no trailing slash)
        self._base = (self.config.base_url or "").strip().rstrip("/")
        # Independent DmsSource from config/brokers/ppt.yaml dms section (no global DataSourceManager)
        self._dms_source: Any = None
        if self.config.dms_config:
            try:
                from zuilow.components.datasource.source.dms_source import DmsSource
                self._dms_source = DmsSource(self.config.dms_config)
            except Exception as e:
                logger.debug("PptGateway DmsSource init: %s", e)

    # ========== Connection ==========

    def connect(self) -> bool:
        """Connect broker: both PPT and DMS must be reachable. Sets _connected=True only when both ok."""
        ppt_ok = False
        if self._base:
            try:
                r = requests.get(f"{self._base}/api/health", timeout=3)
                ppt_ok = r.status_code == 200
            except Exception as e:
                logger.debug("PPT connect (health): %s", e)
        self._ppt_ok = ppt_ok

        dms_ok = False
        if self._dms_source:
            try:
                dms_ok = self._dms_source.connect()
            except Exception as e:
                logger.debug("PPT connect (DMS): %s", e)
        self._dms_ok = dms_ok

        self._connected = self._ppt_ok and self._dms_ok
        return self._connected

    def disconnect(self) -> None:
        """Really disconnect: broker is disabled until connect() is called again."""
        self._connected = False
        self._ppt_ok = False
        self._dms_ok = False
        if self._dms_source:
            try:
                self._dms_source.disconnect()
            except Exception:
                pass

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ========== Quote / History (DMS) ==========

    def get_quote(self, symbol: str) -> dict | None:
        """Get quote from own DmsSource (ppt.yaml dms). Returns dict with price, Open, High, Low, Close, etc."""
        if not self.is_connected or not self._dms_source:
            return None
        as_of = ctrl.get_time_dt() if ctrl.is_sim_mode() else None
        try:
            q = self._dms_source.get_quote(symbol, as_of=as_of)
        except Exception as e:
            logger.debug("PptGateway get_quote %s: %s", symbol, e)
            return None
        if not q or (q.get("error") and not q.get("price") and not q.get("Close")):
            return None
        price = float(q.get("price") or q.get("Close") or 0)
        if price <= 0:
            return None
        return {
            "symbol": symbol,
            "name": q.get("name", symbol),
            "price": price,
            "open": float(q.get("Open", q.get("open", 0)) or 0),
            "high": float(q.get("High", q.get("high", 0)) or 0),
            "low": float(q.get("Low", q.get("low", 0)) or 0),
            "prev_close": float(q.get("Close", price) or price),
            "volume": int(q.get("Volume", q.get("volume", 0)) or 0),
            "turnover": 0,
            "timestamp": ctrl.get_current_time_iso(),
            "source": "ppt",
            "data_type": "dms",
        }

    def get_history(
        self,
        symbol: str,
        start: str,
        end: str,
        ktype: str = "K_DAY",
    ) -> Any:
        """Get history from own DmsSource (ppt.yaml dms). Returns DataFrame with Open, High, Low, Close, Volume."""
        if not self.is_connected or not self._dms_source:
            return None
        from datetime import datetime as dt
        interval = _KTYPE_TO_INTERVAL.get(ktype, "1d")
        try:
            start_dt = dt.strptime(start[:10], "%Y-%m-%d") if start else dt.now()
            end_dt = dt.strptime(end[:10], "%Y-%m-%d") if end else dt.now()
        except ValueError:
            return None
        as_of = ctrl.get_time_dt() if ctrl.is_sim_mode() else None
        try:
            df = self._dms_source.get_history(symbol, start_dt, end_dt, interval, as_of=as_of)
        except Exception as e:
            logger.debug("PptGateway get_history %s: %s", symbol, e)
            return None
        return df

    # ========== Account / Positions / Order (PPT) ==========

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.config.webhook_token:
            h["X-Webhook-Token"] = self.config.webhook_token
        return h

    def _get(self, path: str, params: dict | None = None) -> dict | None:
        if not self._base:
            return None
        url = self._base + path if path.startswith("/") else self._base + "/" + path
        try:
            r = requests.get(url, params=params, headers=self._headers(), timeout=self._timeout)
            return r.json() if r.ok else None
        except (requests.ConnectionError, requests.Timeout) as e:
            logger.debug("PptGateway GET %s (connection error): %s", path, e)
            self.disconnect()
            return None
        except Exception as e:
            logger.debug("PptGateway GET %s: %s", path, e)
            return None

    def _post(self, path: str, json_body: dict) -> dict | None:
        if not self._base:
            return None
        url = self._base + path if path.startswith("/") else self._base + "/" + path
        try:
            r = requests.post(url, json=json_body, headers=self._headers(), timeout=self._timeout)
            return r.json() if r.ok else None
        except (requests.ConnectionError, requests.Timeout) as e:
            logger.debug("PptGateway POST %s (connection error): %s", path, e)
            self.disconnect()
            return None
        except Exception as e:
            logger.debug("PptGateway POST %s: %s", path, e)
            return None

    def get_account_raw(self, account: str | None = None, require_connected: bool = True) -> dict | None:
        """Raw PPT /api/account response. When require_connected=True (default), returns None if not connected."""
        if require_connected and not self.is_connected:
            return None
        params = {"account": account.strip()} if (account and (account or "").strip()) else None
        return self._get("/api/account", params=params)

    def get_positions_raw(self, account: str | None = None) -> dict | None:
        """Raw PPT /api/positions response (dict with 'positions' key). Returns None when not connected or on error."""
        if not self.is_connected:
            return None
        params = {"account": account.strip()} if (account and (account or "").strip()) else None
        return self._get("/api/positions", params=params)

    def get_accounts(self) -> dict | None:
        """Raw PPT /api/accounts (list accounts + current). No is_connected check so Status page can probe connection."""
        return self._get("/api/accounts")

    def get_trades(self, account: str | None = None, page: int = 1, limit: int = 20) -> dict | None:
        """Raw PPT /api/trades response. Returns None when not connected or on error."""
        if not self.is_connected:
            return None
        params: dict = {"page": page, "limit": limit}
        if account and (account or "").strip():
            params["account"] = account.strip()
        return self._get("/api/trades", params=params)

    def get_account_info(self, account: str | None = None) -> dict | None:
        """Get account info from PPT. account: optional; passed as query param (no switch)."""
        if not self.is_connected:
            return None
        params = {"account": account.strip()} if (account and (account or "").strip()) else None
        data = self._get("/api/account", params=params)
        if not data:
            return None
        total_value = float(data.get("total_value", 0))
        cash = float(data.get("cash", 0))
        position_value = float(data.get("position_value", 0))
        return {
            "cash": cash,
            "total_assets": total_value,
            "market_value": position_value,
            "total_value": total_value,
        }

    def get_positions(self, account: str | None = None) -> list[dict]:
        """Get positions from PPT. account: optional; passed as query param (no switch)."""
        if not self.is_connected:
            return []
        params = {"account": account.strip()} if (account and (account or "").strip()) else None
        data = self._get("/api/positions", params=params)
        if not data or not isinstance(data.get("positions"), list):
            return []
        out = []
        for p in data["positions"]:
            out.append({
                "symbol": p.get("symbol", ""),
                "quantity": int(p.get("qty", 0)),
                "avg_price": float(p.get("avg_price", 0)),
                "current_price": float(p.get("current_price", p.get("avg_price", 0))),
                "market_value": float(p.get("market_value", p.get("cost", 0))),
                "pnl": float(p.get("pnl", 0)),
                "pnl_pct": float(p.get("pnl_pct", 0)),
            })
        return out

    def get_orders(self, account: str | None = None, limit: int = 50) -> list[dict]:
        """Get order history from PPT. account: optional; passed as query param (no switch)."""
        if not self.is_connected:
            return []
        params = {"limit": limit}
        if account and (account or "").strip():
            params["account"] = account.strip()
        data = self._get("/api/orders", params=params)
        if not data or not isinstance(data.get("orders"), list):
            return []
        out = []
        for o in data["orders"][:limit]:
            out.append({
                "order_id": str(o.get("id", "")),
                "id": o.get("id"),
                "symbol": o.get("symbol", ""),
                "side": o.get("side", ""),
                "quantity": int(o.get("qty", 0)),
                "qty": int(o.get("qty", 0)),
                "price": float(o.get("price", 0)),
                "value": float(o.get("value", 0)),
                "status": o.get("status", ""),
                "created_at": o.get("time", ""),
                "time": o.get("time", ""),
                "source": o.get("source", "webhook"),
            })
        return out

    def place_order(
        self,
        symbol: str,
        side: Any,
        quantity: int,
        price: float | None = None,
        order_type: Any = None,
        account: str | None = None,
    ) -> str | None:
        """Place order via PPT webhook. Returns order_id or None."""
        if not self.is_connected or not self._base:
            return None
        side_val = getattr(side, "value", side) if hasattr(side, "value") else str(side)
        payload = {"symbol": symbol, "side": side_val.lower(), "qty": quantity, "price": price or 0}
        if account and (account or "").strip():
            payload["account"] = account.strip()
        headers = self._headers()
        if ctrl.is_sim_mode():
            t = ctrl.get_time_iso() or ""
            if t:
                headers["X-Simulation-Time"] = t
        try:
            r = requests.post(f"{self._base}/api/webhook", json=payload, headers=headers, timeout=self._timeout)
            if r.status_code != 200:
                logger.warning("PptGateway place_order: %s %s", r.status_code, r.text)
                return None
            data = r.json()
            order = data.get("order") if isinstance(data.get("order"), dict) else data
            if order and order.get("id") is not None:
                return str(order["id"])
            return None
        except Exception as e:
            logger.warning("PptGateway place_order: %s", e)
            return None
