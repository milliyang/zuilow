"""
Market service: broker-only (Futu, IBKR, or PptGateway). No datasource (YFinance/InfluxDB/DMS).

Live market data is provided by the broker. For backtest/sim or when broker is not
connected, use DataSourceManager via /api/market/quote fallback.

PptGateway (ppt_gateway.py): quote/history from DMS, trading from PPT; same interface as Futu/IBKR.
Routes route by account type (paper -> PptGateway, futu/ibkr -> Futu/IBKR). This module may be removed
later in favour of using gateways directly by account type.

Classes:
    MarketService   Broker-only; get_quote / get_history from Futu, IbkrGateway, or PptGateway

MarketService methods:
    .get_quote(symbol: str) -> dict
    .get_history(symbol, start, end, ktype) -> Optional[DataFrame]
    .futu_connected -> bool   (True when any broker is connected)

Functions:
    get_market_service() -> MarketService
    set_market_service(service: Optional[MarketService]) -> None
"""

from __future__ import annotations

import logging
from typing import Optional, Any

from zuilow.components.control import ctrl

logger = logging.getLogger(__name__)


class MarketService:
    """
    Market service: broker-only. Quote and history from Futu or IBKR when connected.
    """

    def __init__(self, broker: Any = None, futu_broker: Any = None):
        """broker: FutuGateway or IbkrGateway. futu_broker: backward compat alias for broker."""
        self._broker = broker if broker is not None else futu_broker

    def get_quote(self, symbol: str, prefer_db: bool = False) -> dict:
        """Get quote from broker (Futu or IBKR). prefer_db ignored; broker-only."""
        if self._broker and getattr(self._broker, "is_connected", False):
            try:
                quote = self._broker.get_quote(symbol)
                if quote and quote.get("error") is None:
                    return quote
            except Exception as e:
                logger.warning("Broker get_quote failed: %s", e)
        return {
            "symbol": (symbol or "").upper(),
            "error": "Broker not connected or no data",
            "timestamp": ctrl.get_current_time_iso(),
        }

    def get_history(
        self,
        symbol: str,
        start: str,
        end: str,
        ktype: str = "K_DAY",
        prefer_db: bool = False,
    ) -> Any:
        """Get history from broker (Futu or IBKR). prefer_db ignored; broker-only."""
        if not (self._broker and getattr(self._broker, "is_connected", False)):
            return None
        try:
            data = self._broker.get_history(symbol, start, end, ktype)
            if data is not None and (not hasattr(data, "empty") or not data.empty):
                return data
        except Exception as e:
            logger.warning("Broker get_history failed: %s", e)
        return None

    @property
    def futu_connected(self) -> bool:
        """Whether broker (Futu or IBKR) is connected."""
        return self._broker is not None and getattr(self._broker, "is_connected", False)


_market_service: MarketService | None = None


def get_market_service() -> MarketService:
    """Get global market service."""
    global _market_service
    if _market_service is None:
        _market_service = MarketService()
    return _market_service


def set_market_service(service: MarketService) -> None:
    """Set global market service."""
    global _market_service
    _market_service = service
