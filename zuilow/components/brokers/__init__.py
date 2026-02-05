"""
ZuiLow brokers: Futu, IBKR, and PPT gateways; unified market service.

Classes:
    FutuGateway, FutuConfig, FutuMarket   Futu OpenD; see futu_gateway.py
    IbkrGateway, IbkrConfig               IBKR TWS/Gateway; see ibkr_gateway.py
    PptGateway, PptConfig                 Quote/history from DMS, trading from PPT; see ppt_gateway.py
    MarketService, get_market_service, set_market_service   Broker-agnostic; see market_service.py
    FutuBroker   Alias for FutuGateway (backward compat)
"""

from .futu_gateway import FutuGateway, FutuConfig, FutuMarket
from .ppt_gateway import PptGateway, PptConfig
from .market_service import MarketService, get_market_service, set_market_service

# Lazy-load IBKR so ib_insync (and its eventkit/asyncio) are not imported when only Futu is used.
# Importing ib_insync in a Flask worker thread can raise RuntimeError (no event loop).
_IbkrGateway = None
_IbkrConfig = None


def __getattr__(name: str):
    if name == "IbkrGateway":
        global _IbkrGateway
        if _IbkrGateway is None:
            try:
                from .ibkr_gateway import IbkrGateway as _G
                _IbkrGateway = _G
            except Exception:
                _IbkrGateway = False  # mark failed so we don't retry every time
        return _IbkrGateway if _IbkrGateway is not False else None
    if name == "IbkrConfig":
        global _IbkrConfig
        if _IbkrConfig is None:
            try:
                from .ibkr_gateway import IbkrConfig as _C
                _IbkrConfig = _C
            except Exception:
                _IbkrConfig = False
        return _IbkrConfig if _IbkrConfig is not False else None
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


FutuBroker = FutuGateway  # backward compat

__all__ = [
    "FutuGateway",
    "FutuConfig",
    "FutuMarket",
    "IbkrGateway",
    "IbkrConfig",
    "PptGateway",
    "PptConfig",
    "MarketService",
    "get_market_service",
    "set_market_service",
    "FutuBroker",
]
