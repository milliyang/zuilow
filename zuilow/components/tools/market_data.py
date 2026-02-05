"""
Market data utilities: quote, history, info via datasource manager.

Functions are @tool for sAI agent. Uses datasource manager (yfinance, InfluxDB, etc.)
to fetch data; connect_all() if primary not connected.

Functions:
    get_stock_quote(symbol: str) -> dict
        Returns: quote dict (current_price, open, high, low, volume, ...)
    get_stock_history(symbol: str, period: str = "1mo", interval: str = "1d") -> dict
        Returns: history dict (data list or DataFrame-like)
    get_stock_info(symbol: str) -> dict
        Returns: info dict (name, sector, ...)
"""
from typing import Optional
from datetime import datetime, timedelta
import sys
sys.path.insert(0, "../..")

from sai.tools import tool
from ..datasource import get_manager, DataSourceManager


def _get_manager() -> DataSourceManager:
    """Get data source manager."""
    return get_manager()


@tool(description="Get stock real-time quote")
def get_stock_quote(symbol: str) -> dict:
    """
    Get stock real-time quote.

    Args:
        symbol: Symbol, e.g. AAPL, TSLA, MSFT

    Returns:
        Quote info
    """
    manager = _get_manager()
    
    if not manager.primary or not manager.primary.is_connected:
        manager.connect_all()
    
    return manager.get_quote(symbol)


@tool(description="Get stock history")
def get_stock_history(
    symbol: str,
    period: str = "1mo",
    interval: str = "1d",
) -> dict:
    """
    Get stock history.

    Args:
        symbol: Symbol
        period: Period: 1d, 5d, 1mo, 3mo, 6mo, 1y
        interval: Interval: 1d, 1h, 5m

    Returns:
        History summary
    """
    manager = _get_manager()

    if not manager.primary or not manager.primary.is_connected:
        manager.connect_all()

    from zuilow.components.control.ctrl import get_current_dt
    end_date = get_current_dt()
    period_map = {
        "1d": 1,
        "5d": 5,
        "1mo": 30,
        "3mo": 90,
        "6mo": 180,
        "1y": 365,
    }
    days = period_map.get(period, 30)
    start_date = end_date - timedelta(days=days)
    
    data = manager.get_history(symbol, start_date, end_date, interval)
    
    if data is None or data.empty:
        return {
            "symbol": symbol.upper(),
            "error": "No data",
        }

    closes = data["Close"].tolist()
    
    return {
        "symbol": symbol.upper(),
        "period": period,
        "interval": interval,
        "count": len(data),
        "latest": {
            "date": str(data.index[-1]),
            "close": round(closes[-1], 2),
        },
        "earliest": {
            "date": str(data.index[0]),
            "close": round(closes[0], 2),
        },
        "high": round(max(closes), 2),
        "low": round(min(closes), 2),
        "avg": round(sum(closes) / len(closes), 2),
        "change": round(closes[-1] - closes[0], 2),
        "change_pct": f"{(closes[-1] - closes[0]) / closes[0] * 100:+.2f}%",
    }


@tool(description="Get stock info")
def get_stock_info(symbol: str) -> dict:
    """
    Get stock basic info.

    Args:
        symbol: Symbol

    Returns:
        Company basic info
    """
    manager = _get_manager()

    if not manager.primary or not manager.primary.is_connected:
        manager.connect_all()

    yf_source = manager.get_source("yfinance")
    if yf_source and hasattr(yf_source, "get_info"):
        return yf_source.get_info(symbol)

    return manager.get_quote(symbol)


# Aliases
get_price = get_stock_quote
get_history = get_stock_history
get_info = get_stock_info
