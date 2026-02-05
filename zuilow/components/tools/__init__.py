"""
ZuiLow tools: market data and technical indicators.

Uses datasource manager to fetch data. Functions are also registered as @tool for sAI agent.

Functions:
    get_stock_quote(symbol: str) -> dict
    get_stock_history(symbol: str, period: str = "1mo", interval: str = "1d") -> dict
    get_stock_info(symbol: str) -> dict
    calc_rsi(closes: list[float], period: int = 14) -> dict
    calc_macd(closes, fast=12, slow=26, signal=9) -> dict
    calc_moving_average(closes: list[float], period: int) -> list[float]
    calc_bollinger_bands(closes, period=20, num_std=2) -> dict
"""

from .market_data import (
    get_stock_quote,
    get_stock_history,
    get_stock_info,
)

from .indicators import (
    calc_rsi,
    calc_macd,
    calc_moving_average,
    calc_bollinger_bands,
)

__all__ = [
    "get_stock_quote",
    "get_stock_history",
    "get_stock_info",
    "calc_rsi",
    "calc_macd",
    "calc_moving_average",
    "calc_bollinger_bands",
]
