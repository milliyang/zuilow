"""
Technical indicator utilities: RSI, MACD, MA, Bollinger Bands.

Uses datasource manager to fetch close prices when used as @tool (sAI). Otherwise
caller passes closes list.

Functions:
    calc_rsi(closes: list[float], period: int = 14) -> dict
        Returns: rsi (float), signal ("Buy"|"Sell"|"Hold"), level
    calc_macd(closes, fast=12, slow=26, signal=9) -> dict
        Returns: macd_line, signal_line, histogram, trend_signal
    calc_moving_average(closes: list[float], period: int) -> list[float]
        Returns: SMA values
    calc_bollinger_bands(closes, period=20, num_std=2) -> dict
        Returns: upper, middle, lower bands; position ("over"|"under"|"mid")
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


def _get_closes(symbol: str, period: str = "3mo") -> list[float]:
    """Get close price list."""
    manager = _get_manager()

    if not manager.primary or not manager.primary.is_connected:
        manager.connect_all()

    from zuilow.components.control.ctrl import get_current_dt
    end_date = get_current_dt()
    period_map = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365}
    days = period_map.get(period, 90)
    start_date = end_date - timedelta(days=days)

    data = manager.get_history(symbol, start_date, end_date, "1d")

    if data is None or data.empty:
        raise ValueError(f"Cannot get data for {symbol}")

    return data["Close"].tolist()


def _calc_ema(data: list[float], period: int) -> list[float]:
    """Compute EMA."""
    if len(data) < period:
        return []
    
    multiplier = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    
    for price in data[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    
    return ema


@tool(description="Compute RSI indicator")
def calc_rsi(symbol: str, period: int = 14) -> dict:
    """
    Compute RSI (Relative Strength Index).

    Args:
        symbol: Symbol
        period: RSI period, default 14

    Returns:
        RSI value and interpretation
    """
    try:
        closes = _get_closes(symbol, "3mo")

        if len(closes) < period + 1:
            return {"symbol": symbol, "error": "Insufficient data"}

        changes = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        gains = [max(c, 0) for c in changes]
        losses = [abs(min(c, 0)) for c in changes]
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period

        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        rsi = round(rsi, 2)

        if rsi > 70:
            interpretation = "Overbought, possible pullback"
            signal = "Sell signal"
        elif rsi < 30:
            interpretation = "Oversold, possible bounce"
            signal = "Buy signal"
        else:
            interpretation = "Neutral"
            signal = "Hold"

        return {
            "symbol": symbol.upper(),
            "indicator": "RSI",
            "period": period,
            "value": rsi,
            "interpretation": interpretation,
            "signal": signal,
            "latest_price": round(closes[-1], 2),
        }
    except Exception as e:
        return {"symbol": symbol, "indicator": "RSI", "error": str(e)}


@tool(description="Compute MACD indicator")
def calc_macd(
    symbol: str,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> dict:
    """
    Compute MACD (Moving Average Convergence Divergence).

    Args:
        symbol: Symbol
        fast: Fast period, default 12
        slow: Slow period, default 26
        signal: Signal line period, default 9

    Returns:
        MACD values
    """
    try:
        closes = _get_closes(symbol, "6mo")

        if len(closes) < slow + signal:
            return {"symbol": symbol, "error": "Insufficient data"}

        ema_fast = _calc_ema(closes, fast)
        ema_slow = _calc_ema(closes, slow)
        min_len = min(len(ema_fast), len(ema_slow))
        ema_fast = ema_fast[-min_len:]
        ema_slow = ema_slow[-min_len:]
        macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
        signal_line = _calc_ema(macd_line, signal)
        histogram = macd_line[-1] - signal_line[-1] if signal_line else 0

        macd_val = round(macd_line[-1], 4) if macd_line else 0
        signal_val = round(signal_line[-1], 4) if signal_line else 0
        hist_val = round(histogram, 4)

        if macd_val > signal_val and hist_val > 0:
            interpretation = "MACD golden cross, bullish"
            trend_signal = "Buy signal"
        elif macd_val < signal_val and hist_val < 0:
            interpretation = "MACD death cross, bearish"
            trend_signal = "Sell signal"
        else:
            interpretation = "Trend unclear"
            trend_signal = "Hold"

        return {
            "symbol": symbol.upper(),
            "indicator": "MACD",
            "params": f"({fast}, {slow}, {signal})",
            "macd": macd_val,
            "signal": signal_val,
            "histogram": hist_val,
            "interpretation": interpretation,
            "trend_signal": trend_signal,
            "latest_price": round(closes[-1], 2),
        }
    except Exception as e:
        return {"symbol": symbol, "indicator": "MACD", "error": str(e)}


@tool(description="Compute moving average")
def calc_moving_average(
    symbol: str,
    period: int = 20,
    ma_type: str = "SMA",
) -> dict:
    """
    Compute moving average.

    Args:
        symbol: Symbol
        period: Period, default 20
        ma_type: SMA or EMA

    Returns:
        MA value
    """
    try:
        closes = _get_closes(symbol, "6mo")

        if len(closes) < period:
            return {"symbol": symbol, "error": "Insufficient data"}

        if ma_type.upper() == "EMA":
            ma_values = _calc_ema(closes, period)
            ma_value = ma_values[-1] if ma_values else 0
        else:
            ma_value = sum(closes[-period:]) / period

        ma_value = round(ma_value, 2)
        current_price = round(closes[-1], 2)

        if current_price > ma_value:
            position = "Price above MA"
            interpretation = "Short-term bullish"
        else:
            position = "Price below MA"
            interpretation = "Short-term bearish"

        return {
            "symbol": symbol.upper(),
            "indicator": f"{ma_type.upper()}{period}",
            "period": period,
            "type": ma_type.upper(),
            "value": ma_value,
            "current_price": current_price,
            "position": position,
            "interpretation": interpretation,
            "diff": round(current_price - ma_value, 2),
            "diff_pct": f"{(current_price - ma_value) / ma_value * 100:+.2f}%",
        }
    except Exception as e:
        return {"symbol": symbol, "indicator": "MA", "error": str(e)}


@tool(description="Compute Bollinger Bands")
def calc_bollinger_bands(
    symbol: str,
    period: int = 20,
    std_dev: float = 2.0,
) -> dict:
    """
    Compute Bollinger Bands.

    Args:
        symbol: Symbol
        period: Period, default 20
        std_dev: Std dev multiplier, default 2

    Returns:
        Upper, middle, lower bands
    """
    try:
        closes = _get_closes(symbol, "3mo")

        if len(closes) < period:
            return {"symbol": symbol, "error": "Insufficient data"}

        middle = sum(closes[-period:]) / period
        variance = sum((x - middle) ** 2 for x in closes[-period:]) / period
        std = variance ** 0.5
        upper = middle + std_dev * std
        lower = middle - std_dev * std
        current_price = closes[-1]

        if current_price > upper:
            position = "Above upper band"
            interpretation = "Overbought, possible pullback"
        elif current_price < lower:
            position = "Below lower band"
            interpretation = "Oversold, possible bounce"
        elif current_price > middle:
            position = "Upper half"
            interpretation = "Relatively strong"
        else:
            position = "Lower half"
            interpretation = "Relatively weak"

        return {
            "symbol": symbol.upper(),
            "indicator": "BOLL",
            "period": period,
            "std_dev": std_dev,
            "upper": round(upper, 2),
            "middle": round(middle, 2),
            "lower": round(lower, 2),
            "current_price": round(current_price, 2),
            "position": position,
            "interpretation": interpretation,
            "bandwidth": round((upper - lower) / middle * 100, 2),
        }
    except Exception as e:
        return {"symbol": symbol, "indicator": "BOLL", "error": str(e)}
