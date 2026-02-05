"""
Backtest performance metrics: return, drawdown, Sharpe, win rate, etc.

Classes:
    BacktestMetrics   Performance metrics dataclass

BacktestMetrics fields:
    total_return, annual_return, max_drawdown, sharpe_ratio, sortino_ratio, calmar_ratio,
    win_rate, profit_factor, total_trades, winning_trades, losing_trades,
    avg_win, avg_loss, max_win, max_loss

Functions:
    calculate_metrics(trades: Sequence[Trade], equity_curve: Sequence[tuple], initial_capital: float) -> BacktestMetrics

"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from .types import Trade, OrderSide


@dataclass
class BacktestMetrics:
    """
    Backtest performance metrics.

    Attributes:
        total_return: Total return (%)
        annual_return: Annualized return (%)
        max_drawdown: Max drawdown (%)
        sharpe_ratio: Sharpe ratio
        sortino_ratio: Sortino ratio
        calmar_ratio: Calmar ratio
        win_rate: Win rate (%)
        profit_factor: Profit factor
        total_trades: Total trades
        winning_trades: Winning trades
        losing_trades: Losing trades
        avg_win: Average win
        avg_loss: Average loss
        max_win: Max single win
        max_loss: Max single loss
        avg_holding_period: Average holding period (days)
    """
    total_return: float = 0.0
    annual_return: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_win: float = 0.0
    max_loss: float = 0.0
    avg_holding_period: float = 0.0


def calculate_metrics(
    equity_curve: Sequence[tuple[datetime, float]],
    trades: Sequence[Trade],
    initial_capital: float,
    risk_free_rate: float = 0.02,  # Risk-free rate 2%
    trading_days: int = 252,       # Trading days per year
) -> BacktestMetrics:
    """
    Compute backtest performance metrics.

    Args:
        equity_curve: Equity curve [(timestamp, equity), ...]
        trades: Trade list
        initial_capital: Initial capital
        risk_free_rate: Risk-free rate
        trading_days: Trading days per year

    Returns:
        Performance metrics
    """
    if not equity_curve:
        return BacktestMetrics()

    # Extract equity series
    equities = [e for _, e in equity_curve]
    timestamps = [t for t, _ in equity_curve]

    # Basic return
    final_equity = equities[-1]
    total_return = (final_equity - initial_capital) / initial_capital * 100

    # Annualized return
    days = (timestamps[-1] - timestamps[0]).days
    years = max(days / 365, 0.01)  # Avoid div by zero
    annual_return = ((final_equity / initial_capital) ** (1 / years) - 1) * 100

    # Max drawdown
    max_drawdown = _calculate_max_drawdown(equities)

    # Return series (for Sharpe etc.)
    returns = _calculate_returns(equities)

    # Sharpe ratio
    sharpe_ratio = _calculate_sharpe_ratio(returns, risk_free_rate, trading_days)

    # Sortino ratio
    sortino_ratio = _calculate_sortino_ratio(returns, risk_free_rate, trading_days)

    # Calmar ratio
    calmar_ratio = annual_return / max_drawdown if max_drawdown > 0 else 0.0

    # Trade stats
    trade_stats = _calculate_trade_stats(trades)

    return BacktestMetrics(
        total_return=total_return,
        annual_return=annual_return,
        max_drawdown=max_drawdown,
        sharpe_ratio=sharpe_ratio,
        sortino_ratio=sortino_ratio,
        calmar_ratio=calmar_ratio,
        **trade_stats,
    )


def _calculate_max_drawdown(equities: Sequence[float]) -> float:
    """Compute max drawdown."""
    if not equities:
        return 0.0

    peak = equities[0]
    max_dd = 0.0

    for equity in equities:
        if equity > peak:
            peak = equity

        drawdown = (peak - equity) / peak * 100
        if drawdown > max_dd:
            max_dd = drawdown

    return max_dd


def _calculate_returns(equities: Sequence[float]) -> list[float]:
    """Compute return series."""
    if len(equities) < 2:
        return []

    returns = []
    for i in range(1, len(equities)):
        if equities[i-1] != 0:
            ret = (equities[i] - equities[i-1]) / equities[i-1]
            returns.append(ret)

    return returns


def _calculate_sharpe_ratio(
    returns: Sequence[float],
    risk_free_rate: float,
    trading_days: int
) -> float:
    """
    Compute Sharpe ratio.

    Sharpe = (E[R] - Rf) / std(R) * sqrt(N)
    """
    if not returns:
        return 0.0

    # Daily risk-free rate
    daily_rf = risk_free_rate / trading_days

    # Excess returns
    excess_returns = [r - daily_rf for r in returns]

    # Mean and std
    mean_return = sum(excess_returns) / len(excess_returns)

    if len(excess_returns) < 2:
        return 0.0

    variance = sum((r - mean_return) ** 2 for r in excess_returns) / (len(excess_returns) - 1)
    std_dev = math.sqrt(variance) if variance > 0 else 0.0

    if std_dev == 0:
        return 0.0

    # Annualized
    sharpe = (mean_return / std_dev) * math.sqrt(trading_days)

    return sharpe


def _calculate_sortino_ratio(
    returns: Sequence[float],
    risk_free_rate: float,
    trading_days: int
) -> float:
    """
    Compute Sortino ratio.

    Sortino = (E[R] - Rf) / downside_std(R) * sqrt(N)
    Uses downside risk only.
    """
    if not returns:
        return 0.0

    daily_rf = risk_free_rate / trading_days
    excess_returns = [r - daily_rf for r in returns]

    mean_return = sum(excess_returns) / len(excess_returns)

    # Downside returns
    downside_returns = [r for r in excess_returns if r < 0]

    if not downside_returns:
        return 0.0  # No losses

    downside_variance = sum(r ** 2 for r in downside_returns) / len(downside_returns)
    downside_std = math.sqrt(downside_variance) if downside_variance > 0 else 0.0

    if downside_std == 0:
        return 0.0

    sortino = (mean_return / downside_std) * math.sqrt(trading_days)

    return sortino


def _calculate_trade_stats(trades: Sequence[Trade]) -> dict:
    """Compute trade statistics."""
    # Only count sell trades (closes)
    sell_trades = [t for t in trades if t.side == OrderSide.SELL]

    if not sell_trades:
        return {
            "total_trades": len(trades),
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "max_win": 0.0,
            "max_loss": 0.0,
            "avg_holding_period": 0.0,
        }

    # P&L classification
    wins = [t.pnl for t in sell_trades if t.pnl > 0]
    losses = [t.pnl for t in sell_trades if t.pnl < 0]

    total_trades = len(sell_trades)
    winning_trades = len(wins)
    losing_trades = len(losses)

    # Win rate
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0

    # Profit factor
    total_win = sum(wins) if wins else 0
    total_loss = abs(sum(losses)) if losses else 0
    profit_factor = (total_win / total_loss) if total_loss > 0 else float('inf') if total_win > 0 else 0.0

    # Avg win/loss
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0

    # Max win/loss
    max_win = max(wins) if wins else 0.0
    max_loss = min(losses) if losses else 0.0

    return {
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor if profit_factor != float('inf') else 999.99,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "max_win": max_win,
        "max_loss": max_loss,
        "avg_holding_period": 0.0,  # TODO: compute avg holding period
    }
