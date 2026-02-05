"""
PPT analytics: Sharpe ratio, max drawdown, win rate / profit factor, position analysis.

Used for: PPT web UI analytics API; reads equity history and trades from core.db; uses core.utils for current date.

Functions:
    calc_sharpe_ratio(account_name, risk_free_rate=0.02) -> Dict       Sharpe ratio, annual return, volatility
    calc_max_drawdown(account_name) -> Dict                             Max drawdown, peak/trough dates
    calc_trade_stats(account_name) -> Dict                              Win rate, profit factor, total trades
    get_position_analysis(account_name) -> Dict                         Position-level PnL and weights
    get_full_analytics(account_name) -> Dict                            All of the above in one call

Features:
    - Equity history and trades from database; risk-free rate default 2%; returns dicts with standard keys
"""
import math
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from . import db as database
from .utils import get_current_datetime_iso


# ============================================================
# Sharpe ratio
# ============================================================

def calc_sharpe_ratio(account_name: str, risk_free_rate: float = 0.02) -> Dict[str, Any]:
    """Sharpe ratio (Rp - Rf) / sigma; returns sharpe_ratio, annual_return, volatility, data_days."""
    history = database.get_equity_history(account_name)
    
    if len(history) < 2:
        return {
            'sharpe_ratio': 0,
            'annual_return': 0,
            'volatility': 0,
            'data_days': len(history),
            'error': '数据不足'
        }
    
    # 计算日收益率
    daily_returns = []
    for i in range(1, len(history)):
        prev_equity = history[i-1]['equity']
        curr_equity = history[i]['equity']
        if prev_equity > 0:
            daily_return = (curr_equity - prev_equity) / prev_equity
            daily_returns.append(daily_return)
    
    if not daily_returns:
        return {
            'sharpe_ratio': 0,
            'annual_return': 0,
            'volatility': 0,
            'data_days': len(history),
            'error': '无法计算收益率'
        }
    
    # 平均日收益率
    avg_daily_return = sum(daily_returns) / len(daily_returns)
    
    # 日收益率标准差
    variance = sum((r - avg_daily_return) ** 2 for r in daily_returns) / len(daily_returns)
    daily_std = math.sqrt(variance) if variance > 0 else 0
    
    # 年化
    trading_days = 252
    annual_return = avg_daily_return * trading_days
    annual_volatility = daily_std * math.sqrt(trading_days)
    
    # 日无风险收益率
    daily_rf = risk_free_rate / trading_days
    
    # 夏普比率 (年化)
    if annual_volatility > 0:
        sharpe = (annual_return - risk_free_rate) / annual_volatility
    else:
        sharpe = 0
    
    return {
        'sharpe_ratio': round(sharpe, 2),
        'annual_return': round(annual_return * 100, 2),  # 百分比
        'volatility': round(annual_volatility * 100, 2),  # 百分比
        'data_days': len(history),
    }


# ============================================================
# Max drawdown
# ============================================================

def calc_max_drawdown(account_name: str) -> Dict[str, Any]:
    """
    计算最大回撤
    
    Max Drawdown = (Peak - Trough) / Peak
    
    Returns:
        {
            'max_drawdown': float,      # 最大回撤 (百分比)
            'max_drawdown_amount': float,  # 最大回撤金额
            'peak_date': str,           # 峰值日期
            'trough_date': str,         # 谷值日期
            'peak_value': float,        # 峰值
            'trough_value': float,      # 谷值
            'current_drawdown': float,  # 当前回撤
        }
    """
    history = database.get_equity_history(account_name)
    
    if len(history) < 2:
        return {
            'max_drawdown': 0,
            'max_drawdown_amount': 0,
            'peak_date': None,
            'trough_date': None,
            'peak_value': 0,
            'trough_value': 0,
            'current_drawdown': 0,
            'error': '数据不足'
        }
    
    max_drawdown = 0
    max_dd_amount = 0
    peak = history[0]['equity']
    peak_date = history[0]['date']
    trough = peak
    trough_date = peak_date
    
    best_peak_date = peak_date
    best_trough_date = peak_date
    best_peak_value = peak
    best_trough_value = peak
    
    for h in history:
        equity = h['equity']
        date = h['date']
        
        if equity > peak:
            peak = equity
            peak_date = date
            trough = equity
            trough_date = date
        elif equity < trough:
            trough = equity
            trough_date = date
        
        if peak > 0:
            drawdown = (peak - trough) / peak
            dd_amount = peak - trough
            
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_dd_amount = dd_amount
                best_peak_date = peak_date
                best_trough_date = trough_date
                best_peak_value = peak
                best_trough_value = trough
    
    # 当前回撤
    current_equity = history[-1]['equity']
    running_peak = max(h['equity'] for h in history)
    current_dd = (running_peak - current_equity) / running_peak if running_peak > 0 else 0
    
    return {
        'max_drawdown': round(max_drawdown * 100, 2),
        'max_drawdown_amount': round(max_dd_amount, 2),
        'peak_date': best_peak_date,
        'trough_date': best_trough_date,
        'peak_value': round(best_peak_value, 2),
        'trough_value': round(best_trough_value, 2),
        'current_drawdown': round(current_dd * 100, 2),
    }


# ============================================================
# Trade stats (win rate, profit factor)
# ============================================================

def calc_trade_stats(account_name: str) -> Dict[str, Any]:
    """
    计算交易统计
    
    Returns:
        {
            'total_trades': int,       # 总交易次数
            'win_trades': int,         # 盈利交易次数
            'lose_trades': int,        # 亏损交易次数
            'win_rate': float,         # 胜率 (百分比)
            'profit_factor': float,    # 盈亏比 (总盈利/总亏损)
            'avg_win': float,          # 平均盈利
            'avg_loss': float,         # 平均亏损
            'largest_win': float,      # 最大单笔盈利
            'largest_loss': float,     # 最大单笔亏损
            'total_profit': float,     # 总盈利
            'total_loss': float,       # 总亏损
            'net_profit': float,       # 净盈亏
        }
    """
    trades = database.get_trades(account_name, limit=10000)
    
    if not trades:
        return {
            'total_trades': 0,
            'win_trades': 0,
            'lose_trades': 0,
            'win_rate': 0,
            'profit_factor': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'largest_win': 0,
            'largest_loss': 0,
            'total_profit': 0,
            'total_loss': 0,
            'net_profit': 0,
        }
    
    # 按 symbol 分组，计算每个交易对的盈亏
    # 简化: 以 卖出 为准计算盈亏
    # 真实计算需要 FIFO/LIFO 匹配买卖
    
    # 简化方法: 计算已平仓盈亏
    # 使用 buy_value / sell_value 简单计算
    
    symbol_trades: Dict[str, List] = {}
    for t in trades:
        symbol = t['symbol']
        if symbol not in symbol_trades:
            symbol_trades[symbol] = []
        symbol_trades[symbol].append(t)
    
    profits = []  # 每笔交易的盈亏
    
    for symbol, sym_trades in symbol_trades.items():
        # 按时间排序
        sym_trades.sort(key=lambda x: x['time'])
        
        # FIFO 匹配
        buy_queue = []  # (qty, price)
        
        for t in sym_trades:
            if t['side'] == 'buy':
                buy_queue.append({'qty': t['qty'], 'price': t['price']})
            elif t['side'] == 'sell':
                sell_qty = t['qty']
                sell_price = t['price']
                
                # 匹配买入
                while sell_qty > 0 and buy_queue:
                    buy = buy_queue[0]
                    match_qty = min(sell_qty, buy['qty'])
                    
                    # 计算盈亏
                    pnl = (sell_price - buy['price']) * match_qty
                    profits.append(pnl)
                    
                    sell_qty -= match_qty
                    buy['qty'] -= match_qty
                    
                    if buy['qty'] <= 0:
                        buy_queue.pop(0)
    
    if not profits:
        return {
            'total_trades': len(trades),
            'win_trades': 0,
            'lose_trades': 0,
            'win_rate': 0,
            'profit_factor': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'largest_win': 0,
            'largest_loss': 0,
            'total_profit': 0,
            'total_loss': 0,
            'net_profit': 0,
            'note': '无已平仓交易'
        }
    
    # 统计
    wins = [p for p in profits if p > 0]
    losses = [p for p in profits if p < 0]
    
    total_profit = sum(wins)
    total_loss = abs(sum(losses))
    
    return {
        'total_trades': len(profits),
        'win_trades': len(wins),
        'lose_trades': len(losses),
        'win_rate': round(len(wins) / len(profits) * 100, 1) if profits else 0,
        'profit_factor': round(total_profit / total_loss, 2) if total_loss > 0 else float('inf') if total_profit > 0 else 0,
        'avg_win': round(sum(wins) / len(wins), 2) if wins else 0,
        'avg_loss': round(sum(losses) / len(losses), 2) if losses else 0,
        'largest_win': round(max(wins), 2) if wins else 0,
        'largest_loss': round(min(losses), 2) if losses else 0,
        'total_profit': round(total_profit, 2),
        'total_loss': round(total_loss, 2),
        'net_profit': round(total_profit - total_loss, 2),
    }


# ============================================================
# 持仓分析
# ============================================================

def calc_position_analysis(account_name: str, quotes: Dict[str, Dict] = None) -> Dict[str, Any]:
    """
    持仓分析
    
    Returns:
        {
            'total_positions': int,      # 持仓数量
            'total_value': float,        # 总市值
            'concentration': {           # 集中度
                'top1': float,           # 最大持仓占比
                'top3': float,           # 前3持仓占比
                'hhi': float,            # HHI 指数 (赫芬达尔指数)
            },
            'positions': [               # 各持仓详情
                {
                    'symbol': str,
                    'qty': int,
                    'value': float,
                    'weight': float,     # 占比
                    'pnl': float,
                    'pnl_pct': float,
                }
            ],
            'sector_exposure': {},       # 行业分布 (简化版)
        }
    """
    positions = database.get_positions(account_name)
    account = database.get_account(account_name)
    
    if not positions:
        return {
            'total_positions': 0,
            'total_value': 0,
            'concentration': {'top1': 0, 'top3': 0, 'hhi': 0},
            'positions': [],
        }
    
    # 计算每个持仓的市值
    position_details = []
    total_value = 0
    
    for symbol, pos in positions.items():
        qty = pos['qty']
        avg_price = pos['avg_price']
        cost = qty * avg_price
        
        # 获取当前价格
        current_price = avg_price  # 默认用成本价
        if quotes and symbol in quotes:
            q = quotes[symbol]
            if q.get('price', 0) > 0:
                current_price = q['price']
        
        market_value = qty * current_price
        pnl = market_value - cost
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0
        
        position_details.append({
            'symbol': symbol,
            'qty': qty,
            'avg_price': round(avg_price, 2),
            'current_price': round(current_price, 2),
            'cost': round(cost, 2),
            'value': round(market_value, 2),
            'pnl': round(pnl, 2),
            'pnl_pct': round(pnl_pct, 2),
        })
        total_value += market_value
    
    # 计算权重
    for p in position_details:
        p['weight'] = round(p['value'] / total_value * 100, 1) if total_value > 0 else 0
    
    # 按市值排序
    position_details.sort(key=lambda x: x['value'], reverse=True)
    
    # 计算集中度
    weights = [p['weight'] / 100 for p in position_details]
    top1 = weights[0] * 100 if weights else 0
    top3 = sum(weights[:3]) * 100 if len(weights) >= 3 else sum(weights) * 100
    
    # HHI 指数 (0-10000, 越高越集中)
    hhi = sum(w * w for w in weights) * 10000
    
    # 账户总资产占比 (包含现金)
    total_assets = account['cash'] + total_value if account else total_value
    position_pct = (total_value / total_assets * 100) if total_assets > 0 else 0
    
    return {
        'total_positions': len(positions),
        'total_value': round(total_value, 2),
        'total_assets': round(total_assets, 2),
        'position_pct': round(position_pct, 1),  # 仓位比例
        'cash_pct': round(100 - position_pct, 1),  # 现金比例
        'concentration': {
            'top1': round(top1, 1),
            'top3': round(top3, 1),
            'hhi': round(hhi, 0),
        },
        'positions': position_details,
    }


# ============================================================
# 综合分析
# ============================================================

def get_full_analytics(account_name: str, quotes: Dict[str, Dict] = None) -> Dict[str, Any]:
    """
    获取完整的绩效分析
    """
    return {
        'sharpe': calc_sharpe_ratio(account_name),
        'drawdown': calc_max_drawdown(account_name),
        'trade_stats': calc_trade_stats(account_name),
        'positions': calc_position_analysis(account_name, quotes),
        'generated_at': get_current_datetime_iso(),
    }
