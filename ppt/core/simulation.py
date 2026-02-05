"""
PPT simulation: slippage, commission, partial fill, latency; config from config/simulation.yaml or defaults.

Used for: order execution in webhook and trade API; apply slippage/commission to price/qty, optional partial fill.

Functions:
    load_config() -> Dict                    Load from config/simulation.yaml; merge presets
    get_simulation_status() -> Dict          Current config and presets for API
    apply_slippage(price, side, ...) -> float   Apply slippage to price
    apply_commission(qty, price, ...) -> float   Apply commission
    execute_order(account, symbol, side, qty, price=None, ...) -> Dict   Simulate fill; update position/order/trade/equity

Features:
    - Config: slippage, commission, partial_fill, latency; presets in YAML; execute_order updates db (position, order, trade, equity)
"""
import os
import random
import time
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

# Default simulation config
DEFAULT_CONFIG = {
    'slippage': {
        'enabled': True,
        'mode': 'percentage',
        'value': 0.05
    },
    'commission': {
        'enabled': True,
        'mode': 'percentage',
        'rate': 0.001,
        'minimum': 1.0,
        'per_trade': 5.0
    },
    'partial_fill': {
        'enabled': False,
        'threshold': 10000,
        'min_fill_rate': 0.3,
        'max_fill_rate': 1.0
    },
    'latency': {
        'enabled': False,
        'min_ms': 50,
        'max_ms': 200
    }
}

# 全局配置
_config: Dict[str, Any] = {}


def load_config() -> Dict[str, Any]:
    """Load simulation config from config/simulation.yaml; merge presets."""
    global _config
    
    config_path = Path(__file__).parent / "config" / "simulation.yaml"
    
    if config_path.exists():
        try:
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                simulation = data.get('simulation', {})
                presets = data.get('presets', {})
                
                # 检查是否使用预设
                use_preset = simulation.get('use_preset')
                if use_preset and use_preset in presets:
                    # 使用预设配置
                    preset = presets[use_preset]
                    _config = {
                        'slippage': preset.get('slippage', DEFAULT_CONFIG['slippage']),
                        'commission': preset.get('commission', DEFAULT_CONFIG['commission']),
                        'partial_fill': preset.get('partial_fill', DEFAULT_CONFIG['partial_fill']),
                        'latency': preset.get('latency', DEFAULT_CONFIG['latency']),
                        '_preset': use_preset  # 记录使用的预设名
                    }
                    print(f"[Simulation] 使用预设: {use_preset}")
                else:
                    # 使用自定义配置
                    _config = {
                        'slippage': simulation.get('slippage', DEFAULT_CONFIG['slippage']),
                        'commission': simulation.get('commission', DEFAULT_CONFIG['commission']),
                        'partial_fill': simulation.get('partial_fill', DEFAULT_CONFIG['partial_fill']),
                        'latency': simulation.get('latency', DEFAULT_CONFIG['latency']),
                        '_preset': None
                    }
                    if use_preset:
                        print(f"[Simulation] 预设 '{use_preset}' 不存在，使用自定义配置")
                
                return _config
        except Exception as e:
            print(f"[Simulation] 加载配置失败: {e}, 使用默认配置")
    
    _config = DEFAULT_CONFIG.copy()
    return _config


def get_config() -> Dict[str, Any]:
    """获取当前配置"""
    if not _config:
        load_config()
    return _config


def apply_slippage(price: float, side: str) -> Tuple[float, float]:
    """
    应用滑点
    
    Args:
        price: 基准价格
        side: 'buy' 或 'sell'
    
    Returns:
        (执行价格, 滑点金额)
    """
    config = get_config()
    slip_config = config.get('slippage', {})
    
    if not slip_config.get('enabled', False):
        return price, 0.0
    
    mode = slip_config.get('mode', 'percentage')
    value = slip_config.get('value', 0.05)
    
    if mode == 'percentage':
        # 百分比滑点
        slip_amount = price * value / 100
    elif mode == 'fixed':
        # 固定金额滑点
        slip_amount = value
    elif mode == 'random':
        # 随机滑点 (0 ~ value%)
        slip_amount = price * value / 100 * random.random()
    else:
        slip_amount = 0.0
    
    # 买入价格上浮，卖出价格下浮
    if side == 'buy':
        exec_price = price + slip_amount
    else:
        exec_price = price - slip_amount
    
    return round(exec_price, 4), round(slip_amount, 4)


def calc_commission(qty: int, price: float, order_value: float) -> float:
    """
    计算手续费
    
    Args:
        qty: 数量
        price: 价格
        order_value: 订单金额
    
    Returns:
        手续费金额
    """
    config = get_config()
    comm_config = config.get('commission', {})
    
    if not comm_config.get('enabled', False):
        return 0.0
    
    mode = comm_config.get('mode', 'percentage')
    
    if mode == 'percentage':
        rate = comm_config.get('rate', 0.001)
        minimum = comm_config.get('minimum', 1.0)
        commission = max(minimum, order_value * rate)
    
    elif mode == 'fixed':
        commission = comm_config.get('per_trade', 5.0)
    
    elif mode == 'tiered':
        # 阶梯费率
        tiers = comm_config.get('tiers', [])
        commission = 0.0
        remaining = order_value
        prev_max = 0
        
        for tier in tiers:
            tier_max = tier.get('max_value') or float('inf')
            tier_rate = tier.get('rate', 0.001)
            tier_amount = min(remaining, tier_max - prev_max)
            
            if tier_amount > 0:
                commission += tier_amount * tier_rate
                remaining -= tier_amount
                prev_max = tier_max
            
            if remaining <= 0:
                break
    else:
        commission = 0.0
    
    return round(commission, 2)


def calc_partial_fill(order_value: float, qty: int) -> Tuple[int, float]:
    """
    计算部分成交
    
    Args:
        order_value: 订单金额
        qty: 订单数量
    
    Returns:
        (成交数量, 成交比例)
    """
    config = get_config()
    pf_config = config.get('partial_fill', {})
    
    if not pf_config.get('enabled', False):
        return qty, 1.0
    
    threshold = pf_config.get('threshold', 10000)
    
    # 小于阈值的订单全部成交
    if order_value < threshold:
        return qty, 1.0
    
    min_rate = pf_config.get('min_fill_rate', 0.3)
    max_rate = pf_config.get('max_fill_rate', 1.0)
    
    # 随机成交比例
    fill_rate = random.uniform(min_rate, max_rate)
    filled_qty = max(1, int(qty * fill_rate))
    
    return filled_qty, round(fill_rate, 2)


def apply_latency():
    """应用延迟模拟"""
    config = get_config()
    lat_config = config.get('latency', {})
    
    if not lat_config.get('enabled', False):
        return
    
    min_ms = lat_config.get('min_ms', 50)
    max_ms = lat_config.get('max_ms', 200)
    
    delay = random.uniform(min_ms, max_ms) / 1000
    time.sleep(delay)


def simulate_execution(
    symbol: str,
    side: str,
    qty: int,
    price: float
) -> Dict[str, Any]:
    """
    模拟订单执行
    
    Args:
        symbol: 股票代码
        side: 买卖方向
        qty: 数量
        price: 价格
    
    Returns:
        执行结果字典
    """
    # 应用延迟
    apply_latency()
    
    order_value = qty * price
    
    # 部分成交
    filled_qty, fill_rate = calc_partial_fill(order_value, qty)
    
    # 滑点
    exec_price, slippage = apply_slippage(price, side)
    
    # 实际成交金额
    filled_value = filled_qty * exec_price
    
    # 手续费
    commission = calc_commission(filled_qty, exec_price, filled_value)
    
    # 总成本 (买入) 或 净收入 (卖出)
    if side == 'buy':
        total_cost = filled_value + commission
    else:
        total_cost = filled_value - commission
    
    return {
        'symbol': symbol,
        'side': side,
        'requested_qty': qty,
        'filled_qty': filled_qty,
        'fill_rate': fill_rate,
        'requested_price': price,
        'exec_price': exec_price,
        'slippage': slippage,
        'filled_value': round(filled_value, 2),
        'commission': commission,
        'total_cost': round(total_cost, 2),
        'partial_fill': filled_qty < qty
    }


def get_simulation_status() -> Dict[str, Any]:
    """获取模拟配置状态"""
    config = get_config()
    
    return {
        'preset': config.get('_preset'),  # 当前使用的预设名 (None = 自定义)
        'slippage': {
            'enabled': config.get('slippage', {}).get('enabled', False),
            'mode': config.get('slippage', {}).get('mode', 'percentage'),
            'value': config.get('slippage', {}).get('value', 0)
        },
        'commission': {
            'enabled': config.get('commission', {}).get('enabled', False),
            'mode': config.get('commission', {}).get('mode', 'percentage'),
            'rate': config.get('commission', {}).get('rate', 0),
            'minimum': config.get('commission', {}).get('minimum', 0)
        },
        'partial_fill': {
            'enabled': config.get('partial_fill', {}).get('enabled', False),
            'threshold': config.get('partial_fill', {}).get('threshold', 0)
        },
        'latency': {
            'enabled': config.get('latency', {}).get('enabled', False)
        }
    }


# 模块加载时初始化配置
load_config()
