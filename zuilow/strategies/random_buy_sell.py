"""
RandomBuyOrSell: 测试用策略。通过 DMS 拉取当前日期前历史，做简单回测筛选后随机选 5 只、随机权重。
不依赖配置参数；每 N 天由 scheduler job trigger 调仓（如 interval hours: 240）。
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta

from zuilow.components.backtest.strategy import Strategy, StrategyContext
from zuilow.components.backtest.types import Bar, Signal

logger = logging.getLogger(__name__)

# 默认美股池（无配置时使用；DMS 需有对应数据）
_DEFAULT_US_SYMBOLS = [
    "US.AAPL",
    "US.GOOGL",
    "US.MSFT",
    "US.AMD",
    "US.TSLA",
    "US.NVDA",
    "US.META",
    "US.AMZN",
    "US.JPM",
    "US.V",
]
_NUM_STOCKS = 5
_LOOKBACK_DAYS = 150
_MIN_BARS = 5


class RandomBuyOrSell(Strategy):
    """
    通过 DMS 获取历史、简单回测筛选后随机选 5 只、随机权重。无需配置参数。
    """

    @classmethod
    def init_config(cls) -> dict:
        return {"params": {}}

    def __init__(self, **kwargs):
        super().__init__(name="RandomBuyOrSell")

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> Signal | None:
        return None

    def get_rebalance_output(self) -> dict | None:
        """通过 DMS 拉取历史，做简单回测筛选，随机选 5 只并随机权重。"""
        try:
            from zuilow.components.datasource import get_manager
        except ImportError:
            logger.warning("RandomBuyOrSell: DataSourceManager not available")
            return self._random_allocation(_DEFAULT_US_SYMBOLS)

        from zuilow.components.control.ctrl import get_current_dt
        end_date = get_current_dt()
        start_date = end_date - timedelta(days=_LOOKBACK_DAYS)
        manager = get_manager()
        candidates = []

        symbols = manager.get_symbols()
        if not symbols:
            symbols = _DEFAULT_US_SYMBOLS
        n_sel = min(_NUM_STOCKS * 2, len(symbols))
        sel_symbols = random.sample(symbols, n_sel)
        logger.info("RandomBuyOrSell: symbols count %d, selected %d", len(symbols), len(sel_symbols))

        for symbol in sel_symbols:
            try:
                logger.info(f"RandomBuyOrSell: get_history {symbol} {start_date} {end_date}")
                df = manager.get_history(symbol, start_date, end_date, "1d")
                if df is None or len(df) < _MIN_BARS:
                    continue
                close = df.get("Close") if "Close" in df.columns else df.get("close")
                if close is None or len(close) < _MIN_BARS:
                    continue
                # 简单回测：最近 5 日收益为正的才入候选（bull 5d）
                ret_5d = float(close.iloc[-1] / close.iloc[-_MIN_BARS] - 1)
                if ret_5d > 0:
                    candidates.append(symbol)
            except Exception as e:
                logger.debug("RandomBuyOrSell: get_history %s failed: %s", symbol, e)

        if len(candidates) < _NUM_STOCKS:
            candidates = _DEFAULT_US_SYMBOLS[: max(_NUM_STOCKS, len(_DEFAULT_US_SYMBOLS))]
        return self._random_allocation(candidates)

    def _random_allocation(self, pool: list[str]) -> dict | None:
        """从 pool 中随机选 5 只、随机权重，返回 allocation 信号。"""
        if not pool:
            return None
        n = min(_NUM_STOCKS, len(pool))
        chosen = random.sample(pool, n)
        raw = [random.random() for _ in chosen]
        total = sum(raw)
        weights = {s: w / total for s, w in zip(chosen, raw)}
        out = {"kind": "allocation", "target_weights": weights}
        logger.info("RandomBuyOrSell: get_rebalance_output() %s", weights)
        return out


# Alias for scheduler/config that still reference Bull5dRandom (e.g. bull5d_random job)
Bull5dRandom = RandomBuyOrSell
