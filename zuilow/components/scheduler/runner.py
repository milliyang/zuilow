"""
Strategy runner: load config, instantiate strategy, run and produce signals.

Produce signals: write to signal store (default) or send immediately (send_immediately=True).
Runner uses ZuiLow API for market data; converts strategy output dict to TradingSignal and writes to SignalStore.

Classes:
    StrategyRunner   Load and run strategies; produce and store or send signals

StrategyRunner methods:
    .load_strategy_config(config_path: str) -> dict
    .create_strategy(strategy_name: str, config: dict) -> Any
    .run_strategy(strategy, symbols: list[str], mode: str = "paper") -> list[dict]
    .signals_dict_to_trading_signals(signals: list[dict], job_name: str, account: str, market=None) -> list[TradingSignal]
    .write_signals_to_store(signals: list[TradingSignal]) -> list[int]
    .send_signals(signals: list[dict]) -> list[dict]

StrategyRunner config:
    api_base_url: str = "http://localhost:11180"  (for GET /api/market/quote, POST /api/order)

StrategyRunner features:
    - load_strategy_config: path relative to config/; returns YAML dict
    - create_strategy: import zuilow.components.backtest.strategy; instantiate with config["params"]
    - run_strategy: fetch quote per symbol via API; call strategy logic; return list of signal dicts
    - signals_dict_to_trading_signals: map dict to TradingSignal (order/rebalance)
    - write_signals_to_store: add_many to SignalStore
    - send_signals: POST /api/order per signal (when send_immediately=True)
"""

from __future__ import annotations

import logging
import importlib
from pathlib import Path
from typing import Any, Callable, Optional
from datetime import datetime, timedelta
import yaml
import requests

from zuilow.components.control import ctrl
from zuilow.components.signals import TradingSignal, get_signal_store

logger = logging.getLogger(__name__)


def _infer_market(symbol: str) -> str:
    """Infer market from symbol prefix (e.g. HK.00700 -> HK, US.AAPL -> US)."""
    if symbol.startswith("HK."):
        return "HK"
    if symbol.startswith("US."):
        return "US"
    return "UNKNOWN"


class StrategyRunner:
    """
    Strategy runner: load config, instantiate strategy, run and produce signals (store or send).

    Supports:
    - Load strategy config (YAML from config/); create strategy instance from zuilow.components.backtest.strategy
    - Run strategy with market data from ZuiLow API; convert output to TradingSignal; write to store or send

    Features:
    - run_strategy: GET /api/market/quote per symbol; strategy produces signal dicts
    - signals_dict_to_trading_signals: map to TradingSignal.order or .rebalance
    - write_signals_to_store / send_signals for persistence or immediate gateway
    """

    def __init__(self, api_base_url: str = "http://localhost:11180"):
        self.api_base_url = api_base_url
        self._quote_provider: Optional[Callable[[str], Optional[dict]]] = None
        self._history_provider: Optional[Callable[[str, str, str], Any]] = None
        self._get_now: Optional[Callable[[], datetime]] = None
        self._send_dry_run: bool = False

    def set_replay_providers(
        self,
        quote_fn: Optional[Callable[[str], Optional[dict]]] = None,
        history_fn: Optional[Callable[[str, str, str], Any]] = None,
        get_now_fn: Optional[Callable[[], datetime]] = None,
        send_dry_run: bool = False,
    ) -> None:
        """Set data providers and clock for replay (data as of virtual time)."""
        self._quote_provider = quote_fn
        self._history_provider = history_fn
        self._get_now = get_now_fn
        self._send_dry_run = send_dry_run

    def load_strategy_config(self, config_path: str) -> dict:
        """
        Load strategy config file (path relative to config/).

        Args:
            config_path: Path relative to zuilow/config/ (e.g. strategies/sma_daily.yaml)

        Returns:
            Config dict (YAML); typically has "params" for strategy constructor
        """
        config_dir = Path(__file__).parent.parent.parent / "config"
        full_path = config_dir / config_path
        if not full_path.exists():
            raise FileNotFoundError(f"Strategy config not found: {full_path}")
        with open(full_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded strategy config: {config_path}")
        return config

    def get_strategy_config(self, strategy_name: str, config_path: Optional[str] = None) -> dict:
        """
        Get config for a strategy: from YAML file if config_path is given and exists,
        otherwise from strategy's init_config() (in-code defaults).

        Args:
            strategy_name: Strategy class name (e.g. SMAStrategy)
            config_path: Optional path relative to config/ (e.g. strategies/sma_daily.yaml)

        Returns:
            Config dict; config["params"] is passed to strategy constructor
        """
        module = importlib.import_module("zuilow.strategies")
        strategy_class = getattr(module, strategy_name)
        base = {}
        if hasattr(strategy_class, "init_config") and callable(getattr(strategy_class, "init_config")):
            base = strategy_class.init_config() or {}
        path = (config_path or "").strip()
        if path:
            config_dir = Path(__file__).parent.parent.parent / "config"
            full_path = config_dir / path
            if full_path.exists():
                with open(full_path, "r", encoding="utf-8") as f:
                    loaded = yaml.safe_load(f) or {}
                # File overrides in-code defaults
                merged = {**base}
                for k, v in loaded.items():
                    if k == "params" and isinstance(v, dict) and isinstance(merged.get("params"), dict):
                        merged["params"] = {**merged.get("params", {}), **v}
                    else:
                        merged[k] = v
                return merged
        return base

    def create_strategy(self, strategy_name: str, config: dict) -> Any:
        """
        Create strategy instance from zuilow.strategies.

        Args:
            strategy_name: Class name (e.g. SMAStrategy)
            config: Config dict; config["params"] passed as **kwargs to constructor

        Returns:
            Strategy instance

        Raises:
            ImportError, AttributeError: If class not found or load fails
        """
        try:
            module = importlib.import_module("zuilow.strategies")
            strategy_class = getattr(module, strategy_name)
            params = config.get("params", {})
            strategy = strategy_class(**params)
            logger.info(f"Created strategy: {strategy_name} with {params}")
            return strategy
        except (ImportError, AttributeError) as e:
            logger.error(f"Strategy load failed: {strategy_name}, {e}")
            raise

    def run_strategy(
        self,
        strategy: Any,
        symbols: list[str],
        mode: str = "paper",
        account: Optional[str] = None,
        job_name: Optional[str] = None,
        market: Optional[str] = None,
    ) -> list[dict]:
        """
        Run strategy for given symbols; produce signal dicts.

        Args:
            strategy: Strategy instance (from create_strategy)
            symbols: List of symbols (e.g. ["HK.00700"])
            mode: paper | live
            account: Optional account name so quote API uses same gateway (avoid wrong broker).
            job_name: Optional job name (for strategies that check pending/schedule, e.g. grl_5d_topk_reg).
            market: Optional market code (e.g. US, HK).

        Returns:
            List of signal dicts (each has symbol, side, qty, etc. or target_weights/target_mv)
        """
        if hasattr(strategy, "get_rebalance_output"):
            rb = strategy.get_rebalance_output(
                job_name=job_name, account=account, market=market
            )
            if rb:
                rb = dict(rb)
                rb["mode"] = mode
                return [rb]
        signals = []
        for symbol in symbols:
            try:
                market_data = self._fetch_market_data(symbol, account=account)
                if not market_data:
                    logger.warning(f"Cannot get market data for {symbol}")
                    continue
                signal = self._execute_strategy_logic(strategy, symbol, market_data)
                if signal:
                    signal["mode"] = mode
                    signals.append(signal)
                    logger.info(f"Signal: {signal}")
            except Exception as e:
                logger.error(f"Strategy run failed ({symbol}): {e}")
        return signals

    def _fetch_market_data(self, symbol: str, account: Optional[str] = None) -> Optional[dict]:
        """Fetch market data: replay provider or ZuiLow API. Pass account so quote uses same gateway as order."""
        if self._quote_provider is not None:
            out = self._quote_provider(symbol)
            if out and "current_price" not in out and "price" in out:
                out = dict(out)
                out["current_price"] = out["price"]
            return out
        try:
            url = f"{self.api_base_url}/api/market/quote/{symbol}"
            if account:
                from urllib.parse import quote as url_quote
                url = f"{url}?account={url_quote(account, safe='')}"
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                out = response.json()
                if out and "current_price" not in out and ("price" in out or "Close" in out):
                    out = dict(out)
                    out["current_price"] = out.get("price") or out.get("Close") or 0
                return out
            logger.warning(f"Market data failed: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Market data request failed: {e}")
            return None
    
    def _execute_strategy_logic(
        self,
        strategy: Any,
        symbol: str,
        market_data: dict
    ) -> Optional[dict]:
        """Execute strategy logic: load history and run strategy."""
        try:
            now = self._get_now() if self._get_now else ctrl.get_current_dt()
            end_date = now.strftime("%Y-%m-%d")
            start_date = (now - timedelta(days=150)).strftime("%Y-%m-%d")
            history_data = self._fetch_history_data(symbol, start_date, end_date)
            if history_data is None or history_data.empty:
                logger.warning(f"Cannot get history for {symbol}")
                return None
            # Normalize column names to Open/High/Low/Close/Volume for strategy (e.g. ctx.history["Close"])
            hist = history_data.copy()
            for lower_name, cap_name in [("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close"), ("volume", "Volume")]:
                if lower_name in hist.columns and cap_name not in hist.columns:
                    hist = hist.rename(columns={lower_name: cap_name})
            from zuilow.components.backtest.strategy import StrategyContext
            from zuilow.components.backtest.types import Account
            
            context = StrategyContext(
                account=Account(initial_capital=100000, positions={}),
                history=hist,
            )
            
            last_signal = None
            last_bar  = None 
            for idx, row in history_data.iterrows():
                from zuilow.components.backtest.types import Bar
                
                bar = Bar(
                    symbol=symbol,
                    timestamp=idx,
                    open=row.get('open', row.get('Open', 0)),
                    high=row.get('high', row.get('High', 0)),
                    low=row.get('low', row.get('Low', 0)),
                    close=row.get('close', row.get('Close', 0)),
                    volume=row.get('volume', row.get('Volume', 0))
                )
                
                sig = strategy.on_bar(bar, context)
                last_bar = bar
                if sig is not None:
                    last_signal = sig
            
            def _price_from_market_data(md: dict, last_price: float) -> float:
                v = md.get("current_price") or md.get("price") or md.get("Close")
                try:
                    return float(v) if v is not None else last_price
                except (TypeError, ValueError):
                    return last_price

            if last_signal is not None:
                return {
                    "symbol": symbol,
                    "side": last_signal.type.value if hasattr(last_signal.type, "value") else str(last_signal.type),
                    "qty": 100,
                    "price": _price_from_market_data(market_data, last_bar.close),
                    "timestamp": now.isoformat(),
                }
            if hasattr(strategy, "get_last_signal"):
                signal = strategy.get_last_signal()
                if signal:
                    return {
                        "symbol": symbol,
                        "side": signal.type.value if hasattr(signal.type, "value") else str(signal.type),
                        "qty": 100,
                        "price": _price_from_market_data(market_data, last_bar.close),
                        "timestamp": now.isoformat(),
                    }
            logger.debug(f"Strategy done ({symbol}): no signal")
            return None
        except Exception as e:
            logger.error(f"Strategy logic failed ({symbol}): {e}")
            return None

    def _fetch_history_data(self, symbol: str, start: str, end: str):
        """Fetch history OHLCV: replay provider or ZuiLow API."""
        if self._history_provider is not None:
            df = self._history_provider(symbol, start, end)
            return df
        try:
            url = f"{self.api_base_url}/api/market/history"
            params = {
                "symbol": symbol,
                "start": start,
                "end": end,
                "ktype": "K_DAY"
            }
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                import pandas as pd
                data = response.json()
                
                if isinstance(data, dict) and 'data' in data:
                    return pd.DataFrame(data['data'])
                elif isinstance(data, list):
                    return pd.DataFrame(data)
            logger.warning(f"History failed: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"History request failed: {e}")
            return None

    def signals_dict_to_trading_signals(
        self,
        signals: list[dict],
        job_name: str,
        account: str,
        market: Optional[str] = None,
        trigger_at: Optional[datetime] = None,
    ) -> list[TradingSignal]:
        """
        Convert runner output (list of signal dicts) to TradingSignal list for storage.

        Args:
            signals: List of dicts with symbol, side, qty, price?, reason? (or target_weights/target_mv for rebalance)
            job_name: Scheduler job name
            account: Account name
            market: Optional market code; inferred from symbol prefix if None
            trigger_at: Optional desired execution time (for executor list_pending)

        Returns:
            List of TradingSignal (currently order-type only from dict with symbol/side/qty)
        """
        out: list[TradingSignal] = []
        for s in signals:
            # Allocation (资产配置): target_weights + 策略元数据（以下划线开头的键通用透传进 payload）
            if s.get("kind") == "allocation" and "target_weights" in s:
                m = market or (s.get("market") or "UNKNOWN")
                tw = s["target_weights"]
                if isinstance(tw, dict) and tw:
                    payload_extra = {k: v for k, v in s.items() if k.startswith("_")}
                    ts = TradingSignal.allocation(
                        job_name=job_name,
                        account=account,
                        market=m,
                        target_weights=tw,
                        trigger_at=trigger_at,
                        created_at=trigger_at,
                        **payload_extra,
                    )
                    out.append(ts)
                continue
            # Rebalance: target_weights and/or target_mv
            if s.get("kind") == "rebalance" or ("target_weights" in s or "target_mv" in s):
                m = market or (s.get("market") or "UNKNOWN")
                payload = {}
                if "target_weights" in s:
                    payload["target_weights"] = s["target_weights"]
                if "target_mv" in s:
                    payload["target_mv"] = s["target_mv"]
                if not payload:
                    continue
                ts = TradingSignal.rebalance(
                    job_name=job_name,
                    account=account,
                    market=m,
                    payload=payload,
                    trigger_at=trigger_at,
                    created_at=trigger_at,
                )
                out.append(ts)
                continue
            symbol = s.get("symbol", "")
            m = market or _infer_market(symbol)
            side = s.get("side", "buy").lower()
            qty = float(s.get("qty", 0) or 100)
            price = s.get("price")
            if price is not None:
                price = float(price)
            reason = s.get("reason", s.get("timestamp", ""))
            ts = TradingSignal.order(
                job_name=job_name,
                account=account,
                market=m,
                symbol=symbol,
                side=side,
                qty=qty,
                price=price,
                reason=str(reason),
                trigger_at=trigger_at,
                created_at=trigger_at,
            )
            out.append(ts)
        return out

    def write_signals_to_store(self, signals: list[TradingSignal]) -> list[int]:
        """
        Write TradingSignals to signal store (SignalStore.add_many).

        Args:
            signals: List of TradingSignal to insert

        Returns:
            List of inserted row ids
        """
        store = get_signal_store()
        return store.add_many(signals)

    def send_signals(self, signals: list[dict]) -> list[dict]:
        """
        Send signal dicts to trading gateway (POST /api/order per signal).
        When _send_dry_run is True (replay), only log and return mock results.
        In sim mode, X-Simulation-Time is filled by /api/order from tick context (set once at tick entry).
        """
        if self._send_dry_run:
            return [{"signal": s, "status": 200, "response": "replay_dry_run"} for s in signals]
        results = []
        for signal in signals:
            try:
                url = f"{self.api_base_url}/api/order"
                payload = {
                    "symbol": signal.get("symbol"),
                    "side": signal.get("side", "buy"),
                    "qty": signal.get("qty", 100),
                    "mode": signal.get("mode", "paper"),
                }
                # Include account so POST /api/order routes by account type (paper/futu/ibkr).
                if signal.get("account"):
                    payload["account"] = signal["account"]
                if signal.get("price") is not None:
                    payload["price"] = signal["price"]
                response = requests.post(url, json=payload, timeout=10)
                result = {
                    "signal": signal,
                    "status": response.status_code,
                    "response": response.json() if response.status_code == 200 else response.text
                }
                results.append(result)
                logger.info(f"Signal sent: {signal.get('symbol')} -> {result['status']}")
            except Exception as e:
                logger.error(f"Send signal failed: {e}")
                results.append({
                    "signal": signal,
                    "status": "error",
                    "response": str(e)
                })
        return results
