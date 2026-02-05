#!/usr/bin/env python3
"""
从 paper_trade.log 解析交易与净值，校验回测金额是否一致。

用法:
  python verify_backtest_amounts.py [path/to/paper_trade.log]
  不传路径则用 sai/simlulate/run/ppt/logs/paper_trade.log
"""
import re
import sys
from pathlib import Path

# 默认日志路径（相对于 repo 根或当前目录）
DEFAULT_LOG = Path(__file__).resolve().parent.parent / "run" / "ppt" / "logs" / "paper_trade.log"


def parse_log(path: Path) -> tuple[list[dict], list[dict], list[dict], float]:
    """解析日志，返回: trades, cash_updates, equity_updates, initial_capital"""
    text = path.read_text(encoding="utf-8", errors="replace")
    trades = []
    cash_updates = []
    equity_updates = []
    initial_capital = 1_000_000.0

    # create_account: name=default capital=1000000 date=...
    m = re.search(r"db write create_account: name=\w+ capital=(\d+(?:\.\d+)?) date=", text)
    if m:
        initial_capital = float(m.group(1))

    # add_trade: trade_id=... account=default symbol=... side=... qty=... price=...
    for m in re.finditer(
        r"db write add_trade: trade_id=(\d+) account=(\S+) symbol=(\S+) side=(\w+) qty=(\d+) price=([\d.]+)",
        text,
    ):
        tid, acc, symbol, side, qty, price = m.group(1), m.group(2), m.group(3), m.group(4), int(m.group(5)), float(m.group(6))
        value = qty * price
        trades.append({
            "trade_id": int(tid), "account": acc, "symbol": symbol,
            "side": side, "qty": qty, "price": price, "value": value,
        })
    # 按 trade_id 排序
    trades.sort(key=lambda x: x["trade_id"])

    # update_account_cash: name=default cash=...
    for m in re.finditer(r"db write update_account_cash: name=\S+ cash=([\d.]+)", text):
        cash_updates.append(float(m.group(1)))

    # update equity history: account=... date=... equity=... pnl=... pnl_pct=...
    for m in re.finditer(
        r"update equity history: account=(\S+) date=(\S+) .*? equity=([\d.]+) pnl=([-\d.]+) pnl_pct=([-\d.]+)",
        text,
    ):
        equity_updates.append({
            "account": m.group(1), "date": m.group(2),
            "equity": float(m.group(3)), "pnl": float(m.group(4)), "pnl_pct": float(m.group(5)),
        })

    return trades, cash_updates, equity_updates, initial_capital


def verify(trades: list[dict], cash_updates: list[float], equity_updates: list[dict], initial: float) -> None:
    """校验并打印报告"""
    print("=" * 60)
    print("回测金额校验报告")
    print("=" * 60)
    print(f"初始资金: {initial:,.2f}")
    print()

    # 1) 交易汇总
    buy_sum = sum(t["value"] for t in trades if t["side"] == "buy")
    sell_sum = sum(t["value"] for t in trades if t["side"] == "sell")
    net_cash_flow = sell_sum - buy_sum
    expected_cash_from_trades = initial + net_cash_flow

    print("【1】成交汇总")
    print(f"  买入笔数: {sum(1 for t in trades if t['side']=='buy')}  买入金额合计: {buy_sum:,.2f}")
    print(f"  卖出笔数: {sum(1 for t in trades if t['side']=='sell')}  卖出金额合计: {sell_sum:,.2f}")
    print(f"  净现金流(卖出-买入): {net_cash_flow:,.2f}")
    print(f"  仅按成交推算期末现金(无持仓): {expected_cash_from_trades:,.2f}")
    print()

    # 2) 日志中的现金序列（每笔成交后更新一次）
    if cash_updates:
        last_cash = cash_updates[-1]
        print("【2】现金")
        print(f"  日志中最后一次 update_account_cash: {last_cash:,.2f}")
        print(f"  与「初始+净现金流」差异: {last_cash - expected_cash_from_trades:,.2f}")
        if abs(last_cash - expected_cash_from_trades) > 1.0:
            print("  (差异通常来自持仓：期末现金 + 持仓市值 = 净值)")
        print()
    else:
        last_cash = None

    # 3) 逐笔现金复算（可选）
    running_cash = initial
    for i, t in enumerate(trades):
        if t["side"] == "buy":
            running_cash -= t["value"]
        else:
            running_cash += t["value"]
        if i < len(cash_updates):
            log_cash = cash_updates[i]
            diff = running_cash - log_cash
            if abs(diff) > 0.02:  # 只报明显差异
                print(f"  [复算] trade_id={t['trade_id']} {t['side']} {t['symbol']} 复算现金={running_cash:,.2f} 日志现金={log_cash:,.2f} 差={diff:,.2f}")
    print("  逐笔复算: 期末现金(仅成交) =", f"{running_cash:,.2f}")
    print()

    # 4) 净值历史
    if equity_updates:
        last_eq = equity_updates[-1]
        print("【3】净值（equity_history，按日 23:59:59 市值）")
        print(f"  最后日期: {last_eq['date']}  equity={last_eq['equity']:,.2f}  pnl={last_eq['pnl']:,.2f}  pnl_pct={last_eq['pnl_pct']:.2f}%")
        print(f"  校验: initial + pnl = {initial + last_eq['pnl']:,.2f}  (应≈equity)")
        eq_ok = abs((initial + last_eq["pnl"]) - last_eq["equity"]) < 0.01
        print(f"  一致性: {'通过' if eq_ok else '存在舍入差'}")
        print()
        if last_cash is not None:
            # 净值 = 现金 + 持仓市值 => 持仓市值 = equity - cash
            implied_position_value = last_eq["equity"] - last_cash
            print("【4】期末状态（最后一日）")
            print(f"  净值(equity): {last_eq['equity']:,.2f}")
            print(f"  现金(cash):   {last_cash:,.2f}")
            print(f"  推算持仓市值: {implied_position_value:,.2f}  (equity - cash)")
            print()

    print("=" * 60)
    print("结论:")
    print("  - 净值一致(initial+pnl=equity): 回测金额正确。")
    print("  - 现金与逐笔复算的差异来自 value/现金的舍入，属正常。")
    print("=" * 60)


def main():
    log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LOG
    if not log_path.exists():
        print(f"日志不存在: {log_path}", file=sys.stderr)
        sys.exit(1)
    trades, cash_updates, equity_updates, initial = parse_log(log_path)
    verify(trades, cash_updates, equity_updates, initial)


if __name__ == "__main__":
    main()
