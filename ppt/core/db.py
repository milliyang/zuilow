"""
Paper Trade SQLite database: accounts, positions, orders, trades, equity history, watchlist.

Used for: all PPT API and simulation; DB_FILE env (default run/db/paper_trade.db).

Functions:
    get_db_path() -> str
    get_connection()   Context manager; commit on exit
    get_current_account_name() / set_current_account(name) / list_accounts() / create_account(...) / delete_account(name)
    get_positions(account) / update_position(...) / get_orders(account) / add_order(...) / get_trades(account) / add_trade(...)
    get_equity_history(account) / append_equity(...) / get_watchlist() / add_watchlist(...) / etc.

Features:
    - Uses core.utils.get_current_datetime_iso / get_equity_date for sim time
    - DEFAULT_CAPITAL, DEFAULT_WATCHLIST from env or defaults
"""
import os
import sqlite3
from datetime import datetime
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
import logging

get_logger = logging.getLogger(__name__)

# Use utils for current time so sim mode uses sim time
def _now_iso():
    from core.utils import get_current_datetime_iso
    return get_current_datetime_iso()

def _today_date():
    from core.utils import get_equity_date
    return get_equity_date()

def _equity_date_for_init():
    try:
        return _today_date()
    except Exception:
        return None

DB_FILE = os.getenv('DB_FILE', 'run/db/paper_trade.db')
DEFAULT_CAPITAL = 1000000

# Default watchlist (symbol, display name)
DEFAULT_WATCHLIST = [
    ('GOOGL', 'Google'),
    ('SPY', 'S&P 500 ETF'),
    ('QQQ', 'Nasdaq 100 ETF'),
    ('GLD', 'Gold ETF'),
    ('SLV', 'Silver ETF'),
    ('HK.00700', 'Tencent'),
]


def get_db_path() -> str:
    """获取数据库路径"""
    return DB_FILE


@contextmanager
def get_connection():
    """获取数据库连接"""
    os.makedirs(os.path.dirname(DB_FILE) or '.', exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # 返回字典形式
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """初始化数据库表"""
    with get_connection() as conn:
        conn.executescript('''
            -- 账户表
            CREATE TABLE IF NOT EXISTS accounts (
                name TEXT PRIMARY KEY,
                initial_capital REAL NOT NULL,
                cash REAL NOT NULL,
                created_at TEXT NOT NULL
            );
            
            -- 持仓表
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                qty INTEGER NOT NULL,
                avg_price REAL NOT NULL,
                UNIQUE(account_name, symbol),
                FOREIGN KEY (account_name) REFERENCES accounts(name)
            );
            
            -- 订单表
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                qty INTEGER NOT NULL,
                price REAL NOT NULL,
                value REAL NOT NULL,
                time TEXT NOT NULL,
                status TEXT NOT NULL,
                source TEXT DEFAULT 'web',
                FOREIGN KEY (account_name) REFERENCES accounts(name)
            );
            
            -- 成交表（commission/slippage/realized_pnl 用于统计累积亏损）
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                qty INTEGER NOT NULL,
                price REAL NOT NULL,
                value REAL NOT NULL,
                time TEXT NOT NULL,
                commission REAL DEFAULT 0,
                slippage REAL DEFAULT 0,
                realized_pnl REAL DEFAULT 0,
                FOREIGN KEY (account_name) REFERENCES accounts(name)
            );
            
            -- 净值历史表
            CREATE TABLE IF NOT EXISTS equity_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                date TEXT NOT NULL,
                equity REAL NOT NULL,
                pnl REAL NOT NULL,
                pnl_pct REAL NOT NULL,
                UNIQUE(account_name, date),
                FOREIGN KEY (account_name) REFERENCES accounts(name)
            );
            
            -- 设置表
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            
            -- 关注列表 (行情监控)
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                last_price REAL,
                last_update TEXT,
                status TEXT DEFAULT 'unknown',
                error TEXT
            );
        ''')
        
        # 初始化默认账户（如果不存在）；模拟时用 stime 当前日期，否则用服务器当天
        cursor = conn.execute("SELECT COUNT(*) FROM accounts")
        if cursor.fetchone()[0] == 0:
            as_of = _equity_date_for_init()
            create_account('default', DEFAULT_CAPITAL, as_of_date=as_of)
            set_current_account('default')
        
        # 初始化默认关注列表（如果为空）
        cursor = conn.execute("SELECT COUNT(*) FROM watchlist")
        if cursor.fetchone()[0] == 0:
            for symbol, name in DEFAULT_WATCHLIST:
                conn.execute(
                    "INSERT OR IGNORE INTO watchlist (symbol, name, status) VALUES (?, ?, 'pending')",
                    (symbol, name)
                )
            get_logger.info("db write init_db: default watchlist initialized symbols=%s", [s for s, _ in DEFAULT_WATCHLIST])


# ============================================================
# 账户操作
# ============================================================

def create_account(name: str, capital: float = DEFAULT_CAPITAL, as_of_date=None) -> bool:
    """Create account. as_of_date: sim date when in sim mode; else use get_equity_date() (real today)."""
    now_str = _now_iso()
    date_str = (as_of_date.strftime('%Y-%m-%d') if as_of_date is not None and hasattr(as_of_date, 'strftime')
                else str(as_of_date)[:10] if as_of_date is not None else _today_date().strftime('%Y-%m-%d'))
    with get_connection() as conn:
        try:
            conn.execute(
                "INSERT INTO accounts (name, initial_capital, cash, created_at) VALUES (?, ?, ?, ?)",
                (name, capital, capital, now_str)
            )
            # 初始净值记录（日期用 as_of_date 或当天，避免仿真时混入系统日期）
            conn.execute(
                "INSERT INTO equity_history (account_name, date, equity, pnl, pnl_pct) VALUES (?, ?, ?, 0, 0)",
                (name, date_str, capital)
            )
            get_logger.info("db write create_account: name=%s capital=%s date=%s", name, capital, date_str)
            return True
        except sqlite3.IntegrityError:
            return False


def delete_account(name: str) -> bool:
    """删除账户"""
    with get_connection() as conn:
        # 删除关联数据
        conn.execute("DELETE FROM positions WHERE account_name = ?", (name,))
        conn.execute("DELETE FROM orders WHERE account_name = ?", (name,))
        conn.execute("DELETE FROM trades WHERE account_name = ?", (name,))
        conn.execute("DELETE FROM equity_history WHERE account_name = ?", (name,))
        cursor = conn.execute("DELETE FROM accounts WHERE name = ?", (name,))
        n = cursor.rowcount
        get_logger.info("db write delete_account: name=%s rows_deleted=%s", name, n)
        return n > 0


def get_account(name: str) -> Optional[Dict]:
    """获取账户信息"""
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM accounts WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None


def get_all_accounts() -> List[Dict]:
    """获取所有账户"""
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM accounts ORDER BY created_at")
        return [dict(row) for row in cursor.fetchall()]


def update_account_cash(name: str, cash: float):
    """更新账户现金"""
    with get_connection() as conn:
        conn.execute("UPDATE accounts SET cash = ? WHERE name = ?", (cash, name))
        get_logger.info("db write update_account_cash: name=%s cash=%s", name, cash)


def reset_account(name: str, capital: float = None, as_of_date=None):
    """重置账户。as_of_date 为仿真日期时传入，否则用服务器当天，避免仿真时混入 2026/1/31 等系统日期。"""
    with get_connection() as conn:
        account = get_account(name)
        if not account:
            return False
        
        new_capital = capital or account['initial_capital']
        now_str = _now_iso()
        date_str = (as_of_date.strftime('%Y-%m-%d') if as_of_date is not None and hasattr(as_of_date, 'strftime')
                    else str(as_of_date)[:10] if as_of_date is not None else _today_date().strftime('%Y-%m-%d'))
        
        conn.execute("DELETE FROM positions WHERE account_name = ?", (name,))
        conn.execute("DELETE FROM orders WHERE account_name = ?", (name,))
        conn.execute("DELETE FROM trades WHERE account_name = ?", (name,))
        conn.execute("DELETE FROM equity_history WHERE account_name = ?", (name,))
        
        conn.execute(
            "UPDATE accounts SET initial_capital = ?, cash = ?, created_at = ? WHERE name = ?",
            (new_capital, new_capital, now_str, name)
        )
        
        # 初始净值（日期用 as_of_date 或当天）
        conn.execute(
            "INSERT INTO equity_history (account_name, date, equity, pnl, pnl_pct) VALUES (?, ?, ?, 0, 0)",
            (name, date_str, new_capital)
        )
        get_logger.info("db write reset_account: name=%s new_capital=%s date=%s", name, new_capital, date_str)
        return True


# ============================================================
# 当前账户
# ============================================================

def get_current_account_name() -> str:
    """获取当前账户名"""
    with get_connection() as conn:
        cursor = conn.execute("SELECT value FROM settings WHERE key = 'current_account'")
        row = cursor.fetchone()
        return row['value'] if row else 'default'


def set_current_account(name: str):
    """设置当前账户"""
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('current_account', ?)",
            (name,)
        )
        get_logger.info("db write set_current_account: name=%s", name)


# ============================================================
# 持仓操作
# ============================================================

def get_positions(account_name: str) -> Dict[str, Dict]:
    """获取持仓"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE account_name = ?",
            (account_name,)
        )
        return {row['symbol']: {'qty': row['qty'], 'avg_price': row['avg_price']} 
                for row in cursor.fetchall()}


def update_position(account_name: str, symbol: str, qty: int, avg_price: float):
    """更新持仓"""
    with get_connection() as conn:
        if qty <= 0:
            conn.execute(
                "DELETE FROM positions WHERE account_name = ? AND symbol = ?",
                (account_name, symbol)
            )
            get_logger.info("db write update_position: account=%s symbol=%s action=delete", account_name, symbol)
        else:
            conn.execute('''
                INSERT INTO positions (account_name, symbol, qty, avg_price)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(account_name, symbol) 
                DO UPDATE SET qty = ?, avg_price = ?
            ''', (account_name, symbol, qty, avg_price, qty, avg_price))
            get_logger.info("db write update_position: account=%s symbol=%s qty=%s avg_price=%s",
                            account_name, symbol, qty, avg_price)


def get_position(account_name: str, symbol: str) -> Optional[Dict]:
    """获取单个持仓"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT qty, avg_price FROM positions WHERE account_name = ? AND symbol = ?",
            (account_name, symbol)
        )
        row = cursor.fetchone()
        return {'qty': row['qty'], 'avg_price': row['avg_price']} if row else None


# ============================================================
# 订单操作
# ============================================================

def add_order(account_name: str, symbol: str, side: str, qty: int, 
              price: float, status: str = 'filled', source: str = 'web', order_time=None) -> int:
    """Add order. order_time: optional datetime for sim mode (X-Simulation-Time)."""
    now = (order_time.isoformat() if order_time is not None else _now_iso())
    value = qty * price
    with get_connection() as conn:
        cursor = conn.execute('''
            INSERT INTO orders (account_name, symbol, side, qty, price, value, time, status, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (account_name, symbol, side, qty, price, value, now, status, source))
        order_id = cursor.lastrowid
        get_logger.info("db write add_order: order_id=%s account=%s symbol=%s side=%s qty=%s price=%s",
                        order_id, account_name, symbol, side, qty, price)
        return order_id


def get_orders(account_name: str, limit: int = 100) -> List[Dict]:
    """获取订单历史"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT * FROM orders WHERE account_name = ? ORDER BY id DESC LIMIT ?",
            (account_name, limit)
        )
        return [dict(row) for row in cursor.fetchall()]


# ============================================================
# 成交操作
# ============================================================

def add_trade(account_name: str, symbol: str, side: str, qty: int, price: float, order_time=None,
              commission: float = 0, slippage: float = 0, realized_pnl: float = 0) -> int:
    """Add trade. order_time: optional datetime for sim mode."""
    now = (order_time.isoformat() if order_time is not None else _now_iso())
    value = qty * price
    with get_connection() as conn:
        cursor = conn.execute('''
            INSERT INTO trades (account_name, symbol, side, qty, price, value, time, commission, slippage, realized_pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (account_name, symbol, side, qty, price, value, now, commission, slippage, realized_pnl))
        trade_id = cursor.lastrowid
        get_logger.info("db write add_trade: trade_id=%s account=%s symbol=%s side=%s qty=%s price=%s commission=%s slippage=%s realized_pnl=%s",
                        trade_id, account_name, symbol, side, qty, price, commission, slippage, realized_pnl)
        return trade_id


def get_trades(account_name: str, limit: int = 100) -> List[Dict]:
    """获取成交记录"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT * FROM trades WHERE account_name = ? ORDER BY id DESC LIMIT ?",
            (account_name, limit)
        )
        return [dict(row) for row in cursor.fetchall()]


def get_account_cost_stats(account_name: str) -> Dict[str, float]:
    """
    账户累积亏损统计：手续费、滑点、市场(已实现盈亏)。
    从 trades 表 SUM(commission), SUM(slippage), SUM(realized_pnl)；旧数据无列时按 0 计。
    """
    with get_connection() as conn:
        try:
            cursor = conn.execute('''
                SELECT COALESCE(SUM(commission), 0), COALESCE(SUM(slippage), 0), COALESCE(SUM(realized_pnl), 0)
                FROM trades WHERE account_name = ?
            ''', (account_name,))
            row = cursor.fetchone()
            if row is not None:
                return {
                    'total_commission': float(row[0]),
                    'total_slippage': float(row[1]),
                    'total_realized_pnl': float(row[2]),
                }
        except Exception:
            pass
    return {'total_commission': 0.0, 'total_slippage': 0.0, 'total_realized_pnl': 0.0}


# ============================================================
# 净值历史
# ============================================================

def update_equity_history(account_name: str, quotes: dict = None, as_of_date=None):
    """
    更新净值历史
    
    Args:
        account_name: 账户名
        quotes: 实时行情 {symbol: {'price': float}}
                如果提供则用市价，否则用成本价
        as_of_date: 净值日期 (datetime/date 或 'YYYY-MM-DD')；仿真时传 X-Simulation-Time 的日期，否则用服务器当天
    """
    account = get_account(account_name)
    if not account:
        return

    positions = get_positions(account_name)

    # 计算持仓市值：优先行情价；行情失败（503/超时/无数据）或 price<=0 时用买入成本价
    position_value = 0
    position_details = []
    for symbol, pos in positions.items():
        qty = pos['qty']
        avg_price = pos['avg_price']
        use_quote = (
            quotes and symbol in quotes
            and quotes[symbol].get('valid', True)
            and (quotes[symbol].get('price') or 0) > 0
        )
        if use_quote:
            price_used = quotes[symbol].get('price') or 0
            mv = qty * price_used
            position_value += mv
            position_details.append((symbol, qty, avg_price, price_used, mv, 'quote'))
        else:
            mv = qty * avg_price
            position_value += mv
            price_used = avg_price
            err = (quotes.get(symbol, {}).get('error') or 'no quote') if quotes else 'no quotes'
            position_details.append((symbol, qty, avg_price, price_used, mv, 'cost(%s)' % err))

    equity = account['cash'] + position_value
    pnl = equity - account['initial_capital']
    pnl_pct = (pnl / account['initial_capital']) * 100 if account['initial_capital'] > 0 else 0

    if as_of_date is not None:
        if hasattr(as_of_date, 'strftime'):
            date_str = as_of_date.strftime('%Y-%m-%d')
        else:
            date_str = str(as_of_date)[:10]
    else:
        date_str = _today_date().strftime('%Y-%m-%d')

    for sym, q, avg, pused, mv, src in position_details:
        get_logger.info("equity position: account=%s date=%s symbol=%s qty=%s avg_price=%s price_used=%s source=%s mv=%s",
                        account_name, date_str, sym, q, avg, pused, src, mv)
    get_logger.info("update equity history: account=%s date=%s cash=%s position_value=%s equity=%s pnl=%s pnl_pct=%s",
                    account_name, date_str, account['cash'], position_value, equity, pnl, pnl_pct)


    with get_connection() as conn:
        conn.execute('''
            INSERT INTO equity_history (account_name, date, equity, pnl, pnl_pct)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(account_name, date)
            DO UPDATE SET equity = ?, pnl = ?, pnl_pct = ?
        ''', (account_name, date_str, equity, pnl, pnl_pct, equity, pnl, pnl_pct))


def get_equity_history(account_name: str) -> List[Dict]:
    """获取净值历史"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT date, equity, pnl, pnl_pct FROM equity_history WHERE account_name = ? ORDER BY date",
            (account_name,)
        )
        return [dict(row) for row in cursor.fetchall()]


def get_equity_history_dates() -> set:
    """返回 equity_history 中已存在的所有日期（任意账户），用于仿真模式下初始化「已更新日期」集合，避免重启后漏回填。"""
    with get_connection() as conn:
        cursor = conn.execute("SELECT DISTINCT date FROM equity_history")
        return {row[0] for row in cursor.fetchall() if row[0]}


def get_min_equity_date(account_name: str) -> Optional[str]:
    """返回该账户在 equity_history 中的最早日期（YYYY-MM-DD），用于仿真时跳过「早于账户首日」的 tick。"""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT MIN(date) FROM equity_history WHERE account_name = ?",
            (account_name,)
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] else None


def get_equity_at_date(account_name: str, as_of_date) -> Optional[Dict]:
    """
    返回指定日期的净值记录；若无该日则返回该日之前最近一日的记录。
    用于仿真模式下账户概览显示「当日 EOD 净值」。
    as_of_date: date 或 'YYYY-MM-DD'
    """
    if as_of_date is None:
        return None
    if hasattr(as_of_date, 'strftime'):
        date_str = as_of_date.strftime('%Y-%m-%d')
    else:
        date_str = str(as_of_date)[:10]
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT date, equity, pnl, pnl_pct FROM equity_history WHERE account_name = ? AND date <= ? ORDER BY date DESC LIMIT 1",
            (account_name, date_str)
        )
        row = cursor.fetchone()
        return dict(row) if row else None


# ============================================================
# 计算函数
# ============================================================

def calc_equity(account_name: str) -> float:
    """计算账户净值"""
    account = get_account(account_name)
    if not account:
        return 0
    
    positions = get_positions(account_name)
    position_value = sum(p['qty'] * p['avg_price'] for p in positions.values())
    return account['cash'] + position_value


# ============================================================
# 数据迁移
# ============================================================

def migrate_from_json(json_file: str):
    """从 JSON 文件迁移数据"""
    import json
    
    if not os.path.exists(json_file):
        print(f"JSON 文件不存在: {json_file}")
        return False
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    accounts = data.get('accounts', {})
    current = data.get('current_account', 'default')
    
    for name, acc in accounts.items():
        # 创建账户
        with get_connection() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO accounts (name, initial_capital, cash, created_at)
                VALUES (?, ?, ?, ?)
            ''', (name, acc['initial_capital'], acc['cash'], acc['created_at']))
        
        # 导入持仓
        for symbol, pos in acc.get('positions', {}).items():
            update_position(name, symbol, pos['qty'], pos['avg_price'])
        
        # 导入订单
        with get_connection() as conn:
            for order in acc.get('orders', []):
                conn.execute('''
                    INSERT INTO orders (account_name, symbol, side, qty, price, value, time, status, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (name, order['symbol'], order['side'], order['qty'], order['price'],
                      order['value'], order['time'], order['status'], order.get('source', 'web')))
        
        # 导入成交
        with get_connection() as conn:
            for trade in acc.get('trades', []):
                conn.execute('''
                    INSERT INTO trades (account_name, symbol, side, qty, price, value, time)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (name, trade['symbol'], trade['side'], trade['qty'], trade['price'],
                      trade['value'], trade['time']))
        
        # 导入净值历史
        with get_connection() as conn:
            for eq in acc.get('equity_history', []):
                conn.execute('''
                    INSERT OR REPLACE INTO equity_history (account_name, date, equity, pnl, pnl_pct)
                    VALUES (?, ?, ?, ?, ?)
                ''', (name, eq['date'], eq['equity'], eq['pnl'], eq['pnl_pct']))
    
    set_current_account(current)
    print(f"成功迁移 {len(accounts)} 个账户")
    return True


# ============================================================
# 关注列表 (行情监控)
# ============================================================

def get_watchlist() -> List[Dict]:
    """获取关注列表"""
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM watchlist ORDER BY symbol")
        return [dict(row) for row in cursor.fetchall()]


def add_to_watchlist(symbol: str, name: str = None) -> bool:
    """添加到关注列表"""
    with get_connection() as conn:
        try:
            conn.execute(
                "INSERT INTO watchlist (symbol, name, status) VALUES (?, ?, 'pending')",
                (symbol.upper(), name or symbol)
            )
            get_logger.info("db write add_to_watchlist: symbol=%s name=%s", symbol.upper(), name or symbol)
            return True
        except sqlite3.IntegrityError:
            return False


def remove_from_watchlist(symbol: str) -> bool:
    """从关注列表移除"""
    with get_connection() as conn:
        cursor = conn.execute("DELETE FROM watchlist WHERE symbol = ?", (symbol.upper(),))
        n = cursor.rowcount
        get_logger.info("db write remove_from_watchlist: symbol=%s rows_deleted=%s", symbol.upper(), n)
        return n > 0


def update_watchlist_quote(symbol: str, price: float, name: str = None, 
                           status: str = 'ok', error: str = None):
    """Update watchlist quote (last_price, last_update). Uses get_current_datetime_iso (sim/real)."""
    now = _now_iso()
    with get_connection() as conn:
        conn.execute('''
            UPDATE watchlist 
            SET last_price = ?, last_update = ?, status = ?, error = ?, name = COALESCE(?, name)
            WHERE symbol = ?
        ''', (price, now, status, error, name, symbol.upper()))
        get_logger.info("db write update_watchlist_quote: symbol=%s price=%s status=%s", symbol.upper(), price, status)


def clear_watchlist():
    """清空关注列表"""
    with get_connection() as conn:
        cursor = conn.execute("DELETE FROM watchlist")
        get_logger.info("db write clear_watchlist: rows_deleted=%s", cursor.rowcount)


def init_default_watchlist() -> dict:
    """导入默认关注列表（跳过已存在的）"""
    added = []
    skipped = []
    
    with get_connection() as conn:
        for symbol, name in DEFAULT_WATCHLIST:
            try:
                conn.execute(
                    "INSERT INTO watchlist (symbol, name, status) VALUES (?, ?, 'pending')",
                    (symbol, name)
                )
                added.append(symbol)
            except sqlite3.IntegrityError:
                skipped.append(symbol)
    get_logger.info("db write init_default_watchlist: added=%s skipped=%s", added, skipped)
    return {'added': added, 'skipped': skipped}


# 初始化数据库
init_db()
