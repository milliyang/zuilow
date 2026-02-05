"""
Trading signal storage (SQLite).

CRUD for TradingSignal; query by account, market, status, kind, trigger_at.
Used by scheduler runner (add/add_many) and executor (list_pending, update_status).

Classes:
    SignalStore   SQLite store for TradingSignal

SignalStore methods:
    .add(signal: TradingSignal) -> int
    .add_many(signals: list[TradingSignal]) -> list[int]
    .get(signal_id: int) -> Optional[TradingSignal]
    .list_pending(account=None, market=None, trigger_at_before=None) -> list[TradingSignal]
    .list_signals(..., date_from=None, date_to=None, offset=0, limit=200) -> list[TradingSignal]
    .count_signals(account=None, market=None, status=None, kind=None, date_from=None, date_to=None) -> int
    .update_status(signal_id: int, status: SignalStatus, executed_at=None) -> bool
    .cancel(signal_id: int) -> bool

SignalStore config:
    Default DB path: run/db/signals.db (see get_default_db_path())

SignalStore features:
    - Single table trading_signals with indexes on account, market, status, trigger_at
    - list_pending: status=PENDING and (trigger_at IS NULL or trigger_at <= trigger_at_before)
    - list_signals: general filter for API / UI; ordered by created_at DESC

Functions:
    get_default_db_path() -> Path
    get_signal_store(db_path=None) -> SignalStore
    set_signal_store(store: Optional[SignalStore]) -> None
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from zuilow.components.control import ctrl

from .signal_models import TradingSignal, SignalKind, SignalStatus

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH: Path | None = None


def get_default_db_path() -> Path:
    """
    Default DB path for signal store.

    Returns:
        Path to zuilow/run/db/signals.db
    """
    global _DEFAULT_DB_PATH
    if _DEFAULT_DB_PATH is None:
        _DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent.parent / "run" / "db" / "signals.db"
    return _DEFAULT_DB_PATH


class SignalStore:
    """
    SQLite store for TradingSignal.

    Supports:
    - Insert single or many signals; get by id; list with filters
    - list_pending for executor (PENDING + trigger_at_before)
    - list_signals for API/UI (account, market, status, kind, limit)
    - update_status / cancel for lifecycle
    """

    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or get_default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        conn = self._conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS trading_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name TEXT NOT NULL,
                    account TEXT NOT NULL,
                    market TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    symbol TEXT,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    executed_at TEXT,
                    trigger_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_signals_account ON trading_signals(account);
                CREATE INDEX IF NOT EXISTS idx_signals_market ON trading_signals(market);
                CREATE INDEX IF NOT EXISTS idx_signals_status ON trading_signals(status);
                CREATE INDEX IF NOT EXISTS idx_signals_trigger_at ON trading_signals(trigger_at);
            """)
            conn.commit()
        finally:
            conn.close()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.db_path))
        c.row_factory = sqlite3.Row
        return c

    def add(self, signal: TradingSignal) -> int:
        """
        Insert one signal.

        Args:
            signal: TradingSignal to insert

        Returns:
            Inserted row id (int)
        """
        conn = self._conn()
        try:
            cur = conn.execute(
                """
                INSERT INTO trading_signals
                (job_name, account, market, kind, symbol, payload, status, created_at, executed_at, trigger_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.job_name,
                    signal.account,
                    signal.market,
                    signal.kind.value,
                    signal.symbol,
                    json.dumps(signal.payload),
                    signal.status.value,
                    signal.created_at.isoformat() if signal.created_at else None,
                    signal.executed_at.isoformat() if signal.executed_at else None,
                    signal.trigger_at.isoformat() if signal.trigger_at else None,
                ),
            )
            conn.commit()
            row_id = cur.lastrowid or 0
            logger.info("db write signal add: id=%s job_name=%s account=%s kind=%s symbol=%s",
                        row_id, signal.job_name, signal.account, signal.kind.value, signal.symbol)
            return row_id
        finally:
            conn.close()

    def add_many(self, signals: list[TradingSignal]) -> list[int]:
        """
        Insert multiple signals.

        Args:
            signals: List of TradingSignal to insert

        Returns:
            List of inserted row ids
        """
        ids: list[int] = []
        for s in signals:
            ids.append(self.add(s))
        logger.info("db write signal add_many: count=%s ids=%s", len(ids), ids)
        return ids

    def get(self, signal_id: int) -> TradingSignal | None:
        """
        Get one signal by id.

        Args:
            signal_id: Primary key

        Returns:
            TradingSignal or None if not found
        """
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM trading_signals WHERE id = ?", (signal_id,)).fetchone()
            return self._row_to_signal(row) if row else None
        finally:
            conn.close()

    def list_pending(
        self,
        account: str | None = None,
        market: str | None = None,
        trigger_at_before: datetime | None = None,
    ) -> list[TradingSignal]:
        """
        List pending signals (status=PENDING, optional account/market, trigger_at <= trigger_at_before).

        Args:
            account: Optional account filter
            market: Optional market filter
            trigger_at_before: Only signals with trigger_at <= this time (or NULL)

        Returns:
            List of TradingSignal, ordered by created_at ASC
        """
        conn = self._conn()
        try:
            sql = "SELECT * FROM trading_signals WHERE status = ?"
            params: list[Any] = [SignalStatus.PENDING.value]
            if account is not None:
                sql += " AND account = ?"
                params.append(account)
            if market is not None:
                sql += " AND market = ?"
                params.append(market)
            if trigger_at_before is not None:
                sql += " AND (trigger_at IS NULL OR trigger_at <= ?)"
                params.append(trigger_at_before.isoformat())
            sql += " ORDER BY created_at ASC"
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_signal(r) for r in rows]
        finally:
            conn.close()

    def count_signals(
        self,
        account: str | None = None,
        market: str | None = None,
        status: str | None = None,
        kind: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> int:
        """
        Count signals with same filters as list_signals (no offset/limit).
        date_from / date_to: YYYY-MM-DD, filter by created_at date range.
        """
        conn = self._conn()
        try:
            where_sql, params = self._list_signals_where(
                account, market, status, kind, date_from, date_to
            )
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM trading_signals WHERE 1=1" + where_sql,
                params,
            ).fetchone()
            return row["n"] if row else 0
        finally:
            conn.close()

    def _list_signals_where(
        self,
        account: str | None,
        market: str | None,
        status: str | None,
        kind: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> tuple[str, list[Any]]:
        sql = ""
        params: list[Any] = []
        if account is not None:
            sql += " AND account = ?"
            params.append(account)
        if market is not None:
            sql += " AND market = ?"
            params.append(market)
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        if kind is not None:
            sql += " AND kind = ?"
            params.append(kind)
        if date_from:
            sql += " AND created_at >= ?"
            params.append(date_from + "T00:00:00")
        if date_to:
            sql += " AND created_at <= ?"
            params.append(date_to + "T23:59:59")
        return sql, params

    def list_signals(
        self,
        account: str | None = None,
        market: str | None = None,
        status: str | None = None,
        kind: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        offset: int = 0,
        limit: int = 200,
    ) -> list[TradingSignal]:
        """
        List signals with optional filters (for API / UI).

        Args:
            account: Optional account filter
            market: Optional market filter
            status: Optional status filter (pending, executed, failed, cancelled)
            kind: Optional kind filter (order, rebalance)
            date_from: Optional start date YYYY-MM-DD (inclusive)
            date_to: Optional end date YYYY-MM-DD (inclusive)
            offset: Skip N rows (pagination)
            limit: Max rows (default 200)

        Returns:
            List of TradingSignal, ordered by created_at DESC
        """
        conn = self._conn()
        try:
            where_sql, params = self._list_signals_where(
                account, market, status, kind, date_from, date_to
            )
            params.extend([max(1, min(limit, 500)), max(0, offset)])
            sql = "SELECT * FROM trading_signals WHERE 1=1" + where_sql + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_signal(r) for r in rows]
        finally:
            conn.close()

    def update_status(
        self,
        signal_id: int,
        status: SignalStatus,
        executed_at: datetime | None = None,
    ) -> bool:
        """
        Update signal status and optionally executed_at.

        Args:
            signal_id: Primary key
            status: New status (e.g. SignalStatus.EXECUTED)
            executed_at: Optional timestamp when executed

        Returns:
            True if row was updated
        """
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE trading_signals SET status = ?, executed_at = ? WHERE id = ?",
                (status.value, executed_at.isoformat() if executed_at else None, signal_id),
            )
            conn.commit()
            updated = conn.total_changes > 0
            if updated:
                logger.info("db write signal update_status: signal_id=%s status=%s executed_at=%s",
                            signal_id, status.value, executed_at)
            return updated
        finally:
            conn.close()

    def cancel(self, signal_id: int) -> bool:
        """
        Set signal status to CANCELLED.

        Args:
            signal_id: Primary key

        Returns:
            True if row was updated
        """
        return self.update_status(signal_id, SignalStatus.CANCELLED)

    def _row_to_signal(self, row: sqlite3.Row) -> TradingSignal:
        def parse_iso(s: str | None) -> datetime | None:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        payload = json.loads(row["payload"]) if row["payload"] else {}
        return TradingSignal(
            id=row["id"],
            job_name=row["job_name"],
            account=row["account"],
            market=row["market"],
            kind=SignalKind(row["kind"]),
            symbol=row["symbol"],
            payload=payload,
            status=SignalStatus(row["status"]),
            created_at=parse_iso(row["created_at"]) or ctrl.get_current_dt(),
            executed_at=parse_iso(row["executed_at"]),
            trigger_at=parse_iso(row["trigger_at"]),
        )


_store: SignalStore | None = None


def get_signal_store(db_path: Path | None = None) -> SignalStore:
    """
    Get global signal store instance (singleton).

    Args:
        db_path: Optional DB path; default from get_default_db_path()

    Returns:
        SignalStore instance
    """
    global _store
    if _store is None:
        _store = SignalStore(db_path)
    return _store


def set_signal_store(store: SignalStore | None) -> None:
    """
    Set global signal store (e.g. for tests).

    Args:
        store: SignalStore instance or None to reset
    """
    global _store
    _store = store
