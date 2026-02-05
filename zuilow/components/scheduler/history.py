"""
Job run history: store and query scheduler job runs (SQLite).

Classes:
    JobHistory   Single run record dataclass
    HistoryDB    SQLite CRUD for job history

JobHistory fields:
    id, job_name, strategy, symbols (JSON), trigger_time, start_time, end_time,
    status (running/success/failed), signals_count, signals (JSON), error_message
    .to_dict() -> dict

HistoryDB methods:
    .add_history(history: JobHistory) -> int
    .update_history(history_id, end_time=None, status=None, signals_count=None, signals=None, error_message=None) -> bool
    .get_history(job_name: Optional[str] = None, limit: int = 30) -> list[JobHistory]
    .get_statistics() -> dict   (total_runs, success_count, failed_count, total_signals)

HistoryDB config:
    Default DB path: run/db/scheduler_history.db

Functions:
    get_history_db(db_path: Optional[Path] = None) -> HistoryDB

"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass, asdict
import json

logger = logging.getLogger(__name__)


def _default_history_db_path() -> Path:
    """Default path for scheduler_history.db. Prefer ZUILOW_RUN_DIR env if set."""
    run_dir = os.environ.get("ZUILOW_RUN_DIR")
    if run_dir:
        return Path(run_dir) / "db" / "scheduler_history.db"
    return Path(__file__).parent.parent.parent / "run" / "db" / "scheduler_history.db"


@dataclass
class JobHistory:
    """Job run history record."""
    id: Optional[int] = None
    job_name: str = ""
    strategy: str = ""
    symbols: str = ""  # JSON string
    trigger_time: str = ""  # ISO format
    start_time: str = ""
    end_time: Optional[str] = None
    status: str = "running"  # running, success, failed
    signals_count: int = 0
    signals: str = ""  # JSON string
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dict."""
        return asdict(self)


class HistoryDB:
    """Job history database."""

    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            db_path = _default_history_db_path()

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

    def _init_db(self):
        """Initialize DB tables."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    symbols TEXT NOT NULL,
                    trigger_time TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    status TEXT NOT NULL,
                    signals_count INTEGER DEFAULT 0,
                    signals TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_job_name 
                ON job_history(job_name)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_trigger_time 
                ON job_history(trigger_time)
            """)
            
            conn.commit()
            logger.info("History DB initialized: %s", self.db_path)

        except Exception as e:
            logger.error("DB init failed: %s", e)
        finally:
            conn.close()

    def _get_connection(self) -> sqlite3.Connection:
        """Get DB connection."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn
    
    def add_history(self, history: JobHistory) -> int:
        """Add history record."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO job_history 
                (job_name, strategy, symbols, trigger_time, start_time, 
                 end_time, status, signals_count, signals, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                history.job_name,
                history.strategy,
                history.symbols,
                history.trigger_time,
                history.start_time,
                history.end_time,
                history.status,
                history.signals_count,
                history.signals,
                history.error_message
            ))
            
            conn.commit()
            row_id = cursor.lastrowid
            logger.info("db write history add_history: id=%s job_name=%s strategy=%s trigger_time=%s status=%s",
                        row_id, history.job_name, history.strategy, history.trigger_time, history.status)
            return row_id
            
        except Exception as e:
            logger.error("Failed to add history: %s", e)
            return -1
        finally:
            conn.close()
    
    def update_history(self, history_id: int, **kwargs):
        """Update history record."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Build UPDATE dynamically
            fields = []
            values = []
            for key, value in kwargs.items():
                fields.append(f"{key} = ?")
                values.append(value)
            
            values.append(history_id)
            
            query = f"""
                UPDATE job_history 
                SET {', '.join(fields)}
                WHERE id = ?
            """
            
            cursor.execute(query, values)
            conn.commit()
            logger.info("db write history update_history: history_id=%s kwargs=%s", history_id, list(kwargs.keys()))
            
        except Exception as e:
            logger.error("Failed to update history: %s", e)
        finally:
            conn.close()
    
    def get_history(
        self,
        job_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[JobHistory]:
        """Query history records."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            if job_name:
                cursor.execute("""
                    SELECT * FROM job_history 
                    WHERE job_name = ?
                    ORDER BY trigger_time DESC
                    LIMIT ? OFFSET ?
                """, (job_name, limit, offset))
            else:
                cursor.execute("""
                    SELECT * FROM job_history 
                    ORDER BY trigger_time DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))
            
            rows = cursor.fetchall()
            
            histories = []
            for row in rows:
                history = JobHistory(
                    id=row['id'],
                    job_name=row['job_name'],
                    strategy=row['strategy'],
                    symbols=row['symbols'],
                    trigger_time=row['trigger_time'],
                    start_time=row['start_time'],
                    end_time=row['end_time'],
                    status=row['status'],
                    signals_count=row['signals_count'],
                    signals=row['signals'],
                    error_message=row['error_message']
                )
                histories.append(history)
            
            return histories
            
        except Exception as e:
            logger.error("Failed to query history: %s", e)
            return []
        finally:
            conn.close()
    
    def get_statistics(self, job_name: Optional[str] = None) -> dict:
        """Get statistics."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            if job_name:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_runs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                        SUM(signals_count) as total_signals,
                        MAX(trigger_time) as last_run
                    FROM job_history
                    WHERE job_name = ?
                """, (job_name,))
            else:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_runs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                        SUM(signals_count) as total_signals,
                        MAX(trigger_time) as last_run
                    FROM job_history
                """)
            
            row = cursor.fetchone()
            
            return {
                "total_runs": row['total_runs'] or 0,
                "success_count": row['success_count'] or 0,
                "failed_count": row['failed_count'] or 0,
                "total_signals": row['total_signals'] or 0,
                "last_run": row['last_run']
            }
            
        except Exception as e:
            logger.error("Failed to get statistics: %s", e)
            return {}
        finally:
            conn.close()


_history_db: Optional[HistoryDB] = None


def get_history_db() -> HistoryDB:
    """Get global history DB instance."""
    global _history_db
    if _history_db is None:
        _history_db = HistoryDB()
    return _history_db
