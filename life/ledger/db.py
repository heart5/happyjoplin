# 个人财务系统 — 数据库连接与基类

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

import pathmagic
with pathmagic.context():
    from func.first import getdirmain

from .schema import init_db

_local = threading.local()


def _connections() -> dict:
    if not hasattr(_local, "conns"):
        _local.conns = {}
    return _local.conns


def get_connection(db_path: str) -> sqlite3.Connection:
    """获取线程级数据库连接（按路径缓存，自动初始化）。"""
    conns = _connections()
    key = str(Path(db_path).resolve())
    if key not in conns:
        conns[key] = _create_connection(key)
    return conns[key]


def close_connection(db_path: str = None):
    conns = _connections()
    if db_path is None:
        for c in conns.values():
            c.close()
        conns.clear()
        return
    key = str(Path(db_path).resolve())
    conn = conns.pop(key, None)
    if conn is not None:
        conn.close()


def _create_connection(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    init_db(conn)
    return conn


class Database:
    """轻量 SQLite 封装，支持事务。"""

    def __init__(self, db_path: str = None):
        if db_path is None:
            root = getdirmain()
            db_path = str(root / "data" / "finance.db")
        self.db_path = db_path
        self._in_tx = False

    @property
    def conn(self) -> sqlite3.Connection:
        return get_connection(self.db_path)

    @contextmanager
    def transaction(self):
        """批量写入事务：期间不逐条提交。"""
        conn = self.conn
        if self._in_tx:
            yield conn
            return
        self._in_tx = True
        try:
            conn.execute("BEGIN")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._in_tx = False

    def _maybe_commit(self):
        if not self._in_tx:
            self.conn.commit()

    def execute(self, sql: str, params=()) -> sqlite3.Cursor:
        cur = self.conn.execute(sql, params)
        self._maybe_commit()
        return cur

    def executemany(self, sql: str, seq) -> sqlite3.Cursor:
        cur = self.conn.executemany(sql, seq)
        self._maybe_commit()
        return cur

    def fetchone(self, sql: str, params=()) -> dict:
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def fetchall(self, sql: str, params=()) -> list:
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def insert(self, table: str, data: dict) -> int:
        cols = ", ".join(data.keys())
        vals = ", ".join(["?"] * len(data))
        cur = self.conn.execute(
            f"INSERT INTO {table} ({cols}) VALUES ({vals})",
            list(data.values()),
        )
        self._maybe_commit()
        return cur.lastrowid

    def insert_many(self, table: str, records: list) -> int:
        if not records:
            return 0
        cols = ", ".join(records[0].keys())
        vals = ", ".join(["?"] * len(records[0]))
        rows = [list(r.values()) for r in records]
        cur = self.conn.executemany(
            f"INSERT INTO {table} ({cols}) VALUES ({vals})", rows
        )
        self._maybe_commit()
        return cur.rowcount

    def update(self, table: str, data: dict, where: dict):
        set_clause = ", ".join(f"{k}=?" for k in data)
        where_clause = " AND ".join(f"{k}=?" for k in where)
        self.conn.execute(
            f"UPDATE {table} SET {set_clause} WHERE {where_clause}",
            list(data.values()) + list(where.values()),
        )
        self._maybe_commit()

    def upsert(self, table: str, data: dict, conflict_cols: list):
        """INSERT ... ON CONFLICT DO UPDATE。"""
        cols = ", ".join(data.keys())
        vals = ", ".join(["?"] * len(data))
        updates = ", ".join(f"{k}=excluded.{k}" for k in data if k not in conflict_cols)
        if updates:
            sql = (f"INSERT INTO {table} ({cols}) VALUES ({vals}) "
                   f"ON CONFLICT({','.join(conflict_cols)}) DO UPDATE SET {updates}")
        else:
            sql = (f"INSERT INTO {table} ({cols}) VALUES ({vals}) "
                   f"ON CONFLICT({','.join(conflict_cols)}) DO NOTHING")
        self.conn.execute(sql, list(data.values()))
        self._maybe_commit()

    def commit(self):
        self.conn.commit()

    def close(self):
        close_connection(self.db_path)
