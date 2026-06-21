# 个人财务系统 — 数据库连接与基类

import sqlite3
import threading
from pathlib import Path

import pathmagic
with pathmagic.context():
    from func.first import getdirmain

from .schema import init_db

_local = threading.local()


def get_connection(db_path: str) -> sqlite3.Connection:
    """获取线程级数据库连接（自动初始化）。"""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = _create_connection(db_path)
    return _local.conn


def close_connection():
    if hasattr(_local, "conn") and _local.conn is not None:
        _local.conn.close()
        _local.conn = None


def _create_connection(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    return conn


class Database:
    """轻量 SQLite 封装，支持上下文管理。"""

    def __init__(self, db_path: str = None):
        if db_path is None:
            root = getdirmain()
            db_path = str(root / "data" / "ledger.db")
        self.db_path = db_path

    @property
    def conn(self) -> sqlite3.Connection:
        return get_connection(self.db_path)

    def execute(self, sql: str, params=()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def executemany(self, sql: str, seq) -> sqlite3.Cursor:
        return self.conn.executemany(sql, seq)

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
        self.conn.commit()
        return cur.lastrowid

    def insert_many(self, table: str, records: list) -> list:
        if not records:
            return []
        cols = ", ".join(records[0].keys())
        vals = ", ".join(["?"] * len(records[0]))
        rows = [list(r.values()) for r in records]
        cur = self.conn.executemany(
            f"INSERT INTO {table} ({cols}) VALUES ({vals})", rows
        )
        self.conn.commit()
        return [cur.lastrowid]

    def update(self, table: str, data: dict, where: dict):
        set_clause = ", ".join(f"{k}=?" for k in data)
        where_clause = " AND ".join(f"{k}=?" for k in where)
        self.conn.execute(
            f"UPDATE {table} SET {set_clause} WHERE {where_clause}",
            list(data.values()) + list(where.values()),
        )
        self.conn.commit()

    def upsert(self, table: str, data: dict, conflict_cols: list):
        """INSERT ... ON CONFLICT DO UPDATE。"""
        cols = ", ".join(data.keys())
        vals = ", ".join(["?"] * len(data))
        updates = ", ".join(f"{k}=excluded.{k}" for k in data if k not in conflict_cols)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT({','.join(conflict_cols)}) DO UPDATE SET {updates}"
        self.conn.execute(sql, list(data.values()))
        self.conn.commit()

    def commit(self):
        self.conn.commit()

    def close(self):
        close_connection()
