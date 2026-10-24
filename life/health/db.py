# 健康数据只读访问：主库与旧笔记随记库

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.environ.get("HEALTH_DB", "/data/health/huawei_health.db"))
MEMOS_PATH = Path(os.environ.get("HEALTH_MEMOS_DB", "/data/health/health_memos.db"))


def connect(path: str | Path = DB_PATH) -> sqlite3.Connection:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"健康数据库不存在：{path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def daily_rows(conn: sqlite3.Connection, start: str, end: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT s.date, s.night_min, s.nap_min, s.total_min,
               s.night_records, s.nap_records,
               st.steps, st.source AS steps_source
        FROM daily_sleep s
        LEFT JOIN daily_steps st ON st.date = s.date
        WHERE s.date BETWEEN ? AND ?
        ORDER BY s.date
        """,
        (start, end),
    ).fetchall()


def sleep_records(conn: sqlite3.Connection, start: str, end: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM sleep_records WHERE attributed_date BETWEEN ? AND ? "
        "ORDER BY attributed_date, total_sleep_min DESC",
        (start, end),
    ).fetchall()


def main_nights(conn: sqlite3.Connection, start: str, end: str) -> dict[str, sqlite3.Row]:
    """每个归日取时长最长的一条非小睡记录，用于睡眠结构统计。"""
    rows = conn.execute(
        "SELECT * FROM sleep_records WHERE attributed_date BETWEEN ? AND ? "
        "AND sleep_type <> 3 ORDER BY attributed_date, total_sleep_min DESC",
        (start, end),
    ).fetchall()
    result: dict[str, sqlite3.Row] = {}
    for row in rows:
        result.setdefault(row["attributed_date"], row)
    return result


def metric_rows(conn: sqlite3.Connection, start: str, end: str) -> dict[str, dict[str, sqlite3.Row]]:
    rows = conn.execute(
        "SELECT * FROM daily_metrics WHERE date BETWEEN ? AND ? ORDER BY date, metric",
        (start, end),
    ).fetchall()
    result: dict[str, dict[str, sqlite3.Row]] = {}
    for row in rows:
        result.setdefault(row["date"], {})[row["metric"]] = row
    return result


def workouts(conn: sqlite3.Connection, start: str, end: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM workouts WHERE date BETWEEN ? AND ? ORDER BY date, start_ms",
        (start, end),
    ).fetchall()


def emotions(conn: sqlite3.Connection, start: str, end: str) -> dict[str, sqlite3.Row]:
    rows = conn.execute(
        "SELECT * FROM daily_emotion WHERE date BETWEEN ? AND ? ORDER BY date",
        (start, end),
    ).fetchall()
    return {row["date"]: row for row in rows}


def sync_runs(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM sync_runs ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()


def range_bounds(conn: sqlite3.Connection) -> tuple[str | None, str | None]:
    row = conn.execute("SELECT MIN(date), MAX(date) FROM daily_sleep").fetchone()
    return row[0], row[1]


def latest_complete_day(conn: sqlite3.Connection, today) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) FROM daily_sleep WHERE date < ?", (today.isoformat(),)
    ).fetchone()
    return row[0]


def metric_last_dates(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        row["metric"]: row["last"]
        for row in conn.execute(
            "SELECT metric, MAX(date) AS last FROM daily_metrics GROUP BY metric"
        )
    }


def table_last_dates(conn: sqlite3.Connection) -> dict[str, str | None]:
    return {
        "sleep": conn.execute("SELECT MAX(date) FROM daily_sleep").fetchone()[0],
        "steps": conn.execute("SELECT MAX(date) FROM daily_steps").fetchone()[0],
        "workouts": conn.execute("SELECT MAX(date) FROM workouts").fetchone()[0],
        "emotion": conn.execute("SELECT MAX(date) FROM daily_emotion").fetchone()[0],
    }
