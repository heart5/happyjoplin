# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # 笔记监测 —— 存储层

# %% [markdown]
# ## 引入库

# %%
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain
    from func.logme import log

# %%
DB_PATH = Path(getdirmain()) / "data" / "monitor.db"
STABILITY_COOLDOWN_MINUTES = 30
MAX_PENDING_WAIT_HOURS = 6


# %% [markdown]
# ## 数据库连接


# %%
@contextmanager
def _get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# %% [markdown]
# ## 初始化数据库


# %%
def init_db() -> None:
    with _get_conn() as conn:
        conn.executescript("""
CREATE TABLE IF NOT EXISTS notes (
    note_id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    person TEXT NOT NULL DEFAULT '',
    section TEXT NOT NULL DEFAULT '',
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id TEXT NOT NULL REFERENCES notes(note_id),
    captured_at TIMESTAMP NOT NULL,
    content_hash TEXT NOT NULL,
    word_count INTEGER NOT NULL DEFAULT 0,
    body_fulltext TEXT NOT NULL DEFAULT '',
    is_forced INTEGER NOT NULL DEFAULT 0,
    UNIQUE(note_id, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_note_id ON snapshots(note_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_captured_at ON snapshots(captured_at);

CREATE TABLE IF NOT EXISTS daily_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id TEXT NOT NULL REFERENCES notes(note_id),
    entry_date DATE NOT NULL,
    word_count INTEGER NOT NULL DEFAULT 0,
    is_backfill INTEGER NOT NULL DEFAULT 0,
    snapshot_id INTEGER REFERENCES snapshots(id),
    UNIQUE(note_id, entry_date, snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_note_date ON daily_stats(note_id, entry_date);

CREATE TABLE IF NOT EXISTS pending_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id TEXT NOT NULL REFERENCES notes(note_id),
    content_hash TEXT NOT NULL,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    UNIQUE(note_id)
);

CREATE TABLE IF NOT EXISTS report_dirty (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person TEXT NOT NULL,
    dirty_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS spark_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    used_date DATE NOT NULL,
    quote_hash TEXT NOT NULL,
    UNIQUE(used_date, quote_hash)
);

CREATE INDEX IF NOT EXISTS idx_spark_log_date ON spark_log(used_date);
""")


# %% [markdown]
# ## notes 表操作


# %%
def upsert_note(
    note_id: str,
    title: str = "",
    person: str = "",
    section: str = "",
) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
INSERT INTO notes (note_id, title, person, section, first_seen, last_seen)
VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
ON CONFLICT(note_id) DO UPDATE SET
    title=excluded.title,
    person=CASE WHEN notes.person='' THEN excluded.person ELSE notes.person END,
    section=CASE WHEN notes.section='' THEN excluded.section ELSE notes.section END,
    last_seen=datetime('now')
""",
            (note_id, title, person, section),
        )


def get_note_info(note_id: str) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM notes WHERE note_id=?", (note_id,)).fetchone()
        return dict(row) if row else None


def get_notes_by_person(person: str) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("SELECT * FROM notes WHERE person=? AND is_active=1", (person,)).fetchall()
        return [dict(r) for r in rows]


def get_active_notes() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("SELECT * FROM notes WHERE is_active=1").fetchall()
        return [dict(r) for r in rows]


def deactivate_notes_except(note_ids: set) -> int:
    """将不在给定集合中的笔记标记为is_active=0。返回被标记为不活跃的数量。"""
    with _get_conn() as conn:
        # 先查出不在集合中的note_id
        all_ids = {r["note_id"] for r in conn.execute("SELECT note_id FROM notes WHERE is_active=1").fetchall()}
        to_deactivate = all_ids - note_ids
        if to_deactivate:
            placeholders = ",".join("?" * len(to_deactivate))
            conn.execute(f"UPDATE notes SET is_active=0 WHERE note_id IN ({placeholders})", tuple(to_deactivate))
        return len(to_deactivate)


def get_person_set() -> set[str]:
    with _get_conn() as conn:
        rows = conn.execute("SELECT DISTINCT person FROM notes WHERE person!='' AND is_active=1").fetchall()
        return {r["person"] for r in rows}


# %% [markdown]
# ## snapshots 表操作


# %%
def get_last_snapshot_hash(note_id: str) -> str | None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT content_hash FROM snapshots WHERE note_id=? ORDER BY captured_at DESC LIMIT 1",
            (note_id,),
        ).fetchone()
        return row["content_hash"] if row else None


def insert_snapshot(
    note_id: str,
    captured_at: datetime,
    content_hash: str,
    word_count: int,
    body_fulltext: str,
    is_forced: int = 0,
) -> int | None:
    with _get_conn() as conn:
        try:
            cursor = conn.execute(
                """INSERT INTO snapshots (note_id, captured_at, content_hash, word_count, body_fulltext, is_forced)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (note_id, captured_at, content_hash, word_count, body_fulltext, is_forced),
            )
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # hash already exists for this note — no-op
            return None


def get_snapshot_count(note_id: str) -> int:
    with _get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM snapshots WHERE note_id=?", (note_id,)).fetchone()
        return row["cnt"]


def get_latest_snapshot(note_id: str) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM snapshots WHERE note_id=? ORDER BY captured_at DESC LIMIT 1",
            (note_id,),
        ).fetchone()
        return dict(row) if row else None


# %% [markdown]
# ## daily_stats 表操作


# %%
def upsert_daily_stat(
    note_id: str,
    entry_date: str,
    word_count: int,
    is_backfill: int = 0,
    snapshot_id: int | None = None,
) -> None:
    with _get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO daily_stats (note_id, entry_date, word_count, is_backfill, snapshot_id)
               VALUES (?, ?, ?, ?, ?)""",
            (note_id, entry_date, word_count, is_backfill, snapshot_id),
        )


def get_daily_stats(note_id: str) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT entry_date, word_count, is_backfill FROM daily_stats WHERE note_id=? ORDER BY entry_date",
            (note_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_daily_stats_by_person(person: str, active_note_ids: list[str]) -> dict:
    """返回 {note_title: {entry_date: (word_count, is_backfill), ...}}"""
    result = {}
    with _get_conn() as conn:
        for note_id in active_note_ids:
            title_row = conn.execute(
                "SELECT title FROM notes WHERE note_id=? AND person=?", (note_id, person)
            ).fetchone()
            if not title_row:
                continue
            title = title_row["title"]
            rows = conn.execute(
                "SELECT entry_date, word_count, is_backfill FROM daily_stats WHERE note_id=? ORDER BY entry_date",
                (note_id,),
            ).fetchall()
            result[title] = {r["entry_date"]: (r["word_count"], bool(r["is_backfill"])) for r in rows}
    return result


# %% [markdown]
# ## pending_changes 表操作


# %%
def upsert_pending_change(
    note_id: str,
    content_hash: str,
    first_seen: datetime,
    last_seen: datetime,
) -> None:
    with _get_conn() as conn:
        conn.execute(
            """INSERT INTO pending_changes (note_id, content_hash, first_seen, last_seen)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(note_id) DO UPDATE SET
               content_hash=excluded.content_hash,
               first_seen=excluded.first_seen,
               last_seen=excluded.last_seen""",
            (note_id, content_hash, first_seen, last_seen),
        )


def get_pending_change(note_id: str) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM pending_changes WHERE note_id=?", (note_id,)).fetchone()
        return dict(row) if row else None


def delete_pending_change(note_id: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM pending_changes WHERE note_id=?", (note_id,))


def get_pending_changes_summary() -> list[dict]:
    """返回所有 pending 中的变更摘要"""
    with _get_conn() as conn:
        rows = conn.execute(
            """SELECT p.*, n.title FROM pending_changes p
               JOIN notes n ON p.note_id=n.note_id""",
        ).fetchall()
        return [dict(r) for r in rows]


# %% [markdown]
# ## report_dirty 表操作


# %%
def mark_report_dirty(person: str) -> None:
    with _get_conn() as conn:
        conn.execute("INSERT INTO report_dirty (person) VALUES (?)", (person,))


def get_dirty_persons() -> list[str]:
    with _get_conn() as conn:
        rows = conn.execute("SELECT DISTINCT person FROM report_dirty WHERE processed=0").fetchall()
        return [r["person"] for r in rows]


def clear_dirty() -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM report_dirty")


# %% [markdown]
# ## config 表操作（替代 happyjpmonitor.ini）


# %%
def get_config(key: str, default: Any = None) -> Any:
    with _get_conn() as conn:
        row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_config(key: str, value: str) -> None:
    with _get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, value))


def delete_config(key: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM config WHERE key=?", (key,))


# %% [markdown]
# ## spark_log 表操作（火花去重）


# %%
def add_spark_log(used_date: str, quote_hash: str) -> None:
    """记录一条已被使用的火花语录。"""
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO spark_log (used_date, quote_hash) VALUES (?, ?)",
            (used_date, quote_hash),
        )


def get_used_spark_hashes(days: int = 7) -> set:
    """获取最近N天内已使用的火花语录hash集合。"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT quote_hash FROM spark_log WHERE used_date >= ?", (cutoff,)
        ).fetchall()
        return {r["quote_hash"] for r in rows}


def cleanup_spark_log(days: int = 7) -> int:
    """清理超过N天的火花记录。返回删除数。"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _get_conn() as conn:
        cursor = conn.execute("DELETE FROM spark_log WHERE used_date < ?", (cutoff,))
        return cursor.rowcount


# %% [markdown]
# ## 迁移：从旧 JSON + INI 导入数据


# %%
def migrate_from_legacy(json_path: str | None = None) -> tuple[int, int]:
    """从旧 monitor_state_notes.json 和 happyjpmonitor.ini 迁移数据。

    使用单连接批量写入，避免数千次连接开销。

    Returns:
        (notes_count, snapshots_count)
    """
    import json

    from func.configpr import getcfp

    init_db()

    json_path = json_path or str(Path(getdirmain()) / "data" / "monitor_state_notes.json")
    notes_count = 0
    snapshots_count = 0

    if not Path(json_path).exists():
        log.warning(f"旧状态文件不存在: {json_path}")
        return 0, 0

    with open(json_path) as f:
        old_data = json.load(f)

    cfg, cfg_path = getcfp("happyjpmonitor")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    note_sql = """INSERT INTO notes (note_id, title, person, section, first_seen, last_seen)
                  VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                  ON CONFLICT(note_id) DO UPDATE SET
                  title=excluded.title,
                  person=CASE WHEN notes.person='' THEN excluded.person ELSE notes.person END,
                  section=CASE WHEN notes.section='' THEN excluded.section ELSE notes.section END,
                  last_seen=datetime('now')"""

    daily_sql = """INSERT OR REPLACE INTO daily_stats (note_id, entry_date, word_count, is_backfill, snapshot_id)
                   VALUES (?, ?, ?, ?, ?)"""

    snap_sql = """INSERT INTO snapshots (note_id, captured_at, content_hash, word_count, body_fulltext, is_forced)
                  VALUES (?, ?, ?, ?, ?, ?)"""

    note_batch = []
    daily_batch = []
    snap_batch = []

    for note_id, info in old_data.items():
        notes_count += 1
        person = info.get("person", "")
        section = info.get("section", "")
        title = info.get("title", "")

        note_batch.append((note_id, title, person, section))

        for date_str, updates in info.get("content_by_date", {}).items():
            if not updates:
                continue
            _, last_wc = updates[-1]
            entry_date = date_str if isinstance(date_str, str) else str(date_str)
            daily_batch.append((note_id, entry_date, last_wc, int(len(updates) > 1), None))

        first_hash = None
        if cfg.has_section("content_hash") and cfg.has_option("content_hash", note_id):
            first_hash = cfg.get("content_hash", note_id, fallback=None)
        if first_hash:
            snapshots_count += 1
            snap_batch.append((note_id, datetime.now(), first_hash, info.get("previous_word_count", 0), "", 0))

    try:
        conn.executemany(note_sql, note_batch)
        conn.executemany(daily_sql, daily_batch)
        conn.executemany(snap_sql, snap_batch)
        conn.commit()
        log.info(f"迁移完成: {notes_count} 篇笔记, {len(daily_batch)} 条日统计, {len(snap_batch)} 条快照记录")
    except Exception as e:
        conn.rollback()
        log.critical(f"迁移失败: {e}")
        raise
    finally:
        conn.close()

    return notes_count, len(snap_batch)


# %% [markdown]
# ## 主函数，__main__（测试用）

# %%
if __name__ == "__main__":
    import pathmagic

    with pathmagic.context():
        from func.sysfunc import not_IPython
    if not_IPython():
        log.info(f"开始运行文件\t{__file__}")

    init_db()
    print(f"数据库已初始化: {DB_PATH}")
    print(f"活跃笔记数量: {len(get_active_notes())}")
    print(f"人物集合: {get_person_set()}")

    if not_IPython():
        log.info(f"Done.结束执行文件\t{__file__}")
