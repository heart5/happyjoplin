# 健康随记双源只读解析：旧笔记 + 《健康随记登记》，绝不回写来源笔记

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from . import db

OLD_NOTE_ID = "87b31ffd2e764bd9b376100022e6fb25"
MANUAL_TITLE = "健康随记登记"
MANUAL_TEMPLATE = """# 健康随记登记

> 本笔记由你填写，健康系统只读解析，不会修改正文。

## 格式

- 每天用三级标题写日期：`### 2026年10月24日` 或 `### 2026-10-24`。
- 标题下第一行只写啤酒瓶数（纯数字）：喝酒写 `2`，不喝写 `0`。
- 第二行起写随记正文，可多行；正文里的数字不会计为瓶数。
- 如果第一行不是纯数字，按啤酒 0 记录，这一行仍算随记正文。
- 同一天可以写多段，每段单独记录瓶数。
"""

HEADING_RE = re.compile(r"^#{3,4}\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*$", re.M)
DATA_RE = re.compile(
    r"^(\d+)[，,]\s*(\d{1,2})[:：](\d{1,2})"
    r"(?:\s*[，,]\s*(?:啤酒[:：]?\s*)?(\d+))?\s*$"
)
MANUAL_HEADING_RE = re.compile(
    r"^#{3,4}\s*(?:(\d{4})年(\d{1,2})月(\d{1,2})日|(\d{4})-(\d{1,2})-(\d{1,2}))\s*$",
    re.M,
)
MANUAL_COUNT_RE = re.compile(r"^\d+$")

# 解析逻辑变化时递增，缓存命中会因版本不同而自动重解析
LEGACY_PARSER_VERSION = 1
MANUAL_PARSER_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS memos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  source_note_id TEXT NOT NULL,
  content TEXT NOT NULL DEFAULT '',
  beer_count INTEGER NOT NULL DEFAULT 0,
  content_hash TEXT NOT NULL UNIQUE,
  extracted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memos_date ON memos(date);
CREATE TABLE IF NOT EXISTS memo_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


# --- 来源读取 ----------------------------------------------------------------


def read_note_body(note_id: str) -> str:
    import pathmagic

    with pathmagic.context():
        from func.jpfuncs import getnote

        note = getnote(note_id)
    return note.body or ""


def read_note_meta(note_id: str) -> dict:
    import pathmagic

    with pathmagic.context():
        from func.jpfuncs import jpapi

        note = jpapi.get_note(
            note_id,
            fields="id,title,parent_id,updated_time,deleted_time",
        )
    return {
        "id": note.id,
        "title": note.title,
        "parent_id": note.parent_id,
        "updated_time": note.updated_time.isoformat() if note.updated_time else None,
        "deleted": note.deleted_time is not None,
    }


def note_error_kind(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    return "missing" if getattr(response, "status_code", None) == 404 else "unavailable"


def read_source_note() -> tuple[str, str]:
    return OLD_NOTE_ID, read_note_body(OLD_NOTE_ID)


# --- 解析 --------------------------------------------------------------------


def parse_memos(body: str, note_id: str = OLD_NOTE_ID) -> list[dict]:
    matches = list(HEADING_RE.finditer(body))
    stamp = datetime.now().isoformat(timespec="seconds")
    entries = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        raw = body[match.end():end].lstrip("\r\n")
        lines = raw.splitlines()
        beer = 0
        content = raw.strip()
        if lines:
            parsed = DATA_RE.match(lines[0].strip())
            if parsed:
                beer = int(parsed.group(4)) if parsed.group(4) else 0
                content = "\n".join(lines[1:]).strip()
        if not content and beer == 0:
            continue
        day = f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        digest = hashlib.sha256(f"{day}|{beer}|{content}".encode("utf-8")).hexdigest()[:32]
        entries.append({
            "date": day,
            "source_note_id": note_id,
            "content": content,
            "beer_count": beer,
            "content_hash": digest,
            "extracted_at": stamp,
        })
    return entries


def parse_manual(body: str, note_id: str) -> tuple[list[dict], list[str]]:
    """第一行纯数字 = 啤酒瓶数；否则按 0 瓶并把该行保留进随记。"""
    matches = list(MANUAL_HEADING_RE.finditer(body or ""))
    stamp = datetime.now().isoformat(timespec="seconds")
    today = date.today()
    entries: list[dict] = []
    warnings: list[str] = []
    occurrences: dict[str, int] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        if match.group(1):
            day = (f"{int(match.group(1)):04d}-"
                   f"{int(match.group(2)):02d}-{int(match.group(3)):02d}")
        else:
            day = (f"{int(match.group(4)):04d}-"
                   f"{int(match.group(5)):02d}-{int(match.group(6)):02d}")
        try:
            parsed = date.fromisoformat(day)
        except ValueError:
            warnings.append(f"跳过非法日期：{day}")
            continue
        if parsed > today:
            warnings.append(f"跳过未来日期：{day}")
            continue
        lines = body[match.end():end].splitlines()
        while lines and not lines[0].strip():
            lines.pop(0)
        beer = 0
        if lines and MANUAL_COUNT_RE.fullmatch(lines[0].strip()):
            beer = int(lines[0].strip())
            if beer > 99:
                warnings.append(f"{day} 啤酒瓶数 {beer} 偏大，请核对")
            lines = lines[1:]
        while lines and not lines[-1].strip():
            lines.pop()
        content = "\n".join(lines).strip()
        if not content and beer == 0:
            continue
        occurrence = occurrences.get(day, 0) + 1
        occurrences[day] = occurrence
        digest = hashlib.sha256(
            f"{note_id}|{day}|{beer}|{content}|{occurrence}".encode("utf-8")
        ).hexdigest()[:32]
        entries.append({
            "date": day,
            "source_note_id": note_id,
            "content": content,
            "beer_count": beer,
            "content_hash": digest,
            "extracted_at": stamp,
        })
    return entries, warnings


# --- 写库 --------------------------------------------------------------------


def _body_digest(body: str) -> str:
    return hashlib.sha256((body or "").encode("utf-8")).hexdigest()


def _connect_write() -> sqlite3.Connection:
    path = Path(db.MEMOS_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def _meta_conn(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        row = conn.execute("SELECT value FROM memo_meta WHERE key=?", (key,)).fetchone()
    except sqlite3.OperationalError:
        return None
    return row[0] if row else None


def _set_meta_conn(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO memo_meta(key, value) VALUES(?,?)", (key, str(value))
    )


def _source_rows(conn: sqlite3.Connection, note_id: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM memos WHERE source_note_id=?", (note_id,)
    ).fetchone()[0]


def _rebuild_source(conn: sqlite3.Connection, note_id: str, entries: list[dict],
                    body: str, hash_key: str, warnings: list[str],
                    force: bool = False) -> str:
    digest = _body_digest(body)
    existing = _source_rows(conn, note_id)
    if not force and _meta_conn(conn, hash_key) == digest and existing:
        return "skipped"
    if not entries and existing:
        warnings.append(
            f"来源 {note_id[:8]} 解析为 0 条，保留库中 {existing} 条（防清空误删）"
        )
        return "protected"
    conn.execute("DELETE FROM memos WHERE source_note_id=?", (note_id,))
    conn.executemany(
        "INSERT INTO memos(date, source_note_id, content, beer_count, "
        "content_hash, extracted_at) VALUES (?,?,?,?,?,?)",
        [(entry["date"], entry["source_note_id"], entry["content"],
          entry["beer_count"], entry["content_hash"], entry["extracted_at"])
         for entry in entries],
    )
    _set_meta_conn(conn, hash_key, digest)
    return "rebuilt"


def _legacy_cache_valid(conn: sqlite3.Connection) -> bool:
    if not _meta_conn(conn, "legacy_body_sha256"):
        return False
    if _meta_conn(conn, "legacy_parser_version") != str(LEGACY_PARSER_VERSION):
        return False
    stored_rows = _meta_conn(conn, "legacy_rows")
    try:
        expected = int(stored_rows)
    except (TypeError, ValueError):
        return False
    current = _source_rows(conn, OLD_NOTE_ID)
    if current != expected:
        return False
    return current > 0 or _meta_conn(conn, "legacy_parsed_empty") == "1"


def refresh_memos(manual_note_id: str | None = None, *,
                  verify_legacy: bool = False,
                  reimport_legacy: bool = False) -> dict:
    warnings: list[str] = []
    skipped: list[str] = []
    now = datetime.now().isoformat(timespec="seconds")
    status = "absent"
    manual_action = "absent"
    manual_entries: list[dict] | None = None
    manual_body: str | None = None
    manual_parse_warnings: list[str] = []
    manual_key: str | None = None
    legacy_action = "cached"
    legacy_verify_status: str | None = None
    legacy_verify_warning = ""
    legacy_body: str | None = None
    legacy_entries: list[dict] | None = None
    conn = _connect_write()
    try:
        need_legacy = not _legacy_cache_valid(conn)
        if need_legacy or verify_legacy or reimport_legacy:
            try:
                legacy_body = read_note_body(OLD_NOTE_ID)
            except Exception as exc:
                legacy_action = "unavailable"
                legacy_verify_status = "failed"
                legacy_verify_warning = (
                    f"旧笔记读取失败（{note_error_kind(exc)}）：{exc}")
                warnings.append(legacy_verify_warning)
            else:
                legacy_entries = parse_memos(legacy_body, OLD_NOTE_ID)
                if need_legacy or reimport_legacy:
                    legacy_action = "reimported" if reimport_legacy else "imported"
                    legacy_verify_status = "ok"
                else:
                    stored = _meta_conn(conn, "legacy_body_sha256") or ""
                    if stored == _body_digest(legacy_body):
                        legacy_action = "verified"
                        legacy_verify_status = "ok"
                    else:
                        legacy_action = "drift"
                        legacy_verify_status = "drift"
                        legacy_verify_warning = (
                            f"旧笔记内容与导入基线不同（{stored[:8] or '无基线'} → "
                            f"{_body_digest(legacy_body)[:8]}），"
                            "库保持冻结未重导入")
                        warnings.append(legacy_verify_warning)
                        legacy_entries = None

        if manual_note_id:
            if manual_note_id == OLD_NOTE_ID:
                raise RuntimeError("登记笔记 ID 与旧笔记相同，拒绝继续")
            manual_meta = None
            try:
                manual_meta = read_note_meta(manual_note_id)
            except Exception as exc:
                status = manual_action = note_error_kind(exc)
                warnings.append(f"登记笔记读取失败（{status}）：{exc}")
            else:
                if manual_meta.get("deleted"):
                    status = manual_action = "missing"
                    warnings.append("登记笔记在回收站中")
                    manual_meta = None
            if manual_meta is not None:
                manual_key = (f"{manual_note_id}|"
                              f"{manual_meta['updated_time'] or ''}")
                cached = (
                    _meta_conn(conn, "manual_updated_time") == manual_key
                    and _meta_conn(conn, "manual_body_sha256")
                    and _meta_conn(conn, "manual_parser_version")
                    == str(MANUAL_PARSER_VERSION)
                )
                if cached:
                    manual_action = "cached"
                    status = ("empty" if _meta_conn(conn, "manual_parsed_empty") == "1"
                              else "ok")
                else:
                    try:
                        manual_body = read_note_body(manual_note_id)
                    except Exception as exc:
                        status = manual_action = note_error_kind(exc)
                        warnings.append(
                            f"登记笔记正文读取失败（{status}）：{exc}")
                    else:
                        manual_entries, manual_parse_warnings = parse_manual(
                            manual_body, manual_note_id)
                        warnings.extend(manual_parse_warnings)
                        status = "ok" if manual_entries else "empty"
                        manual_action = "parsed"
        else:
            status = manual_action = (
                "missing" if _meta_conn(conn, "manual_note_id") else "absent")

        with conn:
            write_legacy = (legacy_entries is not None
                            and (need_legacy or reimport_legacy))
            if write_legacy:
                result = _rebuild_source(
                    conn, OLD_NOTE_ID, legacy_entries, legacy_body,
                    "legacy_body_sha256", warnings,
                    force=(need_legacy or reimport_legacy))
                legacy_action = result
                if result == "protected":
                    _set_meta_conn(conn, "legacy_body_sha256",
                                   _body_digest(legacy_body))
                _set_meta_conn(conn, "legacy_rows",
                               str(_source_rows(conn, OLD_NOTE_ID)))
                _set_meta_conn(conn, "legacy_parsed_empty",
                               "1" if not legacy_entries else "0")
                _set_meta_conn(conn, "legacy_parser_version",
                               str(LEGACY_PARSER_VERSION))
                _set_meta_conn(conn, "legacy_verified_at", now)
                _set_meta_conn(conn, "legacy_verify_status", "ok")
                _set_meta_conn(conn, "legacy_verify_warning", "")
            elif legacy_verify_status is not None:
                _set_meta_conn(conn, "legacy_verify_status",
                               legacy_verify_status)
                _set_meta_conn(conn, "legacy_verify_warning",
                               legacy_verify_warning)
                if legacy_verify_status in ("ok", "drift"):
                    _set_meta_conn(conn, "legacy_verified_at", now)

            if manual_action == "cached":
                conn.execute(
                    "DELETE FROM memos WHERE source_note_id NOT IN (?,?)",
                    (OLD_NOTE_ID, manual_note_id),
                )
            elif manual_entries is not None and manual_body is not None:
                result = _rebuild_source(
                    conn, manual_note_id, manual_entries, manual_body,
                    "manual_body_sha256", warnings)
                manual_action = result
                if result == "protected":
                    _set_meta_conn(conn, "manual_body_sha256",
                                   _body_digest(manual_body))
                conn.execute(
                    "DELETE FROM memos WHERE source_note_id NOT IN (?,?)",
                    (OLD_NOTE_ID, manual_note_id),
                )
                _set_meta_conn(conn, "manual_updated_time", manual_key)
                _set_meta_conn(conn, "manual_parser_version",
                               str(MANUAL_PARSER_VERSION))
                _set_meta_conn(conn, "manual_parsed_empty",
                               "1" if not manual_entries else "0")
                _set_meta_conn(conn, "manual_note_id", manual_note_id)
                _set_meta_conn(conn, "manual_warnings",
                               json.dumps(manual_parse_warnings[:10],
                                          ensure_ascii=False))
            _set_meta_conn(conn, "manual_status", status)
    finally:
        conn.close()
    os.chmod(Path(db.MEMOS_PATH), 0o600)
    if legacy_action in ("cached", "verified", "skipped"):
        skipped.append("legacy")
    if manual_action in ("cached", "skipped"):
        skipped.append("manual")
    stats = count_memos()
    stats["warnings"] = warnings
    stats["skipped"] = skipped
    stats["manual_status"] = status
    stats["sources"] = {"legacy": legacy_action, "manual": manual_action}
    return stats


# --- 读取与统计 --------------------------------------------------------------


def connect_readonly() -> sqlite3.Connection | None:
    path = Path(db.MEMOS_PATH)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def get_meta(key: str, default: str | None = None) -> str | None:
    conn = connect_readonly()
    if conn is None:
        return default
    try:
        value = _meta_conn(conn, key)
    finally:
        conn.close()
    return default if value is None else value


def get_meta_list(key: str) -> list[str]:
    raw = get_meta(key, "")
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return [str(item) for item in value] if isinstance(value, list) else []


def set_meta(key: str, value: str) -> None:
    conn = _connect_write()
    try:
        with conn:
            _set_meta_conn(conn, key, value)
    finally:
        conn.close()
    os.chmod(Path(db.MEMOS_PATH), 0o600)


def _beer_by_day_conn(conn: sqlite3.Connection, start: str | None,
                      end: str | None) -> dict[str, int]:
    clauses = ["beer_count > 0"]
    params: list[str] = []
    if start:
        clauses.append("date >= ?")
        params.append(start)
    if end:
        clauses.append("date <= ?")
        params.append(end)
    rows = conn.execute(
        "SELECT date, source_note_id, SUM(beer_count) AS bottles FROM memos "
        f"WHERE {' AND '.join(clauses)} GROUP BY date, source_note_id",
        params,
    ).fetchall()
    per_day: dict[str, dict[str, int]] = {}
    for row in rows:
        bucket = per_day.setdefault(row["date"], {"legacy": 0, "manual": 0})
        key = "legacy" if row["source_note_id"] == OLD_NOTE_ID else "manual"
        bucket[key] += row["bottles"] or 0
    return {day: (bucket["manual"] or bucket["legacy"])
            for day, bucket in per_day.items()}


def beer_by_day(start: str | None = None, end: str | None = None) -> dict[str, int]:
    """按日去重：同日两来源都有记录时优先登记笔记。"""
    conn = connect_readonly()
    if conn is None:
        return {}
    try:
        return _beer_by_day_conn(conn, start, end)
    finally:
        conn.close()


def _source_breakdown(conn: sqlite3.Connection) -> dict[str, dict]:
    result = {
        "legacy": {"total": 0, "memos": 0, "beer_days": 0, "beer_total": 0},
        "manual": {"total": 0, "memos": 0, "beer_days": 0, "beer_total": 0},
    }
    rows = conn.execute(
        "SELECT source_note_id, COUNT(*) AS total, "
        "SUM(CASE WHEN content <> '' THEN 1 ELSE 0 END) AS memos, "
        "COUNT(DISTINCT CASE WHEN beer_count > 0 THEN date END) AS beer_days, "
        "SUM(beer_count) AS beer_total FROM memos GROUP BY source_note_id"
    ).fetchall()
    for row in rows:
        key = "legacy" if row["source_note_id"] == OLD_NOTE_ID else "manual"
        for field in ("total", "memos", "beer_days", "beer_total"):
            result[key][field] += row[field] or 0
    return result


def count_memos() -> dict:
    empty = {"total": 0, "memos": 0, "beer_days": 0, "beer_total": 0,
             "legacy": {"total": 0, "memos": 0, "beer_days": 0, "beer_total": 0},
             "manual": {"total": 0, "memos": 0, "beer_days": 0, "beer_total": 0}}
    conn = connect_readonly()
    if conn is None:
        return empty
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN content <> '' THEN 1 ELSE 0 END) AS memos FROM memos"
        ).fetchone()
        beer = _beer_by_day_conn(conn, None, None)
        result = {
            "total": row["total"] or 0,
            "memos": row["memos"] or 0,
            "beer_days": len(beer),
            "beer_total": sum(beer.values()),
        }
        result.update(_source_breakdown(conn))
        return result
    finally:
        conn.close()


def memos_for_day(day: str) -> list[sqlite3.Row]:
    conn = connect_readonly()
    if conn is None:
        return []
    try:
        return conn.execute(
            "SELECT date, source_note_id, content, beer_count FROM memos "
            "WHERE date=? ORDER BY id", (day,)
        ).fetchall()
    finally:
        conn.close()


def memos_by_range(start: str, end: str) -> list[sqlite3.Row]:
    conn = connect_readonly()
    if conn is None:
        return []
    try:
        return conn.execute(
            "SELECT date, source_note_id, content, beer_count FROM memos "
            "WHERE date BETWEEN ? AND ? AND (content <> '' OR beer_count > 0) "
            "ORDER BY date, id", (start, end)
        ).fetchall()
    finally:
        conn.close()


def source_ranges() -> dict[str, dict]:
    result = {
        "legacy": {"start": None, "end": None, "rows": 0},
        "manual": {"start": None, "end": None, "rows": 0},
    }
    conn = connect_readonly()
    if conn is None:
        return result
    try:
        rows = conn.execute(
            "SELECT source_note_id, MIN(date) AS first, MAX(date) AS last, "
            "COUNT(*) AS rows FROM memos GROUP BY source_note_id"
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        key = "legacy" if row["source_note_id"] == OLD_NOTE_ID else "manual"
        entry = result[key]
        entry["rows"] += row["rows"] or 0
        if row["first"] and (entry["start"] is None or row["first"] < entry["start"]):
            entry["start"] = row["first"]
        if row["last"] and (entry["end"] is None or row["last"] > entry["end"]):
            entry["end"] = row["last"]
    return result


def coverage_range() -> tuple[str | None, str | None]:
    manual = source_ranges()["manual"]
    return manual["start"], manual["end"]


def last_memo_date() -> str | None:
    conn = connect_readonly()
    if conn is None:
        return None
    try:
        return conn.execute("SELECT MAX(date) FROM memos").fetchone()[0]
    finally:
        conn.close()


def overlap_beer_days() -> list[str]:
    conn = connect_readonly()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            "SELECT date, source_note_id, SUM(beer_count) AS bottles FROM memos "
            "WHERE beer_count > 0 GROUP BY date, source_note_id"
        ).fetchall()
    finally:
        conn.close()
    per_day: dict[str, dict[str, int]] = {}
    for row in rows:
        bucket = per_day.setdefault(row["date"], {})
        key = "legacy" if row["source_note_id"] == OLD_NOTE_ID else "manual"
        bucket[key] = bucket.get(key, 0) + (row["bottles"] or 0)
    return sorted(day for day, bucket in per_day.items()
                  if bucket.get("legacy") and bucket.get("manual"))


def range_stats(start: str, end: str) -> dict:
    conn = connect_readonly()
    if conn is None:
        return {"entries": 0, "memos": 0, "beer_days": 0, "beer_total": 0}
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS entries, "
            "SUM(CASE WHEN content <> '' THEN 1 ELSE 0 END) AS memos "
            "FROM memos WHERE date BETWEEN ? AND ?", (start, end)
        ).fetchone()
        beer = _beer_by_day_conn(conn, start, end)
    finally:
        conn.close()
    return {
        "entries": row["entries"] or 0,
        "memos": row["memos"] or 0,
        "beer_days": len(beer),
        "beer_total": sum(beer.values()),
    }


def _date_range(start: str, end: str) -> list[str]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    days = []
    while first <= last:
        days.append(first.isoformat())
        first += timedelta(days=1)
    return days


def _mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 1) if values else None


def beer_comparison(conn, start: str, end: str, *, min_drink: int = 3,
                    min_sober: int = 7) -> dict:
    """啤酒日 D 与次日睡眠/晨间指标对齐；非饮酒次夜只在登记覆盖区间内认定。"""
    cov_start, cov_end = coverage_range()
    total_days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    result = {
        "covered_days": 0,
        "range_days": total_days,
        "coverage_pct": 0.0,
        "min_drink": min_drink,
        "min_sober": min_sober,
        "drink": {"n": 0, "sleep": None, "score": None, "rhr": None, "hrv": None},
        "sober": {"n": 0, "sleep": None, "score": None, "rhr": None, "hrv": None},
        "ok": False,
        "reason": None,
    }
    if not cov_start:
        result["reason"] = "尚无《健康随记登记》覆盖，暂不做饮酒与次夜恢复对比。"
        return result

    prev_start = (date.fromisoformat(start) - timedelta(days=1)).isoformat()
    beer = beer_by_day(prev_start, end)
    rows = {row["date"]: row for row in db.daily_rows(conn, start, end)}
    nights = db.main_nights(conn, start, end)
    metrics = db.metric_rows(conn, start, end)
    groups = {
        key: {"sleep": [], "score": [], "rhr": [], "hrv": []}
        for key in ("drink", "sober")
    }
    nights_count = {"drink": 0, "sober": 0}
    for day_text in _date_range(start, end):
        prev_text = (date.fromisoformat(day_text) - timedelta(days=1)).isoformat()
        if not (cov_start <= prev_text <= cov_end):
            continue
        result["covered_days"] += 1
        row = rows.get(day_text)
        if row is None:
            continue
        key = "drink" if beer.get(prev_text, 0) > 0 else "sober"
        nights_count[key] += 1
        bucket = groups[key]
        if row["total_min"] is not None:
            bucket["sleep"].append(row["total_min"])
        night = nights.get(day_text)
        if night is not None and night["sleep_score"] is not None:
            bucket["score"].append(night["sleep_score"])
        day_metrics = metrics.get(day_text, {})
        for field, metric in (("rhr", "resting_hr"), ("hrv", "hrv")):
            entry = day_metrics.get(metric)
            if entry is not None and entry["value"] is not None:
                bucket[field].append(entry["value"])

    result["coverage_pct"] = round(100 * result["covered_days"] / total_days, 1)
    for key in ("drink", "sober"):
        result[key] = {"n": nights_count[key]}
        for field in ("sleep", "score", "rhr", "hrv"):
            result[key][field] = _mean(groups[key][field])

    if result["coverage_pct"] < 60:
        result["reason"] = (
            f"覆盖不足：有效登记 {result['covered_days']}/{total_days} 天"
            f"（{result['coverage_pct']:.0f}%），暂不做对比。"
        )
        return result
    if (result["drink"]["n"] < min_drink
            or result["sober"]["n"] < min_sober):
        result["reason"] = (
            f"样本不足：饮酒次夜 {result['drink']['n']} 夜、"
            f"非饮酒次夜 {result['sober']['n']} 夜"
            f"（门槛 {min_drink}/{min_sober}），暂不做对比。"
        )
        return result
    result["ok"] = True
    return result


# --- 汇编 --------------------------------------------------------------------


def build_compilation() -> str:
    conn = connect_readonly()
    stats = count_memos()
    if conn is None:
        entries = []
    else:
        try:
            entries = conn.execute(
                "SELECT date, content, beer_count FROM memos "
                "WHERE content <> '' OR beer_count > 0 ORDER BY date DESC, id DESC"
            ).fetchall()
        finally:
            conn.close()

    legacy = stats["legacy"]
    manual = stats["manual"]
    lines = [
        "# 健康生活随记汇编",
        "",
        f"> 数据来源：旧《健康运动笔记-（白晔峰）》只读提取 + "
        f"《{MANUAL_TITLE}》（用户录入，系统只读） ｜ "
        f"随记 {stats['memos']} 条 ｜ 啤酒记录 {stats['beer_days']} 天，共 "
        f"{stats['beer_total']} 瓶 ｜ 更新时间：{datetime.now():%Y-%m-%d %H:%M}",
        "",
        f"> 来源拆分：旧笔记 随记 {legacy['memos']} 条 / 啤酒 {legacy['beer_days']} 天 "
        f"{legacy['beer_total']} 瓶；登记笔记 随记 {manual['memos']} 条 / "
        f"啤酒 {manual['beer_days']} 天 {manual['beer_total']} 瓶。",
        "",
        "> 本汇编只保留生活随记与饮酒记录，**不包含旧笔记中的睡眠、步数数字**。"
        "啤酒数用于描述性统计，不做因果或健康判断。",
        "",
    ]
    current_month = None
    for entry in entries:
        month = entry["date"][:7]
        if month != current_month:
            year, mon = month.split("-")
            lines += ["", f"## {year}年{int(mon)}月", ""]
            current_month = month
        day = entry["date"][5:]
        content = re.sub(r"\s+", " ", entry["content"]).strip()
        if entry["beer_count"] and content:
            lines.append(f"- {day}（啤酒 {entry['beer_count']} 瓶）：{content}")
        elif entry["beer_count"]:
            lines.append(f"- {day}：啤酒 {entry['beer_count']} 瓶")
        else:
            lines.append(f"- {day}：{content}")
    if not entries:
        lines += ["", "_尚无随记数据，请先运行 `memos --refresh`。_"]
    lines += [
        "",
        "---",
        "*旧笔记只读提取；《健康随记登记》由用户维护、系统只读。*",
        "",
    ]
    return "\n".join(lines)
