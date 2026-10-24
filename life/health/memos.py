# 旧健康笔记只读解析：随记与啤酒数进入独立库，绝不回写旧笔记

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from . import db

OLD_NOTE_ID = "87b31ffd2e764bd9b376100022e6fb25"
HEADING_RE = re.compile(r"^#{3,4}\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*$", re.M)
DATA_RE = re.compile(
    r"^(\d+)[，,]\s*(\d{1,2})[:：](\d{1,2})"
    r"(?:\s*[，,]\s*(?:啤酒[:：]?\s*)?(\d+))?\s*$"
)

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
"""


def read_source_note() -> tuple[str, str]:
    import pathmagic

    with pathmagic.context():
        from func.jpfuncs import getnote

        note = getnote(OLD_NOTE_ID)
    return note.id, note.body


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


def refresh_memos() -> dict:
    note_id, body = read_source_note()
    entries = parse_memos(body, note_id)
    path = Path(db.MEMOS_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.executescript(SCHEMA)
        with conn:
            conn.execute("DELETE FROM memos WHERE source_note_id=?", (note_id,))
            conn.executemany(
                "INSERT INTO memos(date, source_note_id, content, beer_count, "
                "content_hash, extracted_at) VALUES (?,?,?,?,?,?)",
                [(e["date"], e["source_note_id"], e["content"], e["beer_count"],
                  e["content_hash"], e["extracted_at"]) for e in entries],
            )
    finally:
        conn.close()
    os.chmod(path, 0o600)
    return {
        "total": len(entries),
        "memos": sum(1 for entry in entries if entry["content"]),
        "beer_days": sum(1 for entry in entries if entry["beer_count"]),
        "beer_total": sum(entry["beer_count"] for entry in entries),
    }


def connect_readonly() -> sqlite3.Connection | None:
    path = Path(db.MEMOS_PATH)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def count_memos() -> dict:
    conn = connect_readonly()
    if conn is None:
        return {"total": 0, "memos": 0, "beer_days": 0, "beer_total": 0}
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN content <> '' THEN 1 ELSE 0 END) AS memos, "
            "SUM(CASE WHEN beer_count > 0 THEN 1 ELSE 0 END) AS beer_days, "
            "SUM(beer_count) AS beer_total FROM memos"
        ).fetchone()
        return {key: (row[key] or 0) for key in ("total", "memos", "beer_days", "beer_total")}
    finally:
        conn.close()


def memos_for_day(day: str) -> list[sqlite3.Row]:
    conn = connect_readonly()
    if conn is None:
        return []
    try:
        return conn.execute(
            "SELECT date, content, beer_count FROM memos WHERE date=? ORDER BY id", (day,)
        ).fetchall()
    finally:
        conn.close()


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

    lines = [
        "# 健康生活随记汇编",
        "",
        f"> 数据来源：旧《健康运动笔记-（白晔峰）》只读提取 ｜ "
        f"随记 {stats['memos']} 条 ｜ 啤酒记录 {stats['beer_days']} 天，共 "
        f"{stats['beer_total']} 瓶 ｜ 更新时间：{datetime.now():%Y-%m-%d %H:%M}",
        "",
        "> 本汇编只保留生活随记与饮酒记录，**不包含旧笔记中的睡眠、步数数字**，"
        "仅作生活参考，不参与健康数值计算或任何健康判断。",
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
        "*从旧笔记只读提取；旧笔记本身不做任何修改。*",
        "",
    ]
    return "\n".join(lines)
