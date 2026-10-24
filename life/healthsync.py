# -*- coding: utf-8 -*-
"""Merge Huawei Health sleep/steps into the Joplin health note."""

from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
import sys
from configparser import ConfigParser
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

NOTE_TITLE = "健康运动笔记-（白晔峰）"
NOTE_ID_FALLBACK = "87b31ffd2e764bd9b376100022e6fb25"
HEALTH_DB = Path("/data/codebase/joplin-health-bridge/data/huawei_health.db")
NOTIF_DB = Path("/home/baiyefeng/work/joplin-message-center/data/sms_received.db")
STATE_PATH = ROOT / "data" / "healthsync.ini"
TMP_DIR = Path.home() / "tmp"

HEADING_RE = re.compile(r"^#{3,4}\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*$", re.M)
DATA_RE = re.compile(r"^(\d+)[，,]\s*(\d{1,2})[:：](\d{1,2})(\s*[，,].*)?$")
STEP_TITLE_RE = re.compile(r"^\s*([\d,]+)\s*steps\b", re.I)
NOTIF_START = date(2026, 10, 21)


# --- note blocks -------------------------------------------------------------


def split_blocks(body: str) -> tuple[str, list[dict], str]:
    matches = list(HEADING_RE.finditer(body))
    if not matches:
        return body, [], ""
    prefix = body[: matches[0].start()]
    blocks = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        raw = body[match.start():end]
        lines = raw.splitlines(keepends=True)
        heading = lines[0]
        data = None
        tail = None
        old_steps = old_sleep = None
        memo = ""
        if len(lines) > 1:
            data = lines[1]
            parsed = DATA_RE.match(data.rstrip("\r\n"))
            if parsed:
                old_steps = int(parsed.group(1))
                old_sleep = int(parsed.group(2)) * 60 + int(parsed.group(3))
                tail = parsed.group(4) or ""
                memo = "".join(lines[2:])
            else:
                memo = "".join(lines[1:])
                data = None
        blocks.append(
            {
                "date": date(int(match.group(1)), int(match.group(2)), int(match.group(3))),
                "heading": heading,
                "data": data,
                "tail": tail,
                "memo": memo,
                "raw": raw,
                "steps": old_steps,
                "sleep": old_sleep,
            }
        )
    return prefix, blocks, ""


def render_data(steps: int, sleep_min: int, tail: str) -> str:
    return f"{steps}，{sleep_min // 60}:{sleep_min % 60:02d}{tail or ''}\n"


def render_new_block(day: date, steps: int, sleep_min: int) -> str:
    return (
        f"### {day.year}年{day.month}月{day.day}日\n"
        f"{steps}，{sleep_min // 60}:{sleep_min % 60:02d}\n"
    )


def rebuild(prefix: str, blocks: list[dict], additions: list[tuple[date, str, str]], suffix: str) -> str:
    if additions:
        for day, block, _ in sorted(additions, key=lambda item: item[0], reverse=True):
            position = len(blocks)
            for index, existing in enumerate(blocks):
                if existing["date"] < day:
                    position = index
                    break
            blocks.insert(position, {"date": day, "raw": block})
    return prefix + "".join(block["raw"] for block in blocks) + suffix


# --- data --------------------------------------------------------------------


def open_health_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(HEALTH_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def load_health_rows(conn: sqlite3.Connection, start: date | None, end: date | None):
    sql = """
        SELECT s.date, s.total_min, st.steps
        FROM daily_sleep s
        LEFT JOIN daily_steps st ON st.date = s.date
    """
    clauses, params = [], []
    if start:
        clauses.append("s.date >= ?")
        params.append(start.isoformat())
    if end:
        clauses.append("s.date <= ?")
        params.append(end.isoformat())
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY s.date"
    return {row["date"]: row for row in conn.execute(sql, params)}


def load_step_rows(conn: sqlite3.Connection, start: date | None, end: date | None):
    sql = "SELECT date, steps FROM daily_steps"
    clauses, params = [], []
    if start:
        clauses.append("date >= ?")
        params.append(start.isoformat())
    if end:
        clauses.append("date <= ?")
        params.append(end.isoformat())
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    return {row["date"]: row["steps"] for row in conn.execute(sql, params)}


def sync_notification_steps(conn: sqlite3.Connection, start: date, end: date) -> int:
    if not NOTIF_DB.exists():
        return 0
    source = sqlite3.connect(f"file:{NOTIF_DB}?mode=ro", uri=True)
    snapshot = sqlite3.connect(":memory:")
    try:
        source.backup(snapshot)
        rows = snapshot.execute(
            "SELECT title, when_local FROM notifications "
            "WHERE title LIKE '%steps%' AND when_local >= ? AND when_local < ?",
            (start.isoformat(), (end + timedelta(days=1)).isoformat()),
        ).fetchall()
    finally:
        snapshot.close()
        source.close()

    best: dict[str, int] = {}
    for title, when_local in rows:
        match = STEP_TITLE_RE.match(title or "")
        if not match or not when_local:
            continue
        day = when_local[:10]
        try:
            value = int(match.group(1).replace(",", ""))
        except ValueError:
            continue
        if value > best.get(day, -1):
            best[day] = value

    stamp = datetime.now().isoformat(timespec="seconds")
    written = 0
    for day, steps in best.items():
        if day < NOTIF_START.isoformat() or steps <= 0:
            continue
        conn.execute(
            """
            INSERT INTO daily_steps(date, steps, source, fetched_at)
            VALUES (?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
              steps=excluded.steps, source=excluded.source,
              fetched_at=excluded.fetched_at
            WHERE daily_steps.source <> 'api'
            """,
            (day, steps, "notification", stamp),
        )
        written += 1
    conn.commit()
    return written


# --- Joplin ------------------------------------------------------------------


def joplin_module():
    import pathmagic

    with pathmagic.context():
        from func.jpfuncs import getnote, searchnotes, updatenote_body
    return getnote, searchnotes, updatenote_body


def locate_note():
    getnote, searchnotes, _ = joplin_module()
    for query in (NOTE_TITLE, "健康运动笔记"):
        try:
            results = searchnotes(query)
        except Exception:
            results = []
        for note in results:
            if note.title == NOTE_TITLE:
                return note.id, note.title, note.body
    note = getnote(NOTE_ID_FALLBACK)
    return note.id, note.title, note.body


def read_state() -> ConfigParser:
    config = ConfigParser()
    if STATE_PATH.exists():
        config.read(STATE_PATH, encoding="utf-8")
    return config


def write_state(note_id: str, digest: str) -> None:
    config = read_state()
    if not config.has_section("health"):
        config.add_section("health")
    config.set("health", "note_id", note_id)
    config.set("health", "md5", digest)
    config.set("health", "updated", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as fh:
        config.write(fh)


# --- merge -------------------------------------------------------------------


def apply_updates(body: str, rows, start: date, end: date, mode: str,
                  refresh_existing: bool, fill_missing: bool,
                  report_lines: list[str]) -> tuple[str, dict]:
    prefix, blocks, suffix = split_blocks(body)
    by_date = {block["date"]: block for block in blocks}
    additions: list[tuple[date, str, str]] = []
    stats = {"added": 0, "updated": 0, "unchanged": 0, "skipped": 0, "warn": 0}
    if not blocks and body.strip():
        report_lines.append("ABORT no parsable heading blocks")
        return body, stats

    for iso_day, row in sorted(rows.items()):
        day = date.fromisoformat(iso_day)
        if day < start or day > end:
            continue
        sleep_min = row["total_min"]
        db_steps = row["steps"]
        if sleep_min is None or sleep_min <= 0 or sleep_min > 900:
            stats["skipped"] += 1
            report_lines.append(f"SKIP {iso_day} sleep={sleep_min}")
            continue
        valid_steps = db_steps if db_steps and 0 < db_steps <= 100000 else None
        block = by_date.get(day)
        if block is not None and block.get("data") is None:
            stats["skipped"] += 1
            report_lines.append(f"SKIP {iso_day} existing block has unparsable data line")
            continue
        if block is None:
            if not fill_missing:
                continue
            if valid_steps is None:
                stats["skipped"] += 1
                report_lines.append(f"SKIP {iso_day} missing steps (cannot insert)")
                continue
            additions.append((day, render_new_block(day, valid_steps, sleep_min), "new"))
            stats["added"] += 1
            continue
        if not refresh_existing:
            stats["unchanged"] += 1
            continue
        new_steps = valid_steps if valid_steps is not None else block["steps"]
        new_sleep = sleep_min
        if new_steps is None or new_sleep is None:
            stats["skipped"] += 1
            continue
        if new_steps == block["steps"] and new_sleep == block["sleep"]:
            stats["unchanged"] += 1
            continue
        if block["steps"] != new_steps or block["sleep"] != new_sleep:
            delta_sleep = abs((block["sleep"] or 0) - new_sleep)
            delta_steps = abs((block["steps"] or 0) - new_steps)
            report_lines.append(
                f"UPDATE {iso_day} steps {block['steps']}->{new_steps} "
                f"sleep {block['sleep']}->{new_sleep} "
                f"(d_sleep={delta_sleep} d_steps={delta_steps})"
            )
        block["raw"] = block["heading"] + render_data(new_steps, new_sleep, block["tail"]) + block["memo"]
        block["steps"], block["sleep"] = new_steps, new_sleep
        stats["updated"] += 1

    merged = rebuild(prefix, blocks, additions, suffix)
    return merged, stats


def write_report(lines: list[str], mode: str) -> Path:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    path = TMP_DIR / f"health_backfill_diff_{datetime.now():%Y%m%d_%H%M%S}.txt"
    path.write_text(f"mode={mode}\n" + "\n".join(lines) + "\n", encoding="utf-8")
    return path


def snapshot_note(note_id: str, title: str, body: str) -> Path:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    path = TMP_DIR / f"health_note_pre_backfill_{datetime.now():%Y%m%d}.json"
    import json

    path.write_text(
        json.dumps(
            {"id": note_id, "title": title, "body": body,
             "updated_at": datetime.now().isoformat(timespec="seconds")},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


# --- commands ----------------------------------------------------------------


def command_publish(args) -> int:
    today = date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=max(1, args.days) - 1)
    conn = open_health_db()
    try:
        synced = sync_notification_steps(conn, start, end)
        rows = load_health_rows(conn, start, end)
    finally:
        conn.close()
    note_id, title, body = locate_note()
    report: list[str] = [f"publish {start}..{end}, notification_steps={synced}"]
    merged, stats = apply_updates(
        body, rows, start, end, "publish",
        refresh_existing=True, fill_missing=True, report_lines=report,
    )
    print(f"{start}..{end} rows={len(rows)} {stats}")
    if args.dry_run or not args.apply:
        path = write_report(report + [f"stats={stats}"], "publish")
        out = TMP_DIR / f"health_note_proposed_{datetime.now():%Y%m%d_%H%M%S}.md"
        out.write_text(merged, encoding="utf-8")
        print(f"dry-run: {path} {out}")
        return 0
    if merged == body:
        print("no changes")
        return 0
    _, _, update = joplin_module()
    update(note_id, merged)
    write_state(note_id, hashlib.md5(merged.encode("utf-8")).hexdigest())
    print(f"published {title} ({note_id})")
    return 0


def command_backfill(args) -> int:
    start = date.fromisoformat(args.from_date)
    end = date.fromisoformat(args.to_date) if args.to_date else date.today() - timedelta(days=1)
    refresh = args.refresh_existing
    fill = args.fill_missing or not refresh
    conn = open_health_db()
    try:
        synced = sync_notification_steps(conn, max(start, NOTIF_START), end)
        rows = load_health_rows(conn, start, end)
    finally:
        conn.close()
    note_id, title, body = locate_note()
    report: list[str] = [
        f"backfill {start}..{end} rows={len(rows)} notif_steps={synced} "
        f"refresh={refresh} fill={fill}"
    ]
    merged, stats = apply_updates(
        body, rows, start, end, "backfill",
        refresh_existing=refresh, fill_missing=fill, report_lines=report,
    )
    report.append(f"stats={stats}")
    path = write_report(report, "backfill")
    out = TMP_DIR / f"health_note_backfill_{datetime.now():%Y%m%d_%H%M%S}.md"
    out.write_text(merged, encoding="utf-8")
    print(f"{stats}; diff={path}")
    if not args.apply:
        print(f"dry-run only; proposed body: {out}")
        return 0
    if merged == body:
        print("no changes")
        return 0
    snap = snapshot_note(note_id, title, body)
    _, _, update = joplin_module()
    update(note_id, merged)
    write_state(note_id, hashlib.md5(merged.encode("utf-8")).hexdigest())
    print(f"applied; snapshot={snap}; proposed={out}")
    return 0


def command_sync_notifications(args) -> int:
    end = date.today()
    start = end - timedelta(days=max(1, args.days) - 1)
    conn = open_health_db()
    try:
        count = sync_notification_steps(conn, max(start, NOTIF_START), end)
    finally:
        conn.close()
    print(f"notification steps {start}..{end}: {count}")
    return 0


def command_validate(args) -> int:
    conn = open_health_db()
    try:
        rows = {
            row["date"]: (row["total_min"], row["steps"])
            for row in conn.execute(
                "SELECT s.date, s.total_min, st.steps FROM daily_sleep s "
                "LEFT JOIN daily_steps st ON st.date=s.date "
                "WHERE s.date IN ('2026-10-21','2026-10-22','2026-10-23')"
            )
        }
    finally:
        conn.close()
    expected = {"2026-10-21": 324, "2026-10-22": 411, "2026-10-23": 303}
    ok = True
    for day, sleep_expected in expected.items():
        if day not in rows:
            print(f"{day}: missing")
            ok = False
            continue
        sleep, steps = rows[day]
        mark = "OK" if sleep == sleep_expected else "MISMATCH"
        print(f"{day}: sleep={sleep} expected={sleep_expected} steps={steps} {mark}")
        ok = ok and sleep == sleep_expected
    return 0 if ok else 5


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    publish = sub.add_parser("publish")
    publish.add_argument("--days", type=int, default=5)
    publish.add_argument("--apply", action="store_true")
    publish.add_argument("--dry-run", action="store_true")

    backfill = sub.add_parser("backfill")
    backfill.add_argument("--from", dest="from_date", default="2023-07-01")
    backfill.add_argument("--to", dest="to_date")
    backfill.add_argument("--fill-missing", action="store_true")
    backfill.add_argument("--refresh-existing", action="store_true")
    backfill.add_argument("--apply", action="store_true")

    notifications = sub.add_parser("sync-notifications")
    notifications.add_argument("--days", type=int, default=5)

    sub.add_parser("validate")

    args = parser.parse_args()
    if args.command == "publish":
        return command_publish(args)
    if args.command == "backfill":
        return command_backfill(args)
    if args.command == "sync-notifications":
        return command_sync_notifications(args)
    return command_validate(args)


if __name__ == "__main__":
    raise SystemExit(main())
