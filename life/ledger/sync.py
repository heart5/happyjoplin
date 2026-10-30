# 个人财务系统 — 多源同步（HCX 本地执行）
#
# 数据源：
#   sms          HCX /sms/query?kind=finance&merge=1（全部设备，content_hash 去重）
#   wechat_main  主号微信合并库 wcitemsall_merged.db（本地 SQLite）
#   wechat_work  工作号微信 wx2_chat.db（本地 SQLite）
#   notification 三机系统通知（HCX /inbox/notification/query，仅个人账套）
#
# 每月按 book 原子重建：任一数据源失败则整月跳过，不写入半成品。

import argparse
import hashlib
import json
import os
import sqlite3
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pathmagic
with pathmagic.context():
    from func.logme import log

from .db import Database
from .accounts import AccountManager
from .transactions import TransactionManager, month_bounds
from .cloudcfg import (
    get_sms_api_url,
    get_main_wechat_db_path,
    get_wx2_db_path,
    get_main_wechat_table,
    get_work_wechat_table,
)
from life.wechat_finance import (
    parse_finance_messages,
    classify_merchant,
    load_category_map,
)
from life.sms_finance import parse_sms_records, _get_sms_api_key
from .notifications import fetch_notifications
from .reclassify import reclassify_unclassified

__all__ = ["FinanceSync", "sync_range", "main"]

TZ = timezone(timedelta(hours=8))

BOOK_SOURCES = {
    "personal": ("sms", "wechat_main", "wechat_work", "notification"),
    "work": ("wechat_work",),
}

# 工作号用银行卡付的款项归个人账套（与个人银行卡短信合并），零钱付款留工作账套
_CARD_PM_KW = ("信用卡", "储蓄卡", "借记卡", "银行")

EARLIEST_MONTH = {
    "personal": "2022-08",
    "work": "2024-01",
}

_PREFIX = {"wechat_main": "wxmain", "wechat_work": "wx2"}


def _md5(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _norm_time(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value), TZ).strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip().replace("T", " ")
    return text[:19]


def _epoch(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d")
               .replace(tzinfo=TZ).timestamp())


def _month_list(start: str, end: str) -> list:
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def _current_month() -> str:
    return datetime.now(TZ).strftime("%Y-%m")


def _prev_month(month_key: str) -> str:
    y, m = int(month_key[:4]), int(month_key[5:7])
    return f"{y:04d}-{m - 1:02d}" if m > 1 else f"{y - 1:04d}-12"


def _attach_source_ids(records: list, prefix: str, id_map: dict):
    """用稳定消息标识（主号 row id / 工作号 msg_id）覆盖临时消息号。"""
    for rec in records:
        ids = [id_map.get(str(i)) for i in rec.get("source_ids") or []]
        ids = [str(i) for i in ids if i]
        if ids:
            rec["source_event_id"] = f"{prefix}:" + ",".join(sorted(ids))


def _is_card_paid(rec: dict) -> bool:
    if rec.get("card_suffix"):
        return True
    pm = rec.get("payment_method") or ""
    return any(kw in pm for kw in _CARD_PM_KW)


def _split_work_events(records: list, book: str) -> list:
    """工作号事件按付款方式分账套：卡付→个人，零钱/转账→工作。"""
    want_card = book == "personal"
    out = []
    for rec in records:
        if _is_card_paid(rec) != want_card:
            continue
        rec = dict(rec)
        rec["_source"] = "wechat_work"
        out.append(rec)
    return out


def _enrich_categories(records: list, cat_map: dict):
    for rec in records:
        if rec.get("category", "未分类-其他") == "未分类-其他":
            cat = classify_merchant(rec.get("merchant", ""), cat_map)
            if cat and cat != "未分类-其他":
                rec["category"] = cat


# ── 数据源 ──


def fetch_sms(month_key: str) -> tuple:
    start, end = month_bounds(month_key)
    url = (f"{get_sms_api_url()}?date_from={start}&date_to={end}"
           "&limit=50000&kind=finance&merge=1")
    key = _get_sms_api_key()
    if not key:
        raise RuntimeError("SMS API 密钥未配置（[sms_collector] api_key）")

    req = urllib.request.Request(url, headers={"X-API-Key": key})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"SMS API HTTP {e.code}") from e
    except ssl.SSLError:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=120, context=ctx) as resp:
            data = json.loads(resp.read().decode())

    messages = data.get("records", [])
    digest = _md5([
        (m.get("content_hash"), m.get("received"), m.get("body"))
        for m in messages
    ])
    records = parse_sms_records(messages)
    _enrich_categories(records, load_category_map())
    return records, len(messages), digest


def _read_wechat_db(db_path: str, table: str, month_key: str,
                    use_msg_id: bool = False) -> list:
    path = Path(db_path)
    if not path.exists():
        raise RuntimeError(f"微信库不存在：{path}")
    start, end = month_bounds(month_key)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        msg_id_col = "msg_id" if use_msg_id else "NULL AS msg_id"
        sql = (
            f'SELECT id, time, send, sender, type, content, source, {msg_id_col}'
            f' FROM "{table}"'
            " WHERE (typeof(time)='integer' AND time>=? AND time<?)"
            "    OR (typeof(time)='text' AND time>=? AND time<?)"
            " ORDER BY time"
        )
        rows = conn.execute(sql, (_epoch(start), _epoch(end), start, end)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _wechat_source(month_key: str, db_path: str, table: str,
                   prefix: str, use_msg_id: bool = False) -> tuple:
    rows = _read_wechat_db(db_path, table, month_key, use_msg_id)
    messages = []
    id_map = {}
    for r in rows:
        t = _norm_time(r.get("time"))
        if not t:
            continue
        rid = r.get("id")
        sid = r.get("msg_id") or (str(rid) if rid is not None else None)
        if rid is not None and sid:
            id_map[str(rid)] = str(sid)
        messages.append({
            "id": rid,
            "time": t,
            "send": bool(r.get("send")),
            "sender": r.get("sender") or "",
            "type": r.get("type") or "",
            "content": r.get("content") or "",
            "source": r.get("source") or "",
        })

    digest = _md5([
        (m["id"], m["time"], m["send"], m["sender"], m["type"], m["content"])
        for m in messages
    ])
    records = parse_finance_messages(messages)
    _attach_source_ids(records, prefix, id_map)
    _enrich_categories(records, load_category_map())
    return records, len(messages), digest


def fetch_main_wechat(month_key: str) -> tuple:
    return _wechat_source(
        month_key, get_main_wechat_db_path(), get_main_wechat_table(),
        _PREFIX["wechat_main"],
    )


def fetch_work_wechat(month_key: str) -> tuple:
    return _wechat_source(
        month_key, get_wx2_db_path(), get_work_wechat_table(),
        _PREFIX["wechat_work"], use_msg_id=True,
    )


FETCHERS = {
    "sms": fetch_sms,
    "wechat_main": fetch_main_wechat,
    "wechat_work": fetch_work_wechat,
    "notification": fetch_notifications,
}


# ── 同步 ──


class FinanceSync:
    """按 book/月 拉取多源数据并原子重建账本。"""

    def __init__(self, db: Database = None):
        self.db = db or Database()
        self.acct_mgr = AccountManager(self.db)
        self._tx_mgrs = {}

    def tx_mgr(self, book: str) -> TransactionManager:
        if book not in self._tx_mgrs:
            self._tx_mgrs[book] = TransactionManager(
                self.db, self.acct_mgr, book=book)
        return self._tx_mgrs[book]

    def _batch(self, book: str, source: str, period: str):
        return self.db.fetchone(
            "SELECT md5, status, row_count FROM import_batches"
            " WHERE book=? AND source=? AND period=?",
            (book, source, period),
        )

    def _mark(self, book: str, source: str, period: str, md5: str,
              status: str, row_count: int, error: str = None):
        now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        self.db.upsert("import_batches", {
            "book": book,
            "source": source,
            "period": period,
            "md5": md5,
            "status": status,
            "row_count": row_count,
            "error": error,
            "started_at": now,
            "finished_at": now if status != "running" else None,
        }, conflict_cols=["book", "source", "period"])

    def sync_month(self, book: str, month_key: str, force: bool = False,
                   dry_run: bool = False, reclassify: bool = True) -> dict:
        sources = BOOK_SOURCES[book]
        payloads = {}
        failed = False

        for source in sources:
            try:
                records, count, digest, *extra = FETCHERS[source](month_key)
                signals = extra[0] if extra else []
            except Exception as e:
                log.error(f"[{book}] {month_key} {source} 拉取失败：{e}")
                if not dry_run:
                    self._mark(book, source, month_key, "", "failed", 0, str(e)[:500])
                failed = True
                continue
            payloads[source] = (records, count, digest, signals)
            log.info(f"[{book}] {month_key} {source}: {len(records)}/{count} 条"
                     + (f"（信号 {len(signals)} 条）" if signals else ""))

        if failed:
            return {"book": book, "month": month_key, "status": "failed"}

        unchanged = all(
            (b := self._batch(book, source, month_key))
            and b["status"] == "ok" and b["md5"] == digest
            for source, (_, _, digest, _) in payloads.items()
        )
        if unchanged and not force:
            log.info(f"[{book}] {month_key} 数据未变，跳过")
            return {"book": book, "month": month_key, "status": "skipped"}

        if dry_run:
            return {
                "book": book, "month": month_key, "status": "dry-run",
                "sources": {s: len(p[0]) for s, p in payloads.items()},
            }

        wechat_events = []
        for s in ("wechat_main", "wechat_work"):
            if s not in payloads:
                continue
            recs = payloads[s][0]
            if s == "wechat_work":
                recs = _split_work_events(recs, book)
            wechat_events.extend(recs)
        sms_records = payloads["sms"][0] if "sms" in payloads else []
        notification_events = (payloads["notification"][0]
                               if "notification" in payloads else [])
        notification_signals = (payloads["notification"][3]
                                if "notification" in payloads else [])
        try:
            result = self.tx_mgr(book).import_merged(
                wechat_events, sms_records, month_key,
                notification_events=notification_events,
                notification_signals=notification_signals)
        except Exception as e:
            log.error(f"[{book}] {month_key} 入库失败：{e}")
            for source in sources:
                self._mark(book, source, month_key,
                           payloads[source][2], "failed", 0, str(e)[:500])
            return {"book": book, "month": month_key, "status": "failed",
                    "error": str(e)}

        for source in sources:
            _, count, digest, _ = payloads[source]
            self._mark(book, source, month_key, digest, "ok", count)

        if reclassify:
            try:
                stats = reclassify_unclassified(self.db)
                log.info(f"[{book}] {month_key} 重分类："
                         f"{stats['updated']}/{stats['candidates']} 条")
            except Exception as e:
                log.warning(f"[{book}] {month_key} 重分类失败：{e}")

        year, month = int(month_key[:4]), int(month_key[5:7])
        self.tx_mgr(book).calculate_all_balances(year, month, book=book)

        return {
            "book": book, "month": month_key, "status": "ok",
            "flows": result["total_flows"],
            "parsed": result["parsed_flows"],
            "sources": {s: len(p[0]) for s, p in payloads.items()},
        }

    def sync_range(self, months: list, books: list, force: bool = False,
                   dry_run: bool = False, reclassify: bool = True) -> list:
        results = []
        for month_key in months:
            for book in books:
                if month_key < EARLIEST_MONTH[book]:
                    continue
                res = self.sync_month(book, month_key, force=force,
                                      dry_run=dry_run, reclassify=reclassify)
                results.append(res)
                log.info(f"[{book}] {month_key} → {res['status']}")
        return results


def sync_range(months: list, books: list, force: bool = False,
               dry_run: bool = False, db: Database = None,
               reclassify: bool = True) -> list:
    return FinanceSync(db).sync_range(months, books, force=force,
                                      dry_run=dry_run, reclassify=reclassify)


# ── CLI ──


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="个人财务账本四源同步")
    parser.add_argument("--month", help="单月同步 YYYY-MM")
    parser.add_argument("--from-month", dest="from_month", help="起始月 YYYY-MM")
    parser.add_argument("--to-month", dest="to_month", help="截止月 YYYY-MM（含）")
    parser.add_argument("--books", default="personal,work",
                        help="账套，逗号分隔：personal,work")
    parser.add_argument("--backfill", action="store_true",
                        help="从各账套最早数据回填到当前月")
    parser.add_argument("--force", action="store_true", help="忽略 md5 跳过")
    parser.add_argument("--dry-run", action="store_true", help="只拉取统计，不写入")
    parser.add_argument("--no-reclassify", dest="reclassify",
                        action="store_false",
                        help="同步后不按分类表重映射未分类流水")
    args = parser.parse_args(argv)

    books = [b.strip() for b in args.books.split(",") if b.strip()]
    invalid = [b for b in books if b not in BOOK_SOURCES]
    if invalid:
        parser.error(f"未知账套：{','.join(invalid)}")

    now_month = _current_month()
    if args.month:
        months = [args.month]
    elif args.backfill:
        starts = [EARLIEST_MONTH[b] for b in books]
        months = _month_list(min(starts), now_month)
    elif args.from_month:
        months = _month_list(args.from_month, args.to_month or now_month)
    else:
        months = [_prev_month(now_month), now_month]

    sync = FinanceSync()
    results = sync.sync_range(months, books, force=args.force,
                              dry_run=args.dry_run,
                              reclassify=args.reclassify)
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(json.dumps({
        "months": len(months),
        "books": books,
        "status": counts,
        "results": results,
    }, ensure_ascii=False, indent=2))
    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())
