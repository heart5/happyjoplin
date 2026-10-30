# 个人财务系统 — 报告发布到 Joplin（稳定标题 + 正文 md5 幂等）

import argparse
import hashlib
import re
import sys
from configparser import ConfigParser
from datetime import datetime, timedelta
from pathlib import Path

import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.logme import log

from .db import Database

SYSTEM_NOTEBOOK = "财务管理/系统账本"
CARD_NOTEBOOK = "财务管理/银行卡"
STATE_NAME = "finance_notes.ini"

BOOK_TITLES = {"personal": "个人", "work": "工作号"}

# 报告正文含实时生成时间；md5 计算前归一化，避免每次重发
_TS_RE = re.compile(
    r"((?:生成|生成时间|更新时间)[：:]\s*)\d{4}-\d{2}-\d{2} \d{2}:\d{2}")
_CHART_RE = re.compile(r"\{\{chart:([\w-]+)\}\}")


def _sanitize(body: str) -> str:
    # Joplin API 拒绝含 NUL 的正文；原始短信/聊天数据可能带控制字符
    return body.replace("\x00", "")


def _stable_digest(body: str) -> str:
    return hashlib.md5(_TS_RE.sub(r"\1<ts>", body).encode("utf-8")).hexdigest()


class JoplinPublisher:
    """按标题定位笔记；正文 md5 未变则跳过。"""

    def __init__(self, state_path: str = None):
        root = getdirmain()
        self.state_path = Path(state_path or root / "data" / STATE_NAME)
        self.cfg = ConfigParser()
        if self.state_path.exists():
            self.cfg.read(self.state_path, encoding="utf-8")
        self._notebook_ids = {}

    def _save_state(self, title: str, note_id: str, digest: str):
        if not self.cfg.has_section(title):
            self.cfg.add_section(title)
        self.cfg.set(title, "note_id", note_id)
        self.cfg.set(title, "md5", digest)
        self.cfg.set(title, "updated", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as f:
            self.cfg.write(f)

    def _notebook_id(self, notebook: str) -> str:
        if notebook not in self._notebook_ids:
            from func.jpfuncs import searchnotebook
            self._notebook_ids[notebook] = searchnotebook(notebook)
        return self._notebook_ids[notebook]

    @staticmethod
    def _find_note(title: str, parent_id: str):
        from func.jpfuncs import jpapi
        fields = "id,parent_id,title"
        for query in (f'title:"{title}"', f"title:{title}"):
            try:
                results = jpapi.search(query=query, fields=fields).items
            except Exception:
                log.warning(f"Joplin 搜索失败：{query}", exc_info=True)
                continue
            hit = next(
                (n for n in results
                 if n.title == title and n.parent_id == parent_id), None)
            if hit:
                return hit
        return None

    def publish(self, title: str, body: str, notebook: str = SYSTEM_NOTEBOOK,
                force: bool = False, dry_run: bool = False,
                chart_paths: dict = None) -> str:
        body = _sanitize(body)
        digest = _stable_digest(body)
        stored_digest = self.cfg.get(title, "md5", fallback=None)
        stored_id = self.cfg.get(title, "note_id", fallback=None)

        if not force and stored_id and stored_digest == digest:
            log.info(f"《{title}》正文未变，跳过发布")
            return "skipped"
        if dry_run:
            return "dry-run"

        # func.jpfuncs 首次导入时连接 Joplin；延迟到真正发布时
        from func.jpfuncs import (
            createnote, getnote, jpapi, prune_removed_resources,
            updatenote_body, upsertresource,
        )

        parent_id = self._notebook_id(notebook)
        note = self._find_note(title, parent_id)

        if note is not None:
            note_id = note.id
            old_body = getnote(note_id).body or ""
            created = False
        else:
            skeleton = _CHART_RE.sub(
                lambda m: f"_（图表上传中：{m.group(1)}）_", body)
            note_id = createnote(title, skeleton, parent_id=parent_id)
            old_body = ""
            created = True

        final_body = body
        if chart_paths:
            replacement = {}
            for label, path in chart_paths.items():
                try:
                    resource_id = upsertresource(
                        old_body, str(path), label,
                        title=f"finance_{label}.png")
                    if f":/{resource_id}" not in old_body:
                        jpapi.add_resource_to_note(
                            resource_id=resource_id, note_id=note_id)
                    replacement[label] = f"![{label}](:/{resource_id})"
                except Exception as exc:
                    log.warning(f"图表上传失败 {label}：{exc}")
                    replacement[label] = f"_（图表上传失败：{label}）_"
            final_body = _CHART_RE.sub(
                lambda m: replacement.get(
                    m.group(1), f"_（图表缺失：{m.group(1)}）_"), body)
        else:
            final_body = _CHART_RE.sub(
                lambda m: f"_（图表缺失：{m.group(1)}）_", body)

        updatenote_body(note_id, _sanitize(final_body))
        prune_removed_resources(old_body, final_body, exclude_note_id=note_id)
        self._save_state(title, note_id, digest)
        if created:
            log.info(f"已创建笔记《{title}》（{note_id}）于《{notebook}》")
            return "created"
        log.info(f"已更新笔记《{title}》（{note_id}）于《{notebook}》")
        return "updated"


# ── 报告生成与发布 ──


def _month_str(month_key: str) -> str:
    year, month = int(month_key[:4]), int(month_key[5:7])
    return f"{year}年{month:02d}月"


def _month_end(month_key: str) -> str:
    year, month = int(month_key[:4]), int(month_key[5:7])
    nxt = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    return (nxt - timedelta(days=1)).strftime("%Y-%m-%d")


def _flow_count(db: Database, book: str, start: str, end: str) -> int:
    row = db.fetchone(
        "SELECT COUNT(*) AS n FROM account_flows"
        " WHERE book=? AND tx_date BETWEEN ? AND ?", (book, start, end))
    return row["n"] if row else 0


def publish_month(db: Database, publisher: JoplinPublisher, month_key: str,
                  books=("personal", "work"), force=False, dry_run=False) -> dict:
    from .reports.monthly import generate_monthly_report
    from .reports.charts import build_monthly_charts

    year, month = int(month_key[:4]), int(month_key[5:7])
    results = {}
    for book in books:
        title = f"{BOOK_TITLES[book]}财务月报 - {_month_str(month_key)}"
        if _flow_count(db, book, f"{month_key}-01", _month_end(month_key)) == 0:
            log.info(f"《{title}》本月无流水，跳过发布")
            results[title] = "empty"
            continue
        body = generate_monthly_report(db, year, month, book=book)
        charts = {}
        if not dry_run:
            chart_dir = Path.home() / "tmp" / "finance_charts" / month_key / book
            try:
                charts = build_monthly_charts(db, book, year, month, chart_dir)
            except Exception:
                log.warning(f"《{title}》图表生成失败", exc_info=True)
        results[title] = publisher.publish(title, body, force=force,
                                           dry_run=dry_run, chart_paths=charts)
    return results


def publish_annual(db: Database, publisher: JoplinPublisher, year: int,
                   books=("personal", "work"), force=False, dry_run=False) -> dict:
    from .reports.annual_report import generate_annual_report

    results = {}
    for book in books:
        title = f"{BOOK_TITLES[book]}财务年报 - {year}年"
        if _flow_count(db, book, f"{year}-01-01", f"{year}-12-31") == 0:
            log.info(f"《{title}》本年无流水，跳过发布")
            results[title] = "empty"
            continue
        body = generate_annual_report(db, year, book=book)
        results[title] = publisher.publish(title, body, force=force, dry_run=dry_run)
    return results


def publish_net_worth(db: Database, publisher: JoplinPublisher, month_key: str,
                      force=False, dry_run=False) -> dict:
    from .reports.net_worth import generate_net_worth_report

    title = f"净资产报告 - {_month_str(month_key)}"
    body = generate_net_worth_report(db, _month_end(month_key), book="combined")
    return {title: publisher.publish(title, body, force=force, dry_run=dry_run)}


def publish_credit_cards(db: Database, publisher: JoplinPublisher,
                         force=False, dry_run=False) -> dict:
    from .reports.credit_cards import generate_credit_cards_report

    title = "信用卡状态（自动）"
    body = generate_credit_cards_report(db)
    return {title: publisher.publish(title, body, notebook=CARD_NOTEBOOK,
                                     force=force, dry_run=dry_run)}


def publish_status(db: Database, publisher: JoplinPublisher,
                   force=False, dry_run=False) -> dict:
    rows = db.fetchall(
        """SELECT book, source, period, status, row_count, error, finished_at
           FROM import_batches ORDER BY period DESC, book, source LIMIT 40""")
    total = db.fetchone("SELECT COUNT(*) AS n FROM account_flows")["n"]
    notif = db.fetchone(
        "SELECT COUNT(*) AS n, MAX(tx_date) AS last FROM account_flows"
        " WHERE source='notification'")
    signals = db.fetchone(
        "SELECT COUNT(*) AS n, MAX(observed_at) AS last FROM notification_signals")
    last_ok = db.fetchone(
        "SELECT MAX(finished_at) AS t FROM import_batches WHERE status='ok'")["t"]
    failed = [r for r in rows if r["status"] == "failed"]

    lines = ["# 财务系统运行状态", ""]
    lines.append(f"> 更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}"
                 f" ｜ 流水总数：{total}"
                 f" ｜ 通知流水：{notif['n']}（最近 {notif['last'] or '—'}）"
                 f" ｜ 通知信号：{signals['n']}（最近 {signals['last'] or '—'}）")
    lines.append("")
    if failed:
        lines += ["## 失败批次", ""]
        for r in failed:
            lines.append(f"- [{r['book']}] {r['period']} {r['source']}："
                         f"{(r['error'] or '')[:200]}")
        lines.append("")
    lines += ["## 最近导入批次", "",
              "| 月份 | 账套 | 来源 | 状态 | 原始条数 | 完成时间 |",
              "|------|------|------|------|------|------|"]
    for r in rows:
        lines.append(f"| {r['period']} | {r['book']} | {r['source']} | "
                     f"{r['status']} | {r['row_count']} | "
                     f"{r['finished_at'] or '—'} |")
    lines.append("")

    recent_notif = db.fetchall(
        """SELECT f.tx_date, f.amount, f.direction, f.tx_type, f.merchant,
                  a.name AS account_name
           FROM account_flows f
           LEFT JOIN accounts a ON a.id = f.account_id
           WHERE f.source='notification'
           ORDER BY f.tx_date DESC, f.id DESC LIMIT 10""")
    if recent_notif:
        lines += ["## 通知入账流水（最近 10 条）", "",
                  "| 日期 | 金额 | 方向 | 类型 | 账户 | 摘要 |",
                  "|------|------|------|------|------|------|"]
        for r in recent_notif:
            direction = "收入" if r["direction"] == "inflow" else "支出"
            account = r["account_name"] or "—（未匹配）"
            lines.append(f"| {r['tx_date']} | {r['amount']:,.2f} | {direction} | "
                         f"{r['tx_type']} | {account} | {r['merchant'] or '—'} |")
        lines.append("")

    lines.append(f"**最近成功同步**：{last_ok or '—'}")
    lines += ["", "---", "*由 finance sync 自动维护*", ""]

    title = "财务系统运行状态"
    return {title: publisher.publish(title, "\n".join(lines),
                                     force=force, dry_run=dry_run)}


# ── CLI ──


def _add_common(p):
    p.add_argument("--force", action="store_true", help="忽略 md5 强制重发")
    p.add_argument("--dry-run", dest="dry_run", action="store_true")
    p.add_argument("--books", default="personal,work")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="财务报告发布到 Joplin")
    sub = parser.add_subparsers(dest="cmd")
    sub.required = True

    p_m = sub.add_parser("monthly", help="月度报告 + 账户总览")
    p_m.add_argument("--month", required=True)
    _add_common(p_m)

    p_a = sub.add_parser("annual", help="年度报告")
    p_a.add_argument("--year", type=int, required=True)
    _add_common(p_a)

    p_n = sub.add_parser("net-worth", help="净资产报告")
    p_n.add_argument("--month", required=True)
    _add_common(p_n)

    p_c = sub.add_parser("credit-cards", help="信用卡状态")
    _add_common(p_c)

    p_s = sub.add_parser("status", help="运行状态")
    _add_common(p_s)

    p_all = sub.add_parser("all", help="月报 + 净资产 + 信用卡 + 状态")
    p_all.add_argument("--month", required=True)
    p_all.add_argument("--year", type=int)
    _add_common(p_all)

    args = parser.parse_args(argv)
    books = tuple(b.strip() for b in args.books.split(",") if b.strip())
    db = Database()
    publisher = JoplinPublisher()

    if args.cmd == "monthly":
        result = publish_month(db, publisher, args.month, books,
                               args.force, args.dry_run)
    elif args.cmd == "annual":
        result = publish_annual(db, publisher, args.year, books,
                                args.force, args.dry_run)
    elif args.cmd == "net-worth":
        result = publish_net_worth(db, publisher, args.month,
                                   args.force, args.dry_run)
    elif args.cmd == "credit-cards":
        result = publish_credit_cards(db, publisher, args.force, args.dry_run)
    elif args.cmd == "status":
        result = publish_status(db, publisher, args.force, args.dry_run)
    else:
        result = {}
        result.update(publish_month(db, publisher, args.month, books,
                                    args.force, args.dry_run))
        result.update(publish_net_worth(db, publisher, args.month,
                                        args.force, args.dry_run))
        result.update(publish_credit_cards(db, publisher, args.force,
                                           args.dry_run))
        if args.year:
            result.update(publish_annual(db, publisher, args.year, books,
                                         args.force, args.dry_run))
        result.update(publish_status(db, publisher, args.force, args.dry_run))

    for title, status in result.items():
        print(f"{status:<8} {title}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
