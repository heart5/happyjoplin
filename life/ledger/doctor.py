# 个人财务系统 — 数据体检（只读统计，可发布到 Joplin）

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from .db import Database
from .publish import JoplinPublisher, SYSTEM_NOTEBOOK
from .reports.common import money, now_text, source_label

__all__ = ["collect", "render", "publish_report", "main"]

# 疑似口径错误关键词：商户/对手方/原始文本中的语义线索
_SUSPECT_TRANSFER = re.compile(
    r"对方已收款|other party|转账给|代付|转存|微信转账|汇入|汇出|归还借款")
_SUSPECT_INCOME = re.compile(
    r"工资|奖金|绩效|补贴|报销|利息|收益|分红|退款|代发")

_TOP_N = 20

# 静态数据源（手工刷新的 CSV/xlsx）超过该天数未更新时在体检中提示
_SOURCE_STALE_DAYS = 45


def _recent_months(count: int) -> list:
    now = datetime.now()
    y, m = now.year, now.month
    months = []
    for _ in range(max(count, 1)):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return list(reversed(months))


def _month_span(first: str, last: str = None) -> list:
    y, m = int(first[:4]), int(first[5:7])
    last = last or datetime.now().strftime("%Y-%m")
    ey, em = int(last[:4]), int(last[5:7])
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def _next_month(month_key: str) -> str:
    y, m = int(month_key[:4]), int(month_key[5:7])
    return f"{y + 1:04d}-01" if m == 12 else f"{y:04d}-{m + 1:02d}"


def _is_unclassified(flow: dict) -> bool:
    if not flow["category_id"]:
        return True
    name = flow["category_name"] or ""
    # 「未分类-回执」= 微信支付回执本身无商户字段，属数据源限制，
    # 不计入需人工处理的未分类率，单独统计（2026-11-12）。
    if name == "未分类-回执":
        return False
    return name in ("", "未分类-其他")


def _is_receipt_unclassified(flow: dict) -> bool:
    return (flow["category_name"] or "") == "未分类-回执"


_SCAN_HINT = re.compile(r"交易类型|摘要|交易对象|商户")


def _text_of(flow: dict) -> str:
    """语义线索只看商户/对手方；两者都缺时才有条件退回原始文本。

    原始回执常带「去报销」这类用户备注，混进来会把正常消费误报成收入，
    所以只在原文含银行字段（交易类型/摘要/交易对象/商户）时才扫描。
    """
    core = " ".join(str(x) for x in (flow["merchant"], flow["counterparty"]) if x)
    if core:
        return core
    raw = flow["raw_data"] or ""
    return raw if _SCAN_HINT.search(raw) else ""


def _blank_source() -> dict:
    return {"n": 0, "cons_n": 0, "spend": 0.0, "uncat_n": 0, "uncat_amt": 0.0,
            "receipt_n": 0, "receipt_amt": 0.0,
            "empty_m_n": 0, "empty_m_amt": 0.0}


def _acc_spend(stat: dict, flow: dict):
    stat["n"] += 1
    if flow["tx_type"] != "consumption" or flow["direction"] != "outflow":
        return
    stat["cons_n"] += 1
    stat["spend"] += flow["amount"]
    if _is_receipt_unclassified(flow):
        stat["receipt_n"] += 1
        stat["receipt_amt"] += flow["amount"]
    if _is_unclassified(flow):
        stat["uncat_n"] += 1
        stat["uncat_amt"] += flow["amount"]
    if not (flow["merchant"] or "").strip():
        stat["empty_m_n"] += 1
        stat["empty_m_amt"] += flow["amount"]


def _fetch_flows(db, books: tuple, start: str, end: str) -> list:
    marks = ",".join("?" * len(books))
    return db.fetchall(
        "SELECT af.id, af.book, af.source, af.tx_date, af.amount, af.direction,"
        " af.tx_type, af.category_id, af.merchant, af.counterparty, af.raw_data,"
        " af.account_id,"
        " a.name AS account_name, a.type AS account_type,"
        " c.name AS category_name"
        " FROM account_flows af"
        " LEFT JOIN accounts a ON a.id = af.account_id"
        " LEFT JOIN categories c ON c.id = af.category_id"
        f" WHERE af.book IN ({marks}) AND af.tx_date >= ? AND af.tx_date < ?",
        (*books, start, end),
    )


def _source_freshness(data_dir: Path) -> list:
    """静态数据源（支付宝 CSV / 微信账单 xlsx）的新鲜度。

    这些文件靠手工导出刷新，不参与每日 sync；过期意味着账本缺近段数据。
    每个目录取最新一个文件，报告其 mtime 与距今天数。
    """
    dirs = [
        ("支付宝明细", "alipay", "*.csv"),
        ("工作号账单", "wechat-bill", "*.xlsx"),
        ("主号账单", "wechat-bill-main", "*.xlsx"),
    ]
    result = []
    for label, subdir, pattern in dirs:
        files = sorted((data_dir / subdir).glob(pattern))
        if not files:
            result.append({"label": label, "path": f"data/{subdir}/",
                           "exists": False, "age_days": None})
            continue
        path = files[-1]
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        age_days = (datetime.now() - mtime).days
        result.append({
            "label": label, "path": f"data/{subdir}/{path.name}",
            "exists": True, "mtime": mtime.strftime("%Y-%m-%d %H:%M"),
            "age_days": age_days,
            "stale": age_days > _SOURCE_STALE_DAYS,
        })
    return result


def collect(db: Database, books: tuple, months: list) -> dict:
    start, end = f"{months[0]}-01", f"{_next_month(months[-1])}-01"
    flows = _fetch_flows(db, books, start, end)

    by_source = defaultdict(_blank_source)
    by_month = defaultdict(_blank_source)
    transfer_suspects = []
    income_suspects = []
    unmatched = []
    dup_groups = defaultdict(list)

    for f in flows:
        _acc_spend(by_source[(f["book"], f["source"])], f)
        _acc_spend(by_month[f["tx_date"][:7]], f)
        if not f["account_id"]:
            unmatched.append(f)
        if f["tx_type"] == "consumption" and f["direction"] == "outflow":
            text = _text_of(f)
            if _SUSPECT_TRANSFER.search(text):
                transfer_suspects.append(f)
            elif _SUSPECT_INCOME.search(text):
                income_suspects.append(f)
        if (f["account_id"]
                and f["tx_type"] not in ("adjustment", "transfer",
                                         "loan_disbursement", "loan_repayment")):
            key = (f["book"], f["account_id"], f["tx_date"], f["direction"],
                   round(f["amount"], 2))
            dup_groups[key].append(f)

    def _same_merchant(v: list) -> bool:
        """组内商户名归一后是否一致——一致才可能是真重复。

        同日同额但商户明显不同（如两家便利店各 25 元）是两笔真实消费，
        不算疑似重复（2026-11-12）。
        """
        names = {(r["merchant"] or "").strip() for r in v}
        names.discard("")
        return len(names) <= 1

    duplicates = [
        {"key": k, "rows": v,
         "cross_source": len({r["source"] for r in v}) > 1,
         "high_confidence": _same_merchant(v)}
        for k, v in dup_groups.items() if len(v) > 1
    ]
    # 高置信（同商户）排前面，低置信降为提示
    duplicates.sort(key=lambda d: (not d["high_confidence"],
                                   not d["cross_source"], -len(d["rows"]),
                                   d["key"][2]))

    unanchored = db.fetchall(
        """SELECT a.book, a.name, a.type, a.card_suffix,
                  (SELECT COUNT(*) FROM account_flows f
                    WHERE f.account_id = a.id) AS flow_count
           FROM accounts a
           WHERE a.is_active=1 AND (a.opening_date IS NULL OR a.opening_date='')
           ORDER BY a.book, a.type, a.id""")

    bound = int(months[-1][:4]) * 100 + int(months[-1][5:7])
    negative = db.fetchall(
        """SELECT a.book, a.name, a.type, ab.year, ab.month,
                  ab.closing_balance
           FROM account_balances ab JOIN accounts a ON a.id = ab.account_id
           WHERE a.is_active=1 AND ab.closing_balance < -0.005
             AND ab.year*100 + ab.month <= ?
             AND ab.rowid = (
                 SELECT ab2.rowid FROM account_balances ab2
                 WHERE ab2.account_id = ab.account_id
                   AND ab2.year*100 + ab2.month <= ?
                 ORDER BY ab2.year DESC, ab2.month DESC LIMIT 1)
           ORDER BY a.book, a.type, a.id""",
        (bound, bound))

    batches = db.fetchall(
        "SELECT book, source, period, status, row_count, error"
        " FROM import_batches WHERE period>=? AND period<=? AND status!='ok'"
        " ORDER BY period DESC, book, source",
        (months[0], months[-1]))

    spend = sum(s["spend"] for s in by_source.values())
    cons_n = sum(s["cons_n"] for s in by_source.values())
    uncat_n = sum(s["uncat_n"] for s in by_source.values())
    uncat_amt = sum(s["uncat_amt"] for s in by_source.values())
    receipt_n = sum(s["receipt_n"] for s in by_source.values())
    receipt_amt = sum(s["receipt_amt"] for s in by_source.values())
    empty_m_n = sum(s["empty_m_n"] for s in by_source.values())

    return {
        "months": months,
        "books": list(books),
        "flow_count": len(flows),
        "spend": spend,
        "cons_n": cons_n,
        "uncat_n": uncat_n,
        "uncat_amt": uncat_amt,
        "uncat_rate": uncat_n / cons_n if cons_n else 0.0,
        "receipt_n": receipt_n,
        "receipt_amt": receipt_amt,
        "empty_merchant_n": empty_m_n,
        "by_source": sorted(by_source.items()),
        "by_month": sorted(by_month.items()),
        "transfer_suspects": transfer_suspects,
        "income_suspects": income_suspects,
        "unmatched": unmatched,
        "duplicates": duplicates,
        "unanchored": unanchored,
        "negative_balances": negative,
        "bad_batches": batches,
        "data_sources": _source_freshness(Path(db.db_path).parent),
    }


def _flow_rows(rows: list, limit: int = _TOP_N) -> list:
    rows = sorted(rows, key=lambda r: -r["amount"])[:limit]
    return [
        f"| {r['tx_date']} | {r['book']} | {source_label(r['source'])}"
        f" | {money(r['amount'])} | {(r['merchant'] or r['counterparty'] or '—')[:40]} |"
        for r in rows
    ]


def render(stats: dict) -> str:
    months = stats["months"]
    period = months[0] if len(months) == 1 else f"{months[0]} ~ {months[-1]}"
    lines = ["# 财务数据体检", ""]
    lines.append(f"> 生成时间：{now_text()} ｜ 期间：{period}"
                 f" ｜ 账套：{','.join(stats['books'])}"
                 f" ｜ 流水 {stats['flow_count']} 条")
    lines.append("")

    uncat_target = 0.10
    lines += ["## 〇、结论", ""]
    lines.append(f"- 消费支出：{money(stats['spend'])}")
    lines.append(f"- 消费未分类：{stats['uncat_n']} 笔 {money(stats['uncat_amt'])}"
                 f"（占消费 {stats['uncat_rate']:.1%}）")
    lines.append(f"- 支付回执无商户（数据源限制，不计入未分类率）："
                 f"{stats['receipt_n']} 笔 {money(stats['receipt_amt'])}")
    lines.append(f"- 疑似转账记为消费：{len(stats['transfer_suspects'])} 笔")
    lines.append(f"- 疑似收入记为支出：{len(stats['income_suspects'])} 笔")
    lines.append(f"- 未匹配账户：{len(stats['unmatched'])} 笔")
    cross_dups = sum(1 for d in stats["duplicates"]
                     if d["cross_source"] and d["high_confidence"])
    hard_dups = sum(1 for d in stats["duplicates"] if d["high_confidence"])
    lines.append(f"- 疑似重复：{len(stats['duplicates'])} 组"
                 f"（高置信 {hard_dups} 组，其中跨来源 {cross_dups} 组；"
                 "低置信=同日同额不同商户，多为真实多笔消费）")
    status = ("达标" if not stats["transfer_suspects"] and cross_dups == 0
              and stats["uncat_rate"] < uncat_target else "待处理")
    lines.append(f"- 判定：**{status}**（未分类率目标 <{uncat_target:.0%}，"
                 "转账误记与高置信跨来源重复目标 0）")
    lines.append("")

    lines += ["## 一、分类与商户质量", "",
              "| 账套 | 来源 | 流水 | 消费支出 | 未分类 | 未分类额 | 回执无商户 | 空商户 |",
              "|------|------|------|------|------|------|------|------|"]
    for (book, source), s in stats["by_source"]:
        lines.append(
            f"| {book} | {source_label(source)} | {s['n']} | "
            f"{money(s['spend'])} | {s['uncat_n']} | {money(s['uncat_amt'])} | "
            f"{s['receipt_n']} | {s['empty_m_n']} |")
    lines.append("")

    lines += ["## 二、月度走势", "",
              "| 月份 | 流水 | 消费支出 | 未分类 | 未分类额 | 空商户 |",
              "|------|------|------|------|------|------|"]
    for month_key, s in stats["by_month"]:
        lines.append(f"| {month_key} | {s['n']} | {money(s['spend'])} | "
                     f"{s['uncat_n']} | {money(s['uncat_amt'])} | "
                     f"{s['empty_m_n']} |")
    lines.append("")

    lines += ["## 三、疑似口径错误", ""]
    for title, rows in (("疑似转账记为消费", stats["transfer_suspects"]),
                        ("疑似收入记为支出", stats["income_suspects"])):
        lines.append(f"### {title} — {len(rows)} 笔")
        lines.append("")
        if rows:
            lines += ["| 日期 | 账套 | 来源 | 金额 | 商户/摘要 |",
                      "|------|------|------|------|------|"]
            lines += _flow_rows(rows)
            if len(rows) > _TOP_N:
                lines.append(f"| … | 其余 {len(rows) - _TOP_N} 笔略 | | | |")
        else:
            lines.append("无。")
        lines.append("")

    unmatched = stats["unmatched"]
    lines.append(f"### 未匹配账户 — {len(unmatched)} 笔")
    lines.append("")
    if unmatched:
        lines += ["| 日期 | 账套 | 来源 | 方向 | 金额 | 商户/摘要 |",
                  "|------|------|------|------|------|------|"]
        for r in sorted(unmatched, key=lambda x: -x["amount"])[:_TOP_N]:
            d = "收入" if r["direction"] == "inflow" else "支出"
            lines.append(
                f"| {r['tx_date']} | {r['book']} | "
                f"{source_label(r['source'])} | {d} | "
                f"{money(r['amount'])} | "
                f"{(r['merchant'] or r['counterparty'] or '—')[:40]} |")
        if len(unmatched) > _TOP_N:
            lines.append(f"| … | 其余 {len(unmatched) - _TOP_N} 笔略 | | | | |")
    else:
        lines.append("无。")
    lines.append("")

    lines += ["## 四、疑似重复（同日同账户同额同向）", ""]
    if stats["duplicates"]:
        lines += ["| 日期 | 账套 | 账户 | 方向 | 金额 | 笔数 | 来源 | 摘要 |",
                  "|------|------|------|------|------|------|------|------|"]
        for d in stats["duplicates"][:_TOP_N]:
            book, account_id, tx_date, direction, amount = d["key"]
            name = d["rows"][0]["account_name"] or f"#{account_id}"
            sources = "/".join(sorted({source_label(r["source"])
                                       for r in d["rows"]}))
            merchants = "、".join(sorted({(r["merchant"] or "—")[:12]
                                          for r in d["rows"]}))
            src_flag = "**跨来源**" if d["cross_source"] else "同来源"
            conf = "高置信" if d["high_confidence"] else "低置信(商户不同)"
            lines.append(
                f"| {tx_date} | {book} | {name} | "
                f"{'收入' if direction == 'inflow' else '支出'} | "
                f"{money(amount)} | {len(d['rows'])} | {sources} | "
                f"{merchants}（{src_flag}·{conf}） |")
        lines.append("")
        lines.append("*高置信=商户归一后一致，可能是真重复需核对；"
                     "低置信=同日同额但商户不同，多为真实多笔消费。*")
    else:
        lines.append("无。")
    lines.append("")

    lines += ["## 五、账户与余额", ""]
    zombie = [a for a in stats["unanchored"] if a["flow_count"] == 0]
    pending = [a for a in stats["unanchored"] if a["flow_count"] > 0]
    lines.append(f"### 待锚定账户（有流水，缺真实余额）— {len(pending)} 个")
    lines.append("")
    if pending:
        for a in pending:
            suffix = f"（尾号 {a['card_suffix']}）" if a["card_suffix"] else ""
            lines.append(f"- [{a['book']}] {a['name']} · {a['type']}{suffix}"
                         f"（{a['flow_count']} 笔）")
    else:
        lines.append("无。")
    lines.append("")
    lines.append(f"### 未使用账户（0 流水且无锚点，保留备用）— {len(zombie)} 个")
    lines.append("")
    if zombie:
        for a in zombie:
            suffix = f"（尾号 {a['card_suffix']}）" if a["card_suffix"] else ""
            lines.append(f"- [{a['book']}] {a['name']} · {a['type']}{suffix}")
        lines.append("")
        lines.append("*用户决定：**不停用**，保留备用（2026-11-13）。*")
    else:
        lines.append("无。")
    lines.append("")
    lines.append(f"### 余额为负 — {len(stats['negative_balances'])} 个")
    lines.append("")
    if stats["negative_balances"]:
        lines += ["| 账套 | 账户 | 类型 | 期间 | 期末 |",
                  "|------|------|------|------|------|"]
        for r in stats["negative_balances"]:
            lines.append(f"| {r['book']} | {r['name']} | {r['type']} | "
                         f"{r['year']}-{r['month']:02d} | "
                         f"{money(r['closing_balance'])} |")
        lines.append("")
        lines.append("*未锚定账户的余额是用历史流水硬滚的估值（`is_estimated=1`），"
                     "负数不代表真实透支，只说明缺锚点；已锚定账户出现负余额才需核对。*")
    else:
        lines.append("无。")
    lines.append("")

    lines += ["## 六、导入批次异常", ""]
    if stats["bad_batches"]:
        lines += ["| 月份 | 账套 | 来源 | 状态 | 原始条数 | 错误 |",
                  "|------|------|------|------|------|------|"]
        for r in stats["bad_batches"]:
            lines.append(f"| {r['period']} | {r['book']} | {r['source']} | "
                         f"{r['status']} | {r['row_count']} | "
                         f"{(r['error'] or '—')[:60]} |")
    else:
        lines.append("无。")
    lines.append("")

    lines += ["## 七、静态数据源新鲜度", "",
              "| 数据源 | 最新文件 | 最后刷新 | 距今 |",
              "|------|------|------|------|"]
    for s in stats["data_sources"]:
        if not s["exists"]:
            lines.append(f"| {s['label']} | {s['path']} | — | 未找到 |")
        else:
            flag = " ⚠ 需刷新" if s["stale"] else ""
            lines.append(f"| {s['label']} | {s['path']} | {s['mtime']} | "
                         f"{s['age_days']} 天{flag} |")
    lines.append("")
    lines.append(f"*超过 {_SOURCE_STALE_DAYS} 天未刷新会标 ⚠。这些文件**只能手工导出**"
                 "（支付宝/微信无对外接口，用户 2026-11-13 确认），不参与每日 sync；"
                 "本检查只作提醒，过期意味着账本缺近段数据。*")
    lines.append("")

    lines += ["", "---", f"*生成时间：{now_text()} ｜ 数据来源：finance.db*", ""]
    return "\n".join(lines)


def publish_report(db: Database, months: list, books: tuple,
                   force: bool = False, dry_run: bool = False) -> str:
    body = render(collect(db, books, months))
    publisher = JoplinPublisher()
    return publisher.publish("财务数据体检", body, notebook=SYSTEM_NOTEBOOK,
                             force=force, dry_run=dry_run)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="财务数据体检（只读）")
    parser.add_argument("--month", help="单月 YYYY-MM")
    parser.add_argument("--months", type=int, default=3,
                        help="最近 N 个月（默认 3）")
    parser.add_argument("--from-month", dest="from_month")
    parser.add_argument("--to-month", dest="to_month")
    parser.add_argument("--books", default="personal,work")
    parser.add_argument("--publish", action="store_true",
                        help="发布到 Joplin《财务数据体检》")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--json", action="store_true", help="输出统计 JSON")
    args = parser.parse_args(argv)

    books = tuple(b.strip() for b in args.books.split(",") if b.strip())
    if args.month:
        months = [args.month]
    elif args.from_month:
        months = _month_span(args.from_month, args.to_month)
    else:
        months = _recent_months(args.months)

    db = Database()
    stats = collect(db, books, months)

    if args.json:
        print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
    else:
        body = render(stats)
        print(body)
        if args.publish:
            status = JoplinPublisher().publish(
                "财务数据体检", body, notebook=SYSTEM_NOTEBOOK,
                force=args.force, dry_run=args.dry_run)
            print(f"\n发布状态：{status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
