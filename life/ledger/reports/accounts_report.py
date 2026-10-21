# 个人财务系统 — 账户总览报告

from ..db import Database
from ..transactions import TransactionManager
from .common import money, now_text
from .monthly import BOOK_LABELS

__all__ = ["generate_accounts_report"]

_TYPE_LABELS = {
    "bank": "储蓄卡", "payment": "支付平台", "credit_card": "信用卡",
    "loan": "贷款", "investment": "投资", "cash": "现金",
}


def generate_accounts_report(db: Database, year: int, month: int,
                             book: str = "all") -> str:
    books = ["personal", "work"] if book == "all" else [book]
    for b in books:
        TransactionManager(db, book=b).calculate_all_balances(year, month, book=b)

    balance_map = {
        r["account_id"]: r["closing_balance"]
        for r in db.fetchall(
            "SELECT account_id, closing_balance FROM account_balances"
            " WHERE year=? AND month=?", (year, month))
    }
    stats = {
        r["account_id"]: r
        for r in db.fetchall(
            """SELECT account_id, COUNT(*) AS n, MAX(tx_date) AS last_date
               FROM account_flows WHERE book IN (%s)
               GROUP BY account_id""" % ",".join("?" * len(books)),
            books)
    }

    rows = db.fetchall(
        f"SELECT * FROM accounts WHERE book IN ({','.join('?' * len(books))})"
        " ORDER BY book, type, id", books)

    lines = [f"# 账户总览 — {year}年{month:02d}月", ""]
    lines.append(f"> 生成：{now_text()} ｜ 账户 {len(rows)} 个")
    lines.append("")
    lines.append("| 账套 | 账户 | 类型 | 银行/机构 | 尾号 | 余额 | 流水数 | 最后交易 |")
    lines.append("|------|------|------|------|------|------|------|------|")
    for a in rows:
        s = stats.get(a["id"], {})
        name = a["name"] + ("" if a["is_active"] else "（停用）")
        lines.append(
            f"| {BOOK_LABELS.get(a['book'], a['book'])} | {name} | "
            f"{_TYPE_LABELS.get(a['type'], a['type'])} | "
            f"{a['bank'] or a['institution'] or a['platform'] or '—'} | "
            f"{a['card_suffix'] or '—'} | {money(balance_map.get(a['id'], 0))} | "
            f"{s.get('n', 0)} | {s.get('last_date', '—')} |")
    lines += ["", "---", f"*生成时间：{now_text()}*", ""]
    return "\n".join(lines)
