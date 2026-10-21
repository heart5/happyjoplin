# 个人财务系统 — 账户余额报告

from ..db import Database
from ..transactions import TransactionManager
from .common import money, now_text
from .monthly import BOOK_LABELS

__all__ = ["generate_balance_report"]

_TYPE_LABELS = {
    "bank": "储蓄卡", "payment": "支付平台", "credit_card": "信用卡",
    "loan": "贷款", "investment": "投资", "cash": "现金",
}


def generate_balance_report(db: Database, year: int, month: int,
                            book: str = "personal",
                            account_id: int = None) -> str:
    tx_mgr = TransactionManager(db, book=book)
    for b in (["personal", "work"] if book == "all" else [book]):
        tx_mgr.calculate_all_balances(year, month, book=b)

    books = ["personal", "work"] if book == "all" else [book]
    placeholders = ",".join("?" * len(books))
    next_month = (f"{year + 1:04d}-01-01" if month == 12
                  else f"{year:04d}-{month + 1:02d}-01")
    sql = (
        "SELECT ab.*, a.name, a.type, a.book, a.bank, a.card_suffix"
        " FROM account_balances ab JOIN accounts a ON ab.account_id = a.id"
        f" WHERE a.book IN ({placeholders}) AND ab.year=? AND ab.month=?"
        " AND a.is_active=1 AND a.opening_date IS NOT NULL"
        " AND a.opening_date < ?"
    )
    params = books + [year, month, next_month]
    if account_id:
        sql += " AND a.id=?"
        params.append(account_id)
    sql += (" ORDER BY CASE a.type WHEN 'loan' THEN 3 WHEN 'credit_card' THEN 2"
            " ELSE 1 END, a.book, a.id")
    rows = db.fetchall(sql, params)

    lines = [f"# 账户余额 — {year}年{month:02d}月", ""]
    lines.append(f"> 生成：{now_text()} ｜ 仅含已设余额锚点的账户")
    lines.append("")

    assets = [r for r in rows if r["type"] not in ("credit_card", "loan")]
    liabilities = [r for r in rows if r["type"] in ("credit_card", "loan")]

    for title, group in (("资产账户", assets), ("负债账户", liabilities)):
        if not group:
            continue
        lines += [f"## {title}", "",
                  "| 账套 | 账户 | 类型 | 期初 | 流入 | 流出 | 期末 | 状态 |",
                  "|------|------|------|------|------|------|------|------|"]
        for r in group:
            lines.append(
                f"| {BOOK_LABELS.get(r['book'], r['book'])} | {r['name']} | "
                f"{_TYPE_LABELS.get(r['type'], r['type'])} | "
                f"{money(r['opening_balance'])} | {money(r['total_inflow'])} | "
                f"{money(r['total_outflow'])} | {money(r['closing_balance'])} | "
                f"{'估算' if r['is_estimated'] else '已校准'} |")
        lines.append("")

    asset_total = sum(r["closing_balance"] for r in assets)
    liability_total = sum(r["closing_balance"] for r in liabilities)
    lines += ["## 合计", "", "| 项目 | 金额 |", "|------|------|"]
    lines.append(f"| 资产合计 | {money(asset_total)} |")
    lines.append(f"| 负债合计 | {money(liability_total)} |")
    lines.append(f"| 净资产 | {money(asset_total - liability_total)} |")
    lines += ["", "---", f"*生成时间：{now_text()}*", ""]
    return "\n".join(lines)
