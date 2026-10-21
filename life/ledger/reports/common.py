# 个人财务系统 — 报告共用查询与格式化

from collections import defaultdict
from datetime import datetime

from ..transactions import month_bounds

__all__ = [
    "fetch_flows", "fetch_year_flows", "money", "pct",
    "sum_dir", "debt_total", "month_label", "now_text", "by_category",
]

_FLOW_SELECT = (
    "SELECT af.*, a.name AS account_name, a.type AS account_type,"
    " c.name AS category_name FROM account_flows af"
    " LEFT JOIN accounts a ON af.account_id = a.id"
    " LEFT JOIN categories c ON af.category_id = c.id"
)


def fetch_flows(db, book: str, start: str, end: str) -> list:
    return db.fetchall(
        _FLOW_SELECT + " WHERE af.book=? AND af.tx_date>=? AND af.tx_date<?"
        " ORDER BY af.tx_date, af.id",
        (book, start, end),
    )


def fetch_year_flows(db, book: str, year: int) -> list:
    return fetch_flows(db, book, f"{year:04d}-01-01", f"{year + 1:04d}-01-01")


def money(value) -> str:
    value = float(value or 0)
    sign = "-" if value < 0 else ""
    return f"{sign}¥{abs(value):,.2f}"


def pct(part, whole) -> str:
    return f"{part / whole * 100:.1f}%" if whole else "-"


def month_label(year: int, month: int) -> str:
    return f"{year}年{month:02d}月"


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def sum_dir(flows: list, direction: str, tx_types=None) -> float:
    return sum(
        f["amount"] for f in flows
        if f["direction"] == direction
        and (tx_types is None or f["tx_type"] in tx_types)
    )


def debt_total(flows: list, tx_type: str, primary_direction: str) -> dict:
    """借贷总额：主侧计入；另一侧仅在未配对时计入，避免双边重复。"""
    total = count = 0
    for f in flows:
        if f["tx_type"] != tx_type:
            continue
        if f["direction"] == primary_direction:
            total += f["amount"]
            count += 1
        elif not f.get("linked_flow_id"):
            total += f["amount"]
            count += 1
    return {"total": total, "count": count}


def by_category(flows: list, direction: str = "outflow") -> list:
    sums = defaultdict(float)
    for f in flows:
        if f["direction"] == direction:
            sums[f.get("category_name") or "未分类"] += f["amount"]
    return sorted(sums.items(), key=lambda x: -x[1])
