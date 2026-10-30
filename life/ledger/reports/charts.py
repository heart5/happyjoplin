# 个人财务系统 — 月报图表（Agg + 中文字体，输出 PNG）

from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

from ..db import Database
from .common import fetch_flows, month_label

__all__ = ["build_monthly_charts", "CHART_LABELS"]

CHART_LABELS = ("cat_donut", "merchant_bar", "daily_spend",
                "month_trend", "debt")
FONT_FAMILY = "Noto Sans CJK JP"
PALETTE = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3",
           "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"]


def _setup() -> None:
    families = {item.name for item in font_manager.fontManager.ttflist}
    family = next((name for name in (
        "Noto Sans CJK SC", "Noto Sans CJK JP", "WenQuanYi Micro Hei", "SimHei"
    ) if name in families), FONT_FAMILY)
    plt.rcParams["font.family"] = family
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.dpi"] = 110


def _save(fig, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, format="png", bbox_inches="tight")
    plt.close(fig)
    return path


def category_donut(cats: list, path, title: str = "消费分类") -> Path:
    """cats: [(name, amount)] 已按金额降序。"""
    _setup()
    names = [name for name, _ in cats]
    amounts = [amt for _, amt in cats]
    fig, ax = plt.subplots(figsize=(6.4, 4.6))
    colors = [PALETTE[i % len(PALETTE)] for i in range(len(names))]
    wedges, _texts = ax.pie(
        amounts, colors=colors, startangle=90, counterclock=False,
        wedgeprops={"width": 0.42, "edgecolor": "white"})
    total = sum(amounts)
    ax.text(0, 0, f"合计\n¥{total:,.0f}", ha="center", va="center", fontsize=11)
    ax.legend(wedges, [f"{n} ¥{a:,.0f}" for n, a in zip(names, amounts)],
              loc="center left", bbox_to_anchor=(1.0, 0.5), fontsize=8,
              frameon=False)
    ax.set_title(title)
    return _save(fig, path)


def merchant_bar(merchant_rows: list, path, title: str = "消费商户 Top 10") -> Path:
    """merchant_rows: [(merchant, amount)] 已按金额降序、取前 10。"""
    _setup()
    rows = merchant_rows[:10][::-1]
    names = [name for name, _ in rows]
    amounts = [amt for _, amt in rows]
    fig, ax = plt.subplots(figsize=(7.2, max(2.6, 0.42 * len(rows) + 1.2)))
    ax.barh(names, amounts, color=PALETTE[0], height=0.62)
    for y, amt in enumerate(amounts):
        ax.text(amt, y, f" ¥{amt:,.0f}", va="center", fontsize=8)
    ax.set_xlabel("金额（元）")
    ax.grid(axis="x", alpha=0.3)
    ax.set_title(title)
    return _save(fig, path)


def daily_spend(day_totals: dict, path, year: int, month: int,
                title: str = "每日消费") -> Path:
    """day_totals: {date: amount}，按天铺满整月。"""
    _setup()
    days = list(range(1, calendar.monthrange(year, month)[1] + 1))
    values = [day_totals.get(date(year, month, d), 0.0) for d in days]
    fig, ax = plt.subplots(figsize=(9.6, 3.0))
    ax.bar(days, values, color=PALETTE[0], width=0.8)
    if values and max(values) > 0:
        peak = max(range(len(values)), key=lambda i: values[i])
        ax.annotate(f"¥{values[peak]:,.0f}", (days[peak], values[peak]),
                    textcoords="offset points", xytext=(0, 4),
                    ha="center", fontsize=8, color=PALETTE[3])
    ax.set_xlabel("日")
    ax.set_ylabel("消费（元）")
    ax.set_xticks([d for d in days if d % 5 == 0] or [1])
    ax.grid(axis="y", alpha=0.3)
    ax.set_title(title)
    return _save(fig, path)


def month_trend(rows: list, path, title: str = "近 12 个月收支趋势") -> Path:
    """rows: [{"month": "YYYY-MM", "income": x, "spend": y, "net": z}] 升序。"""
    _setup()
    labels = [r["month"][2:].replace("-", "/") for r in rows]
    income = [r["income"] for r in rows]
    spend = [r["spend"] for r in rows]
    net = [r["net"] for r in rows]
    fig, ax = plt.subplots(figsize=(9.6, 3.4))
    idx = list(range(len(rows)))
    width = 0.38
    ax.bar([i - width / 2 for i in idx], income, width,
           color=PALETTE[2], label="收入")
    ax.bar([i + width / 2 for i in idx], spend, width,
           color=PALETTE[3], label="消费")
    ax.plot(idx, net, color=PALETTE[0], marker="o", markersize=3, label="净结余")
    ax.axhline(0, color="#888", linewidth=0.8)
    ax.set_xticks(idx)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("金额（元）")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="upper left", fontsize=8, ncol=3)
    ax.set_title(title)
    return _save(fig, path)


def debt_bars(rows: list, path, title: str = "负债账户余额（期末）") -> Path:
    """rows: [{"name": ..., "closing": x}]。"""
    _setup()
    rows = sorted(rows, key=lambda r: -abs(r["closing"]))
    names = [r["name"] for r in rows]
    values = [abs(r["closing"]) for r in rows]
    fig, ax = plt.subplots(figsize=(7.2, max(2.4, 0.5 * len(rows) + 1.2)))
    ax.barh(names[::-1], values[::-1], color=PALETTE[3], height=0.6)
    for y, amt in enumerate(values[::-1]):
        ax.text(amt, y, f" ¥{amt:,.0f}", va="center", fontsize=8)
    ax.set_xlabel("欠款余额（元）")
    ax.grid(axis="x", alpha=0.3)
    ax.set_title(title)
    return _save(fig, path)


# ── 数据准备 ──


def _consumption(flows: list) -> list:
    return [f for f in flows if f["tx_type"] == "consumption"]


def _category_totals(flows: list) -> list:
    sums = defaultdict(float)
    for f in _consumption(flows):
        if f["direction"] == "outflow":
            sums[f.get("category_name") or "未分类"] += f["amount"]
    return sorted(sums.items(), key=lambda x: -x[1])


def _merchant_totals(flows: list) -> list:
    sums = defaultdict(float)
    for f in _consumption(flows):
        if f["direction"] == "outflow" and (f.get("merchant") or "").strip():
            sums[f["merchant"]] += f["amount"]
    return sorted(sums.items(), key=lambda x: -x[1])


def _month_summary(flows: list) -> dict:
    spend = sum(f["amount"] for f in flows
                if f["tx_type"] == "consumption" and f["direction"] == "outflow")
    refund = sum(f["amount"] for f in flows
                 if f["direction"] == "inflow"
                 and (f.get("category_name") or "") == "收入-退款")
    income = sum(f["amount"] for f in flows
                 if f["tx_type"] == "income" and f["direction"] == "inflow"
                 and (f.get("category_name") or "") != "收入-退款")
    fees = sum(f["amount"] for f in flows
               if f["tx_type"] == "fee" and f["direction"] == "outflow")
    return {"spend": spend - refund, "income": income,
            "net": income - (spend - refund) - fees}


def _trailing_months(year: int, month: int, count: int = 12) -> list:
    months = []
    y, m = year, month
    for _ in range(count):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return list(reversed(months))


def build_monthly_charts(db: Database, book: str, year: int, month: int,
                         out_dir: str | Path) -> dict:
    """生成月报图表，返回 {label: path}；无数据的图不生成。"""
    from ..transactions import month_bounds

    out_dir = Path(out_dir)
    start, end = month_bounds(f"{year:04d}-{month:02d}")
    flows = fetch_flows(db, book, start, end)
    label = month_label(year, month)
    charts = {}

    cats = _category_totals(flows)
    if cats:
        charts["cat_donut"] = str(category_donut(
            cats, out_dir / f"cat_donut_{book}_{year}{month:02d}.png",
            f"{label} 消费分类"))

    merchants = _merchant_totals(flows)
    if merchants:
        charts["merchant_bar"] = str(merchant_bar(
            merchants, out_dir / f"merchant_bar_{book}_{year}{month:02d}.png",
            f"{label} 消费商户 Top 10"))

    day_totals = defaultdict(float)
    for f in flows:
        if f["tx_type"] == "consumption" and f["direction"] == "outflow":
            day_totals[date.fromisoformat(f["tx_date"])] += f["amount"]
    charts["daily_spend"] = str(daily_spend(
        day_totals, out_dir / f"daily_spend_{book}_{year}{month:02d}.png",
        year, month, title=f"{label} 每日消费"))

    months = _trailing_months(year, month)
    window_flows = fetch_flows(db, book, f"{months[0]}-01", end)
    grouped = defaultdict(list)
    for f in window_flows:
        grouped[f["tx_date"][:7]].append(f)
    rows = [{"month": key, **_month_summary(grouped.get(key, []))}
            for key in months]
    charts["month_trend"] = str(month_trend(
        rows, out_dir / f"month_trend_{book}_{year}{month:02d}.png"))

    debt = db.fetchall(
        """SELECT a.name, ab.closing_balance
           FROM account_balances ab JOIN accounts a ON a.id = ab.account_id
           WHERE a.book=? AND ab.year=? AND ab.month=?
             AND a.type IN ('credit_card','loan')
             AND ab.closing_balance < -0.005
           ORDER BY ab.closing_balance""",
        (book, year, month),
    )
    if debt:
        charts["debt"] = str(debt_bars(
            [{"name": r["name"], "closing": r["closing_balance"]}
             for r in debt],
            out_dir / f"debt_{book}_{year}{month:02d}.png"))

    return charts
