# 个人财务系统 — 月度报告（消费口径排除转账/借贷/卡还款）

from collections import defaultdict

from ..db import Database
from ..accounts import AccountManager
from ..transactions import TransactionManager, month_bounds
from .common import (
    fetch_flows, money, pct, month_label, now_text, sum_dir, debt_total,
    by_category,
)

__all__ = ["generate_monthly_report", "BOOK_LABELS"]

BOOK_LABELS = {"personal": "个人", "work": "工作号"}


def _account_balances(db, book: str, year: int, month: int) -> tuple:
    """返回（已锚定账户本月余额, 未锚定账户数）。

    未设期初锚点的账户无法从流水推出真实余额，不进入报告。
    """
    tx_mgr = TransactionManager(db, book=book)
    tx_mgr.calculate_all_balances(year, month, book=book)
    next_month = (f"{year + 1:04d}-01-01" if month == 12
                  else f"{year:04d}-{month + 1:02d}-01")
    rows = db.fetchall(
        """SELECT ab.*, a.name, a.type, a.bank, a.card_suffix
           FROM account_balances ab JOIN accounts a ON ab.account_id = a.id
           WHERE a.book=? AND ab.year=? AND ab.month=? AND a.is_active=1
             AND a.opening_date IS NOT NULL AND a.opening_date < ?
           ORDER BY CASE a.type WHEN 'loan' THEN 3 WHEN 'credit_card' THEN 2
                                ELSE 1 END, a.id""",
        (book, year, month, next_month),
    )
    unanchored = db.fetchone(
        """SELECT COUNT(*) AS n FROM accounts
           WHERE book=? AND is_active=1 AND opening_date IS NULL""",
        (book,),
    )["n"]
    return rows, unanchored


def _import_rows(db, book: str, period: str) -> list:
    return db.fetchall(
        "SELECT source, status, row_count FROM import_batches"
        " WHERE book=? AND period=? ORDER BY source",
        (book, period),
    )


def generate_monthly_report(db: Database, year: int, month: int,
                            book: str = "personal") -> str:
    start, end = month_bounds(f"{year:04d}-{month:02d}")
    flows = fetch_flows(db, book, start, end)

    consumption = [f for f in flows if f["tx_type"] == "consumption"]
    income = [f for f in flows if f["tx_type"] == "income"]
    fees = [f for f in flows if f["tx_type"] == "fee"]
    refunds = [f for f in flows if f["tx_type"] == "refund"]
    transfers = [f for f in flows if f["tx_type"] == "transfer"]

    spend = sum_dir(consumption, "outflow") - sum_dir(refunds, "inflow")
    fee_total = sum_dir(fees, "outflow")
    income_total = sum_dir(income, "inflow")
    net = income_total - spend - fee_total
    loan_repay = debt_total(flows, "loan_repayment", "outflow")
    loan_new = debt_total(flows, "loan_disbursement", "inflow")
    cc_repay = debt_total(flows, "credit_card_repayment", "inflow")

    label = BOOK_LABELS.get(book, book)
    lines = [f"# {label}财务月报 — {month_label(year, month)}", ""]
    lines.append(f"> 数据源：{'短信 + 主号微信 + 通知' if book == 'personal' else '工作号微信'}"
                 f" ｜ 生成：{now_text()}")
    lines.append("")

    lines += ["## 一、本月概览", "", "| 指标 | 数值 |", "|------|------|"]
    lines.append(f"| 收入 | {money(income_total)} |")
    lines.append(f"| 消费 | {money(spend)} |")
    if refunds:
        lines.append(f"| 退款 | {money(sum_dir(refunds, 'inflow'))}（已冲减消费） |")
    lines.append(f"| 手续费 | {money(fee_total)} |")
    lines.append(f"| 净结余 | {money(net)} |")
    lines.append(f"| 转账 | 转出 {money(sum_dir(transfers, 'outflow'))} ／ "
                 f"转入 {money(sum_dir(transfers, 'inflow'))} |")
    lines.append(f"| 贷款还款 | {money(loan_repay['total'])}（{loan_repay['count']} 笔） |")
    lines.append(f"| 贷款放款 | {money(loan_new['total'])}（{loan_new['count']} 笔） |")
    lines.append(f"| 信用卡还款 | {money(cc_repay['total'])}（{cc_repay['count']} 笔） |")
    lines.append(f"| 流水笔数 | {len(flows)} 笔 |")
    lines.append("")

    cats = by_category(consumption, "outflow")
    if cats:
        lines += ["## 二、消费分类", "", "| 分类 | 金额 | 占比 |", "|------|------|------|"]
        for name, amt in cats:
            lines.append(f"| {name} | {money(amt)} | {pct(amt, spend)} |")
        lines.append("")

    merchants = defaultdict(float)
    for f in consumption:
        if f["direction"] == "outflow" and f.get("merchant"):
            merchants[f["merchant"]] += f["amount"]
    if merchants:
        lines += ["## 三、消费商户 Top 10", "",
                  "| 排名 | 商户 | 金额 |", "|------|------|------|"]
        for i, (name, amt) in enumerate(
                sorted(merchants.items(), key=lambda x: -x[1])[:10], 1):
            lines.append(f"| {i} | {name} | {money(amt)} |")
        lines.append("")

    inc_cats = by_category(income, "inflow")
    if inc_cats:
        lines += ["## 四、收入构成", "", "| 分类 | 金额 |", "|------|------|"]
        for name, amt in inc_cats:
            lines.append(f"| {name} | {money(amt)} |")
        lines.append("")

    lines += ["## 五、转账与借贷", ""]
    if transfers:
        counterparties = defaultdict(lambda: [0.0, 0.0])
        for f in transfers:
            key = f.get("merchant") or f.get("counterparty") or "(未知)"
            idx = 0 if f["direction"] == "outflow" else 1
            counterparties[key][idx] += f["amount"]
        lines += ["### 转账往来（Top 10 对手方）", "",
                  "| 对手方 | 转出 | 转入 |", "|------|------|------|"]
        for name, (out, inc) in sorted(
                counterparties.items(), key=lambda x: -(x[1][0] + x[1][1]))[:10]:
            lines.append(f"| {name} | {money(out)} | {money(inc)} |")
        lines.append("")

    loan_rows = defaultdict(lambda: {"repay": 0.0, "new": 0.0})
    for f in flows:
        if f["tx_type"] not in ("loan_repayment", "loan_disbursement"):
            continue
        # 只取贷款账户这一腿，平台名才准确且每笔只计一次
        if f.get("account_type") != "loan":
            continue
        key = f.get("account_name") or "?"
        if f["tx_type"] == "loan_repayment":
            loan_rows[key]["repay"] += f["amount"]
        else:
            loan_rows[key]["new"] += f["amount"]
    if loan_rows:
        lines += ["### 贷款活动", "", "| 平台 | 放款 | 还款 |",
                  "|------|------|------|"]
        for name, v in sorted(loan_rows.items()):
            lines.append(f"| {name} | {money(v['new'])} | {money(v['repay'])} |")
        lines.append("")

    cc_rows = defaultdict(lambda: {"in": 0.0, "out": 0.0})
    for f in flows:
        if f["tx_type"] != "credit_card_repayment":
            continue
        key = f.get("account_name") or "?"
        cc_rows[key]["in" if f["direction"] == "inflow" else "out"] += f["amount"]
    if cc_rows:
        lines += ["### 信用卡还款", "", "| 账户 | 还款入账 | 还款转出 |",
                  "|------|------|------|"]
        for name, v in sorted(cc_rows.items()):
            lines.append(f"| {name} | {money(v['in'])} | {money(v['out'])} |")
        lines.append("")

    balances, unanchored = _account_balances(db, book, year, month)
    assets = [r for r in balances if r["type"] not in ("credit_card", "loan")]
    liabilities = [r for r in balances if r["type"] in ("credit_card", "loan")]
    lines += ["## 六、账户余额", ""]
    if balances:
        lines += ["| 账户 | 期初 | 流入 | 流出 | 期末 |",
                  "|------|------|------|------|------|"]
        for r in assets + liabilities:
            lines.append(
                f"| {r['name']} | {money(r['opening_balance'])} | "
                f"{money(r['total_inflow'])} | {money(r['total_outflow'])} | "
                f"{money(r['closing_balance'])} |")
        lines.append("")
        lines.append("*仅列已设余额锚点的账户（锚点来自余额/账单短信）。*")
    else:
        lines.append("本月早于所有账户的余额锚点，暂无可用余额数据。")
    if unanchored:
        lines.append(f"另有 {unanchored} 个未锚定账户未列入（流水不足以推算真实余额）。")
    lines.append("")

    unmatched = sum(1 for f in flows if not f.get("account_id"))
    uncategorized = sum(
        1 for f in flows
        if f["tx_type"] == "consumption"
        and (f.get("category_name") or "未分类-其他") == "未分类-其他")
    lines += ["## 七、数据质量", ""]
    lines.append(f"- 未匹配账户：{unmatched} 笔")
    lines.append(f"- 未分类消费：{uncategorized} 笔")
    for r in _import_rows(db, book, f"{year:04d}-{month:02d}"):
        lines.append(f"- 导入 {r['source']}：{r['status']}，原始 {r['row_count']} 条")
    lines += ["", "---", f"*生成时间：{now_text()} ｜ 数据来源：finance.db*", ""]
    return "\n".join(lines)
