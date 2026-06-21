# 月度收支报告（兼容现有 Markdown 格式 + 账户维度）

from collections import defaultdict
from datetime import datetime

from ..db import Database
from ..accounts import AccountManager
from ..transactions import TransactionManager

__all__ = ["generate_monthly_report"]


def generate_monthly_report(db: Database, year: int, month: int) -> str:
    """生成月度收支报告。"""
    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    flows = tx_mgr.get_flows(year=year, month=month, limit=5000)

    # 排除贷款流水（贷款单独统计）
    regular = [f for f in flows if f["tx_type"] not in ("loan_disbursement", "loan_repayment")]
    loans = [f for f in flows if f["tx_type"] in ("loan_disbursement", "loan_repayment")]

    total_income = sum(f["amount"] for f in regular if f["direction"] == "inflow")
    total_expense = sum(f["amount"] for f in regular if f["direction"] == "outflow")
    net = total_income - total_expense

    lines = []
    lines.append(f"# 月度收支报告 — {year}年{month}月")
    lines.append("")

    # 概要
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} |")
    lines.append(f"| 总支出 | ¥{total_expense:,.2f} |")
    lines.append(f"| 净结余 | ¥{net:,.2f} |")
    lines.append(f"| 交易笔数 | {len(regular)} 笔 |")
    if total_expense > 0:
        top = max(regular, key=lambda f: f["amount"] if f["direction"] == "outflow" else 0)
        if top:
            lines.append(f"| 最大单笔 | ¥{top['amount']:,.2f} ({top.get('merchant', '')}) |")
    lines.append("")

    # 按账户
    acct_expense = defaultdict(float)
    acct_income = defaultdict(float)
    for f in regular:
        name = f.get("account_name", "?")
        if f["direction"] == "inflow":
            acct_income[name] += f["amount"]
        else:
            acct_expense[name] += f["amount"]

    lines.append("## 按账户")
    lines.append("")
    lines.append("| 账户 | 收入 | 支出 | 净额 |")
    lines.append("|------|------|------|------|")
    for name in sorted(set(list(acct_income.keys()) + list(acct_expense.keys()))):
        inc = acct_income.get(name, 0)
        exp = acct_expense.get(name, 0)
        lines.append(f"| {name} | ¥{inc:,.2f} | ¥{exp:,.2f} | ¥{inc - exp:,.2f} |")
    lines.append("")

    # 按分类（支出）
    cat_expense = defaultdict(float)
    for f in regular:
        if f["direction"] == "outflow":
            cat = f.get("category_name") or "未分类"
            cat_expense[cat] += f["amount"]

    if cat_expense:
        lines.append("## 支出分类排行")
        lines.append("")
        sorted_cats = sorted(cat_expense.items(), key=lambda x: -x[1])
        lines.append("| 分类 | 金额 | 占比 |")
        lines.append("|------|------|------|")
        for cat, amt in sorted_cats:
            pct = amt / total_expense * 100 if total_expense > 0 else 0
            lines.append(f"| {cat} | ¥{amt:,.2f} | {pct:.1f}% |")
        lines.append("")

    # 商户排行
    merchant_expense = defaultdict(float)
    for f in regular:
        if f["direction"] == "outflow" and f.get("merchant"):
            merchant_expense[f["merchant"]] += f["amount"]

    if merchant_expense:
        lines.append("## 商户消费排行 Top 10")
        lines.append("")
        sorted_mchs = sorted(merchant_expense.items(), key=lambda x: -x[1])[:10]
        lines.append("| 排名 | 商户 | 金额 |")
        lines.append("|------|------|------|")
        for i, (mch, amt) in enumerate(sorted_mchs, 1):
            lines.append(f"| {i} | {mch} | ¥{amt:,.2f} |")
        lines.append("")

    # 贷款活动
    if loans:
        lines.append("## 贷款活动")
        lines.append("")
        loan_summary = defaultdict(lambda: {"in": 0.0, "out": 0.0})
        for f in loans:
            name = f.get("account_name", "?")
            if f["direction"] == "inflow":
                loan_summary[name]["in"] += f["amount"]
            else:
                loan_summary[name]["out"] += f["amount"]
        lines.append("| 平台 | 放款 | 还款 |")
        lines.append("|------|------|------|")
        for name, vals in sorted(loan_summary.items()):
            lines.append(f"| {name} | ¥{vals['out']:,.2f} | ¥{vals['in']:,.2f} |")
        lines.append("")

    lines.append("---")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：个人分类账*")
    lines.append("")

    return "\n".join(lines)
