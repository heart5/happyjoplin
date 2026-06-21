# 现金流量表

from collections import defaultdict
from datetime import datetime

from ..db import Database
from ..accounts import AccountManager
from ..transactions import TransactionManager

__all__ = ["generate_cash_flow_report"]


def generate_cash_flow_report(db: Database, year: int, month: int,
                              by_account: bool = False, by_category: bool = False) -> str:
    """生成现金流量表 Markdown。"""
    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    flows = tx_mgr.get_flows(year=year, month=month, limit=5000)

    # 按是否常规交易过滤贷款
    regular = [f for f in flows if f["tx_type"] not in ("loan_disbursement", "loan_repayment")]
    loans = [f for f in flows if f["tx_type"] in ("loan_disbursement", "loan_repayment")]

    total_income = sum(f["amount"] for f in regular if f["direction"] == "inflow")
    total_expense = sum(f["amount"] for f in regular if f["direction"] == "outflow")

    loan_in = sum(f["amount"] for f in loans if f["direction"] == "outflow")  # 负债增加
    loan_out = sum(f["amount"] for f in loans if f["direction"] == "inflow")  # 负债减少

    lines = []
    lines.append(f"# 现金流量表 — {year}年{month}月")
    lines.append("")

    # 概要
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} |")
    lines.append(f"| 总支出 | ¥{total_expense:,.2f} |")
    lines.append(f"| 净现金流 | ¥{total_income - total_expense:,.2f} |")
    lines.append(f"| 交易笔数 | {len(regular)} 笔 |")
    if loans:
        lines.append(f"| 贷款放款 | ¥{loan_in:,.2f} |")
        lines.append(f"| 贷款还款 | ¥{loan_out:,.2f} |")
    lines.append("")

    # 按分类
    if by_category or not by_account:
        cat_income = defaultdict(float)
        cat_expense = defaultdict(float)
        for f in regular:
            cat = f.get("category_name") or "未分类"
            if f["direction"] == "inflow":
                cat_income[cat] += f["amount"]
            else:
                cat_expense[cat] += f["amount"]

        lines.append("## 收入分类")
        lines.append("")
        sorted_inc = sorted(cat_income.items(), key=lambda x: -x[1])
        lines.append("| 分类 | 金额 | 占比 |")
        lines.append("|------|------|------|")
        for cat, amt in sorted_inc:
            pct = amt / total_income * 100 if total_income > 0 else 0
            lines.append(f"| {cat} | ¥{amt:,.2f} | {pct:.1f}% |")
        lines.append("")

        lines.append("## 支出分类")
        lines.append("")
        sorted_exp = sorted(cat_expense.items(), key=lambda x: -x[1])
        lines.append("| 分类 | 金额 | 占比 |")
        lines.append("|------|------|------|")
        for cat, amt in sorted_exp:
            pct = amt / total_expense * 100 if total_expense > 0 else 0
            lines.append(f"| {cat} | ¥{amt:,.2f} | {pct:.1f}% |")
        lines.append("")

    # 按账户
    if by_account:
        acct_income = defaultdict(float)
        acct_expense = defaultdict(float)
        for f in regular:
            name = f.get("account_name", "?")
            if f["direction"] == "inflow":
                acct_income[name] += f["amount"]
            else:
                acct_expense[name] += f["amount"]

        lines.append("## 按账户")
        lines.append("")
        all_accounts = sorted(set(list(acct_income.keys()) + list(acct_expense.keys())))
        lines.append("| 账户 | 收入 | 支出 | 净额 |")
        lines.append("|------|------|------|------|")
        for name in all_accounts:
            inc = acct_income.get(name, 0)
            exp = acct_expense.get(name, 0)
            net = inc - exp
            lines.append(f"| {name} | ¥{inc:,.2f} | ¥{exp:,.2f} | ¥{net:,.2f} |")
        lines.append("")

    # 贷款活动
    if loans:
        lines.append("## 贷款活动")
        lines.append("")
        loan_by_acct = defaultdict(lambda: {"in": 0.0, "out": 0.0})
        for f in loans:
            name = f.get("account_name", "?")
            if f["direction"] == "inflow":  # 还款（负债减少）
                loan_by_acct[name]["in"] += f["amount"]
            else:  # 放款（负债增加）
                loan_by_acct[name]["out"] += f["amount"]
        lines.append("| 账户 | 放款(负债增加) | 还款(负债减少) |")
        lines.append("|------|------|------|")
        for name, vals in sorted(loan_by_acct.items()):
            lines.append(f"| {name} | ¥{vals['out']:,.2f} | ¥{vals['in']:,.2f} |")
        lines.append("")

    lines.append("---")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：个人分类账*")
    lines.append("")

    return "\n".join(lines)
