"""
年度财务报告生成器 — 生成综合 Markdown 报告并同步到 Joplin。
"""

import sys, os
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pathmagic
with pathmagic.context():
    from life.ledger.db import Database
    from life.ledger.accounts import AccountManager
    from life.ledger.transactions import TransactionManager


def generate_annual_report():
    db = Database()
    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    flows = tx_mgr.get_flows(limit=10000)
    active = [f for f in flows if f["tx_type"] not in ("loan_disbursement", "loan_repayment")]
    loans = [f for f in flows if f["tx_type"] in ("loan_disbursement", "loan_repayment")]

    lines = []
    lines.append("# 个人财务年度报告")
    lines.append("")
    lines.append(f"**报告周期**：2025年7月 — 2026年6月")
    lines.append(f"**生成时间**：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── 年度总览 ──
    total_inc = sum(f["amount"] for f in active if f["direction"] == "inflow")
    total_exp = sum(f["amount"] for f in active if f["direction"] == "outflow")
    net_total = total_inc - total_exp

    lines.append("## 一、年度总览")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 总收入 | ¥{total_inc:,.2f} |")
    lines.append(f"| 总支出 | ¥{total_exp:,.2f} |")
    lines.append(f"| 净结余 | ¥{net_total:,.2f} |")
    lines.append(f"| 总交易笔数 | {len(active)} 笔 |")
    if loans:
        loan_in = sum(f["amount"] for f in loans if f["direction"] == "outflow")  # 放款
        loan_out = sum(f["amount"] for f in loans if f["direction"] == "inflow")  # 还款
        lines.append(f"| 贷款放款 | ¥{loan_in:,.2f} |")
        lines.append(f"| 贷款还款 | ¥{loan_out:,.2f} |")
    lines.append("")

    # ── 月度走势 ──
    lines.append("## 二、月度收支走势")
    lines.append("")
    monthly = defaultdict(lambda: {"inc": 0.0, "exp": 0.0, "cnt": 0})
    for f in active:
        mon = f["tx_date"][:7]
        if f["direction"] == "inflow":
            monthly[mon]["inc"] += f["amount"]
        else:
            monthly[mon]["exp"] += f["amount"]
        monthly[mon]["cnt"] += 1

    lines.append("| 月份 | 收入 | 支出 | 净额 | 笔数 |")
    lines.append("|------|------|------|------|------|")
    for mon in sorted(monthly):
        d = monthly[mon]
        net = d["inc"] - d["exp"]
        lines.append(f"| {mon} | ¥{d['inc']:,.2f} | ¥{d['exp']:,.2f} | ¥{net:,.2f} | {d['cnt']} |")
    lines.append("")

    # ── 支出分类年度统计 ──
    lines.append("## 三、年度支出分类")
    lines.append("")
    cat_exp = defaultdict(float)
    cat_inc = defaultdict(float)
    for f in active:
        cat = f.get("category_name") or "未分类"
        if f["direction"] == "inflow":
            cat_inc[cat] += f["amount"]
        else:
            cat_exp[cat] += f["amount"]

    lines.append("### 支出 Top 15")
    lines.append("")
    lines.append("| 分类 | 金额 | 占比 |")
    lines.append("|------|------|------|")
    for cat, amt in sorted(cat_exp.items(), key=lambda x: -x[1])[:15]:
        pct = amt / total_exp * 100
        lines.append(f"| {cat} | ¥{amt:,.2f} | {pct:.1f}% |")
    lines.append("")

    lines.append("### 收入 Top 10")
    lines.append("")
    lines.append("| 分类 | 金额 |")
    lines.append("|------|------|")
    for cat, amt in sorted(cat_inc.items(), key=lambda x: -x[1])[:10]:
        lines.append(f"| {cat} | ¥{amt:,.2f} |")
    lines.append("")

    # ── 下钻：分类详情（排除内部-转账） ──
    lines.append("## 四、主要消费分类下钻")
    lines.append("")
    exclude_cats = ("内部-转账", "未分类-其他", "金融-其他", "金融-贷款", "金融-手续费")
    # 取余额最大的几个消费类
    consumer_cats = [(c, a) for c, a in cat_exp.items() if c not in exclude_cats]
    for cat_name, _ in sorted(consumer_cats, key=lambda x: -x[1])[:6]:
        cat_flows = [f for f in active if f["direction"] == "outflow" and f.get("category_name") == cat_name]
        cat_total = sum(f["amount"] for f in cat_flows)
        lines.append(f"### {cat_name} —— ¥{cat_total:,.2f}")
        lines.append("")
        # Top 商户
        merchants = defaultdict(float)
        for f in cat_flows:
            m = f.get("merchant") or "(未知)"
            merchants[m] += f["amount"]
        sorted_m = sorted(merchants.items(), key=lambda x: -x[1])[:5]
        for m, a in sorted_m:
            lines.append(f"- {m}: ¥{a:,.2f}")
        lines.append("")

    # ── 账户分析 ──
    lines.append("## 五、账户分析")
    lines.append("")
    acct_inc = defaultdict(float)
    acct_exp = defaultdict(float)
    for f in active:
        name = f.get("account_name") or "?"
        if f["direction"] == "inflow":
            acct_inc[name] += f["amount"]
        else:
            acct_exp[name] += f["amount"]

    all_accts = sorted(set(list(acct_inc.keys()) + list(acct_exp.keys())))
    lines.append("| 账户 | 收入 | 支出 | 净额 | 笔数 |")
    lines.append("|------|------|------|------|------|")
    for name in all_accts:
        inc = acct_inc.get(name, 0)
        exp = acct_exp.get(name, 0)
        cnt = sum(1 for f in active if f.get("account_name") == name)
        lines.append(f"| {name} | ¥{inc:,.2f} | ¥{exp:,.2f} | ¥{inc-exp:,.2f} | {cnt} |")
    lines.append("")

    # ── 贷款活动 ──
    if loans:
        lines.append("## 六、贷款活动")
        lines.append("")
        loan_totals = defaultdict(lambda: {"in": 0.0, "out": 0.0})
        for f in loans:
            name = f.get("account_name") or "?"
            if f["direction"] == "inflow":
                loan_totals[name]["in"] += f["amount"]  # 还款(负债减少)
            else:
                loan_totals[name]["out"] += f["amount"]  # 放款(负债增加)
        lines.append("| 平台 | 放款(负债增加) | 还款(负债减少) | 净变化 |")
        lines.append("|------|------|------|------|")
        for name, d in sorted(loan_totals.items()):
            net_change = d["out"] - d["in"]  # 正值 = 总负债增加
            lines.append(f"| {name} | ¥{d['out']:,.2f} | ¥{d['in']:,.2f} | ¥{net_change:,.2f} |")
        lines.append("")

    # ── 合规说明 ──
    lines.append("---")
    lines.append("*免责声明：本报告基于个人财务数据自动生成，仅供个人参考。*")
    lines.append("")
    lines.append("*数据来源：个人分类账 (happyjoplin/ledger)*")
    lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    report = generate_annual_report()
    print(report)
