# 个人财务系统 — 年度报告

from collections import defaultdict

from ..db import Database
from ..transactions import TransactionManager
from .common import (
    fetch_year_flows, money, pct, now_text, sum_dir, debt_total, by_category,
)
from .monthly import BOOK_LABELS

__all__ = ["generate_annual_report"]


def generate_annual_report(db: Database, year: int,
                           book: str = "personal") -> str:
    flows = fetch_year_flows(db, book, year)

    consumption = [f for f in flows if f["tx_type"] == "consumption"]
    # 退款按分类识别（账本不产生 tx_type='refund'）：单独冲减消费，不计收入
    refunds = [f for f in flows
               if f["direction"] == "inflow"
               and (f.get("category_name") or "") == "收入-退款"]
    income = [f for f in flows
              if f["tx_type"] == "income"
              and (f.get("category_name") or "") != "收入-退款"]
    fees = [f for f in flows if f["tx_type"] == "fee"]
    transfers = [f for f in flows if f["tx_type"] == "transfer"]

    spend = sum_dir(consumption, "outflow") - sum_dir(refunds, "inflow")
    income_total = sum_dir(income, "inflow")
    fee_total = sum_dir(fees, "outflow")
    net = income_total - spend - fee_total

    label = BOOK_LABELS.get(book, book)
    lines = [f"# {label}财务年报 — {year}年", ""]
    lines.append(f"> 生成：{now_text()} ｜ 共 {len(flows)} 条流水")
    lines.append("")

    lines += ["## 一、年度总览", "", "| 指标 | 数值 |", "|------|------|"]
    lines.append(f"| 总收入 | {money(income_total)} |")
    lines.append(f"| 总消费 | {money(spend)} |")
    if refunds:
        lines.append(f"| 退款 | {money(sum_dir(refunds, 'inflow'))}（已冲减消费） |")
    lines.append(f"| 手续费 | {money(fee_total)} |")
    lines.append(f"| 净结余 | {money(net)} |")
    lines.append(f"| 日均消费 | {money(spend / 365)} |")
    for tx_type, primary, title in (
        ("loan_disbursement", "inflow", "贷款放款"),
        ("loan_repayment", "outflow", "贷款还款"),
        ("credit_card_repayment", "inflow", "信用卡还款"),
    ):
        d = debt_total(flows, tx_type, primary)
        lines.append(f"| {title} | {money(d['total'])}（{d['count']} 笔） |")
    lines.append(f"| 转账 | 转出 {money(sum_dir(transfers, 'outflow'))} ／ "
                 f"转入 {money(sum_dir(transfers, 'inflow'))} |")
    lines.append("")

    monthly = defaultdict(lambda: {"inc": 0.0, "spend": 0.0, "n": 0})
    for f in flows:
        m = monthly[f["tx_date"][:7]]
        m["n"] += 1
        if (f["tx_type"] == "income" and f["direction"] == "inflow"
                and (f.get("category_name") or "") != "收入-退款"):
            m["inc"] += f["amount"]
        elif f["tx_type"] == "consumption" and f["direction"] == "outflow":
            m["spend"] += f["amount"]
        elif (f["direction"] == "inflow"
              and (f.get("category_name") or "") == "收入-退款"):
            m["spend"] -= f["amount"]
        elif f["tx_type"] == "fee" and f["direction"] == "outflow":
            m["spend"] += f["amount"]
    lines += ["## 二、月度走势", "",
              "| 月份 | 收入 | 消费 | 净额 | 笔数 |",
              "|------|------|------|------|------|"]
    for mon in sorted(monthly):
        d = monthly[mon]
        lines.append(f"| {mon} | {money(d['inc'])} | {money(d['spend'])} | "
                     f"{money(d['inc'] - d['spend'])} | {d['n']} |")
    lines.append("")

    cats = by_category(consumption, "outflow")
    if cats:
        lines += ["## 三、年度消费分类", "",
                  "| 分类 | 金额 | 占比 |", "|------|------|------|"]
        for name, amt in cats:
            lines.append(f"| {name} | {money(amt)} | {pct(amt, spend)} |")
        lines.append("")
        if refunds:
            lines.append("*退款已按总额冲减，分类排名未逐类扣减。*")
            lines.append("")

        lines += ["## 四、主要分类下钻", ""]
        skip = {"内部-转账", "未分类-其他", "金融-其他", "金融-手续费", "金融-贷款"}
        for cat_name, cat_total in [c for c in cats if c[0] not in skip][:6]:
            merchants = defaultdict(float)
            for f in consumption:
                if f["direction"] != "outflow":
                    continue
                if (f.get("category_name") or "未分类") != cat_name:
                    continue
                merchants[f.get("merchant") or "(未知)"] += f["amount"]
            lines.append(f"### {cat_name} — {money(cat_total)}")
            lines.append("")
            for name, amt in sorted(merchants.items(), key=lambda x: -x[1])[:5]:
                lines.append(f"- {name}：{money(amt)}")
            lines.append("")

    merchants = defaultdict(float)
    for f in consumption:
        if f["direction"] == "outflow" and f.get("merchant"):
            merchants[f["merchant"]] += f["amount"]
    if merchants:
        lines += ["## 五、年度商户 Top 20", "",
                  "| 排名 | 商户 | 金额 |", "|------|------|------|"]
        for i, (name, amt) in enumerate(
                sorted(merchants.items(), key=lambda x: -x[1])[:20], 1):
            lines.append(f"| {i} | {name} | {money(amt)} |")
        lines.append("")

    loans = defaultdict(lambda: {"new": 0.0, "repay": 0.0})
    for f in flows:
        if f["tx_type"] not in ("loan_disbursement", "loan_repayment"):
            continue
        # 只取贷款账户这一腿，避免与银行腿重复且平台名准确
        if f.get("account_type") != "loan":
            continue
        key = f.get("account_name") or "?"
        if f["tx_type"] == "loan_disbursement":
            loans[key]["new"] += f["amount"]
        else:
            loans[key]["repay"] += f["amount"]
    if loans:
        lines += ["## 六、贷款活动", "",
                  "| 平台 | 放款 | 还款 | 净变化 |",
                  "|------|------|------|------|"]
        for name, v in sorted(loans.items(), key=lambda x: -(x[1]["new"] + x[1]["repay"])):
            lines.append(f"| {name} | {money(v['new'])} | {money(v['repay'])} | "
                         f"{money(v['new'] - v['repay'])} |")
        lines.append("")
        lines.append("*净变化为正表示负债增加。*")
        lines.append("")

    tx_mgr = TransactionManager(db, book=book)
    tx_mgr.calculate_all_balances(year, 12, book=book)
    balances = db.fetchall(
        """SELECT ab.*, a.name, a.type FROM account_balances ab
           JOIN accounts a ON ab.account_id = a.id
           WHERE a.book=? AND ab.year=? AND ab.month=? AND a.is_active=1
             AND a.opening_date IS NOT NULL AND a.opening_date < ?
           ORDER BY a.type, a.id""",
        (book, year, 12, f"{year + 1:04d}-01-01"),
    )
    unanchored = db.fetchone(
        """SELECT COUNT(*) AS n FROM accounts
           WHERE book=? AND is_active=1 AND opening_date IS NULL""",
        (book,),
    )["n"]
    if balances:
        assets = sum(r["closing_balance"] for r in balances
                     if r["type"] not in ("credit_card", "loan"))
        liabilities = sum(r["closing_balance"] for r in balances
                          if r["type"] in ("credit_card", "loan"))
        lines += ["## 七、年末账户余额", "",
                  "| 账户 | 类型 | 期末 |", "|------|------|------|"]
        type_labels = {"bank": "储蓄卡", "payment": "支付平台",
                       "credit_card": "信用卡", "loan": "贷款",
                       "investment": "投资", "cash": "现金"}
        for r in balances:
            lines.append(f"| {r['name']} | {type_labels.get(r['type'], r['type'])} | "
                         f"{money(r['closing_balance'])} |")
        lines.append("")
        lines.append(f"**年末资产合计**：{money(assets)} ｜ "
                     f"**负债合计**：{money(liabilities)} ｜ "
                     f"**净资产**：{money(assets - liabilities)}")
        lines.append("")
        lines.append("*仅含已设余额锚点的账户（锚点来自余额/账单短信）。*")
        if unanchored:
            lines.append(f"*另有 {unanchored} 个未锚定账户未列入。*")
        lines.append("")

    lines += ["---", f"*生成时间：{now_text()} ｜ 数据来源：finance.db*", ""]
    return "\n".join(lines)
