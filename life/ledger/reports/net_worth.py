# 净资产报告

from collections import defaultdict
from datetime import datetime

from ..db import Database
from ..accounts import AccountManager
from ..transactions import TransactionManager

__all__ = ["generate_net_worth_report"]


def generate_net_worth_report(db: Database, date: str = None) -> str:
    """生成净资产报告 Markdown。"""
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)
    snapshot = tx_mgr.snapshot_net_worth(date)

    year, month = int(date[:4]), int(date[5:7])

    lines = []
    lines.append(f"# 净资产报告 — {year}年{month}月")
    lines.append("")

    # 总览
    lines.append("## 总览")
    lines.append("")
    lines.append(f"| 指标 | 数值 |")
    lines.append(f"|------|------|")
    lines.append(f"| 总资产 | ¥{snapshot['total_assets']:,.2f} |")
    lines.append(f"| 总负债 | ¥{snapshot['total_liabilities']:,.2f} |")
    lines.append(f"| **净资产** | **¥{snapshot['net_worth']:,.2f}** |")
    lines.append("")

    # 按类型分组
    by_type = defaultdict(list)
    for name, info in snapshot["details"].items():
        by_type[info["type"]].append((name, info))

    type_labels = {
        "bank_debit": "储蓄卡",
        "bank_credit": "信用卡（负债）",
        "wechat_wallet": "微信零钱",
        "alipay": "支付宝",
        "loan": "贷款（负债）",
        "cash": "现金",
    }
    type_order = ["bank_debit", "wechat_wallet", "alipay", "bank_credit", "loan", "cash"]

    lines.append("## 账户明细")
    lines.append("")
    lines.append("| 类型 | 账户 | 余额 | 状态 |")
    lines.append("|------|------|------|------|")
    for t in type_order:
        if t not in by_type:
            continue
        for name, info in by_type[t]:
            balance = info["balance"]
            is_liability = info.get("is_liability", False)
            bal_str = f"¥{balance:,.2f}" if not is_liability else f"(¥{balance:,.2f})"
            bal_display = f"**{bal_str}**" if t in ("bank_debit", "wechat_wallet") else bal_str
            label = type_labels.get(t, t)
            lines.append(f"| {label} | {name} | {bal_display} | {'📉 负债' if is_liability else '✓'} |")
    lines.append("")

    # 月度趋势
    lines.append("## 月度趋势")
    lines.append("")
    prev_snapshots = db.fetchall(
        "SELECT * FROM net_worth_snapshots WHERE snapshot_date <= ? ORDER BY snapshot_date DESC LIMIT 6",
        (date,),
    )
    if prev_snapshots:
        lines.append("| 月份 | 资产 | 负债 | 净资产 | 环比 |")
        lines.append("|------|------|------|--------|------|")
        prev_nw = None
        for snap in reversed(prev_snapshots):
            nw = snap["net_worth"]
            if prev_nw is not None and prev_nw > 0:
                pct = (nw - prev_nw) / prev_nw * 100
                trend = f"{pct:+.1f}%"
            else:
                trend = "—"
            lines.append(f"| {snap['snapshot_date']} | ¥{snap['total_assets']:,.2f} | ¥{snap['total_liabilities']:,.2f} | **¥{nw:,.2f}** | {trend} |")
            prev_nw = nw
        lines.append("")

    lines.append("---")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：微信聊天记录 + 手机短信 + 分类账*")
    lines.append("")

    return "\n".join(lines)
