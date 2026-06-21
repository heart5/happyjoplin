# 账户余额跟踪报告

from datetime import datetime

from ..db import Database
from ..accounts import AccountManager
from ..transactions import TransactionManager

__all__ = ["generate_balance_report"]


def generate_balance_report(db: Database, year: int, month: int,
                            account_id: int = None) -> str:
    """生成账户余额跟踪报告。"""
    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    accounts = acct_mgr.list_accounts()
    if account_id:
        accounts = [a for a in accounts if a.id == account_id]

    lines = []
    lines.append(f"# 账户余额跟踪 — {year}年{month}月")
    lines.append("")

    for acct in accounts:
        history = tx_mgr.list_balance_history(acct.id, months=12)
        if not history:
            continue

        lines.append(f"## {acct.name}")
        lines.append("")
        lines.append(f"类型：{acct.type}")
        if acct.bank:
            lines.append(f"  \n银行：{acct.bank} 尾号{acct.card_suffix}")
        lines.append("")

        is_liability = acct.type in ("bank_credit", "loan")
        inc_label = "流入" if not is_liability else "还款(负债↓)"
        out_label = "流出" if not is_liability else "消费/放款(负债↑)"

        lines.append(f"| 月份 | 期初 | {inc_label} | {out_label} | 期末 | 状态 |")
        lines.append(f"|------|------|--------|--------|------|------|")
        for h in reversed(history):
            status = "估算" if h.is_estimated else "已校准"
            lines.append(
                f"| {h.year}-{h.month:02d} | ¥{h.opening_balance:,.2f} "
                f"| ¥{h.total_inflow:,.2f} | ¥{h.total_outflow:,.2f} "
                f"| ¥{h.closing_balance:,.2f} | {status} |"
            )
        lines.append("")

    lines.append("---")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*标记为「估算」的余额尚未手动校准*")
    lines.append("")

    return "\n".join(lines)
