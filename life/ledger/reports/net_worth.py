# 个人财务系统 — 净资产报告

from ..db import Database
from ..transactions import TransactionManager
from .common import money, now_text

__all__ = ["generate_net_worth_report"]

_TYPE_LABELS = {
    "bank": "储蓄卡", "payment": "支付平台", "credit_card": "信用卡",
    "loan": "贷款", "investment": "投资", "cash": "现金",
}


def generate_net_worth_report(db: Database, date_str: str,
                              book: str = "combined") -> str:
    tx_mgr = TransactionManager(db, book="personal")
    snap = tx_mgr.snapshot_net_worth(date_str, book=book, anchored_only=True)

    title_book = {"combined": "全部账套", "personal": "个人", "work": "工作号"}[book]
    lines = [f"# 净资产报告 — {date_str}", ""]
    lines.append(f"> 口径：{title_book} ｜ 生成：{now_text()}")
    lines.append("")
    included = snap['included_accounts']
    skipped = snap['skipped_accounts']
    total = included + skipped
    coverage = included / total if total else 0.0
    lines.append(f"> 仅统计已设余额锚点的账户：纳入 {included} 个，"
                 f"未纳入 {skipped} 个（覆盖率 {coverage:.0%}）。"
                 f"未锚定账户的流水不足以推算真实余额，不参与合计。")
    lines.append("")
    # 覆盖率过低时，总资产/净资产数字没有引用价值，明确警示而非静默给出结论
    if coverage < 0.8:
        lines.append(f"> ⚠️ **数据不完整**：当前仅 {coverage:.0%} 的账户设了余额锚点，"
                     f"下列「总资产 / 净资产」不能代表真实财务状况。"
                     f"请先用 `accounts anchor` 为主力储蓄卡/支付钱包补锚点。")
        lines.append("")

    lines += ["## 一、总览", "", "| 指标 | 数值 |", "|------|------|"]
    lines.append(f"| 总资产 | {money(snap['total_assets'])} |")
    lines.append(f"| 总负债 | {money(snap['total_liabilities'])} |")
    nw_label = "净资产（已锚定账户）" if coverage >= 0.8 else "净资产（已锚定账户·不可引用）"
    lines.append(f"| **{nw_label}** | **{money(snap['net_worth'])}** |")
    lines.append("")

    details = snap["details"]
    assets = {k: v for k, v in details.items() if not v["is_liability"]}
    liabilities = {k: v for k, v in details.items() if v["is_liability"]}

    lines += ["## 二、资产", "", "| 账户 | 账套 | 类型 | 余额 |",
              "|------|------|------|------|"]
    for name, v in sorted(assets.items(), key=lambda x: -x[1]["balance"]):
        lines.append(f"| {name} | {v['book']} | "
                     f"{_TYPE_LABELS.get(v['type'], v['type'])} | "
                     f"{money(v['balance'])} |")
    lines.append("")

    lines += ["## 三、负债", "", "| 账户 | 账套 | 类型 | 余额 |",
              "|------|------|------|------|"]
    for name, v in sorted(liabilities.items(), key=lambda x: -x[1]["balance"]):
        lines.append(f"| {name} | {v['book']} | "
                     f"{_TYPE_LABELS.get(v['type'], v['type'])} | "
                     f"{money(v['balance'])} |")
    lines.append("")

    trend = db.fetchall(
        """SELECT snapshot_date, total_assets, total_liabilities, net_worth
           FROM net_worth_snapshots WHERE book=?
             AND total_assets >= 0 AND total_liabilities >= 0
           ORDER BY snapshot_date DESC LIMIT 12""",
        (book,),
    )
    if trend:
        lines += ["## 四、历史趋势", "",
                  "| 日期 | 总资产 | 总负债 | 净资产 |",
                  "|------|------|------|------|"]
        for r in reversed(trend):
            lines.append(f"| {r['snapshot_date']} | {money(r['total_assets'])} | "
                         f"{money(r['total_liabilities'])} | {money(r['net_worth'])} |")
        lines.append("")

    lines += ["---",
              "*余额由锚点余额/账单金额起按流水滚算；信用卡以账单口径为准，"
              "可能不含未出账单消费。*",
              f"*生成时间：{now_text()}*", ""]
    return "\n".join(lines)
