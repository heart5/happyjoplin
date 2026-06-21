# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # 微信人际关系分析月报

# %%
"""从微信聊天记录分析人际关系：联系强度、响应时效、发起主动性、群聊活跃度。

数据源：
- 合并库 wcitemsall_merged.db — 全量聊天记录
- wcdelay 库 — 消息延时数据（可选，增强分析）
- wccontact 库 — 联系人变更追踪（可选）

用法：
    python life/wechat_relationship.py --account 白晔峰 --month 2026-06
    python life/wechat_relationship.py --account 白晔峰 --month prev
"""

import argparse
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pathmagic

with pathmagic.context():
    from func.logme import log

JOPLINAI = Path(__file__).resolve().parent.parent.parent / "joplinai"
if str(JOPLINAI) not in sys.path:
    sys.path.insert(0, str(JOPLINAI))

log = logging.getLogger("wechat_relationship")

__all__ = [
    "analyze_contacts",
    "classify_contacts",
    "detect_changes",
    "generate_relationship_report",
    "process_month",
]

# 群聊标记：sender 含 (群) 前缀
_RE_GROUP = re.compile(r"[（(].*?群.*?[）)]")


def _is_group(sender: str) -> bool:
    return bool(_RE_GROUP.search(sender))


def _clean_sender(sender: str) -> str:
    """去除群聊前缀，保留发送者名。"""
    return _RE_GROUP.sub("", sender).strip()


def _is_myself(sender: str, account: str) -> bool:
    """判断是否为自己发出的消息。

    sender 格式可能为：
    - "白晔峰"（个人聊天）
    - "91级二高群(群)白晔峰"（群聊中自己发出）
    """
    if sender == account:
        return True
    # 群聊格式：群名(群)账号名
    m = _RE_GROUP.search(sender)
    if m:
        after_group = _RE_GROUP.sub("", sender).strip()
        return after_group == account
    return False


def analyze_contacts(messages: list, account: str) -> dict:
    """分析所有联系人/群聊的月度互动数据。

    返回：{
        "contacts": {contact_name: {msg_count, active_days, sent_count, received_count, ...}},
        "groups": {group_name: {...}},
        "summary": {total_msgs, total_contacts, ...}
    }
    """
    contacts = defaultdict(lambda: {
        "msg_count": 0, "active_days": set(),
        "sent_count": 0, "received_count": 0,
        "last_time": "", "days_active": 0,
        "type_counts": defaultdict(int),
        "night_count": 0,  # 23:00-06:00
        "is_group": False,
        "group_name": "",
    })

    for msg in messages:
        sender = msg.get("sender", "")
        msg_time = msg.get("time", "")
        send = msg.get("send", False)
        msg_type = msg.get("type", "")
        content = msg.get("content", "")

        if not sender:
            continue

        is_group = _is_group(sender)

        if is_group:
            # 群聊：用群名作为 key
            group_match = _RE_GROUP.search(sender)
            group_name = group_match.group(0).strip("（）()") if group_match else sender
            key = sender  # 用完整 sender 区分不同群
            name = sender
            contacts[key]["is_group"] = True
            contacts[key]["group_name"] = group_name
        else:
            key = sender
            name = sender

        contacts[key]["msg_count"] += 1
        contacts[key]["type_counts"][msg_type] += 1

        if send:
            contacts[key]["sent_count"] += 1
        else:
            contacts[key]["received_count"] += 1

        day = msg_time[:10] if msg_time else "unknown"
        contacts[key]["active_days"].add(day)

        # 深夜消息
        if len(msg_time) >= 19:
            hour = int(msg_time[11:13])
            if hour >= 23 or hour < 6:
                contacts[key]["night_count"] += 1

        # 更新时间
        if msg_time and msg_time > contacts[key]["last_time"]:
            contacts[key]["last_time"] = msg_time

    # 汇总
    summary = {
        "total_msgs": sum(c["msg_count"] for c in contacts.values()),
        "total_contacts": len(contacts),
        "total_groups": sum(1 for c in contacts.values() if c["is_group"]),
        "total_personal": sum(1 for c in contacts.values() if not c["is_group"]),
    }

    # 计算衍生指标
    result = {}
    for name, data in contacts.items():
        days_active = len(data["active_days"])
        data["days_active"] = days_active
        data["daily_avg"] = round(data["msg_count"] / max(days_active, 1), 1)
        data["sent_ratio"] = round(
            data["sent_count"] / max(data["msg_count"], 1) * 100, 1
        )
        data["type_counts"] = dict(data["type_counts"])
        data["active_days"] = sorted(data["active_days"])
        result[name] = dict(data)

    return {"contacts": result, "summary": summary}


def _compute_trend(current: int, previous: int) -> str:
    if previous <= 0:
        return "—"
    pct = (current - previous) / previous * 100
    if pct > 30:
        return "↑↑"
    if pct > 10:
        return "↑"
    if pct < -30:
        return "↓↓"
    if pct < -10:
        return "↓"
    return "→"


def classify_contacts(contacts: dict) -> dict:
    """对联系人进行分类。

    返回 {contact_name: classification}
    分类：亲密圈, 日常圈, 事务圈, 沉寂联系人, 新联系人
    """
    result = {}
    for name, data in contacts.items():
        if data["is_group"]:
            continue
        msg_count = data["msg_count"]
        days_active = data["days_active"]
        sent_ratio = data["sent_ratio"]

        if days_active >= 20 and msg_count >= 100:
            result[name] = "亲密圈"
        elif days_active >= 8 and msg_count >= 30:
            result[name] = "日常圈"
        elif days_active >= 3:
            result[name] = "事务圈"
        elif msg_count > 0:
            result[name] = "沉寂联系人"
        else:
            result[name] = "事务圈"
    return result


def detect_changes(current_contacts: dict, prev_contacts: dict) -> list:
    """对比本月和上月的联系人数据，检测变化。"""
    changes = []

    # 新增
    for name in current_contacts:
        if name not in prev_contacts:
            changes.append({"type": "新增联系人", "contact": name, "detail": _clean_sender(name)})

    # 消失
    for name in prev_contacts:
        if name not in current_contacts:
            changes.append({"type": "沉寂联系", "contact": name, "detail": _clean_sender(name)})

    # 大幅变化
    for name in current_contacts:
        if name in prev_contacts:
            cur = current_contacts[name]
            prev = prev_contacts[name]
            diff_pct = (cur["msg_count"] - prev["msg_count"]) / max(prev["msg_count"], 1) * 100
            if abs(diff_pct) > 50 and cur["msg_count"] >= 20:
                direction = "大幅上升" if diff_pct > 0 else "大幅下降"
                changes.append({
                    "type": f"消息量{direction}",
                    "contact": name,
                    "detail": f"{prev['msg_count']}→{cur['msg_count']}条 ({diff_pct:+.0f}%)",
                })

    return changes


def generate_relationship_report(
    analysis: dict, prev_analysis: dict = None, changes: list = None
) -> str:
    """生成人际关系月报 Markdown。"""
    contacts = analysis["contacts"]
    summary = analysis["summary"]

    lines = []
    lines.append(f"# 人际关系月报 — {datetime.now().strftime('%Y年%m月')}")
    lines.append("")

    # 概要
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 总消息量 | {summary['total_msgs']:,} 条 |")
    lines.append(f"| 活跃联系人 | {summary['total_personal']} 人 |")
    lines.append(f"| 活跃群聊 | {summary['total_groups']} 个 |")
    lines.append("")

    # 联系人热度排行
    personal = {n: d for n, d in contacts.items() if not d["is_group"]}
    sorted_personal = sorted(personal.items(), key=lambda x: x[1]["msg_count"], reverse=True)[:20]

    lines.append("## 联系人热度排行 Top 20")
    lines.append("")
    lines.append("| 排名 | 联系人 | 消息数 | 趋势 | 我发出 | 活跃天数 | 日均 |")
    lines.append("|------|--------|--------|------|--------|---------|------|")
    prev_contacts = prev_analysis["contacts"] if prev_analysis else {}

    for i, (name, data) in enumerate(sorted_personal, 1):
        display_name = _clean_sender(name)[:20]
        prev_cnt = prev_contacts.get(name, {}).get("msg_count", 0)
        trend = _compute_trend(data["msg_count"], prev_cnt)
        lines.append(
            f"| {i} | {display_name} | {data['msg_count']} | {trend} "
            f"| {data['sent_count']} | {data['days_active']}天 | {data['daily_avg']} |"
        )
    lines.append("")

    # 群聊活跃度
    groups = {n: d for n, d in contacts.items() if d["is_group"]}
    if groups:
        sorted_groups = sorted(groups.items(), key=lambda x: x[1]["msg_count"], reverse=True)
        lines.append("## 群聊活跃度")
        lines.append("")
        lines.append("| 群名 | 消息量 | 我发言 | 占比 | 活跃天数 |")
        lines.append("|------|--------|--------|------|---------|")
        for name, data in sorted_groups[:10]:
            display = _clean_sender(name)[:25] or name[:25]
            lines.append(
                f"| {display} | {data['msg_count']} | {data['sent_count']} "
                f"| {data['sent_ratio']}% | {data['days_active']}天 |"
            )
        lines.append("")

    # 消息类型分布（总体）
    all_types = defaultdict(int)
    for data in contacts.values():
        for t, c in data["type_counts"].items():
            all_types[t] += c
    if all_types:
        lines.append("## 消息类型分布")
        lines.append("")
        sorted_types = sorted(all_types.items(), key=lambda x: x[1], reverse=True)
        total = sum(c for _, c in sorted_types)
        lines.append("| 类型 | 数量 | 占比 |")
        lines.append("|------|------|------|")
        for t, c in sorted_types:
            lines.append(f"| {t} | {c:,} | {c/total*100:.1f}% |")
        lines.append("")

    # 联系人变更
    if changes:
        lines.append("## 联系人变化检测")
        lines.append("")
        lines.append("| 类型 | 联系人 | 详情 |")
        lines.append("|------|--------|------|")
        for ch in changes[:15]:
            lines.append(f"| {ch['type']} | {_clean_sender(ch['contact'])[:20]} | {ch['detail']} |")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：微信聊天记录合并库*")
    lines.append("")

    return "\n".join(lines)


def process_month(account: str, year: int, month: int, client=None) -> str:
    """处理指定月份，返回人际关系 Markdown 报告。"""
    if client is None:
        from aimod.wechat_client import WeChatClient
        client = WeChatClient(mode="local", db_path=str(JOPLINAI / "data" / "wcitemsall_merged.db"))

    date_from = f"{year}-{month:02d}-01"
    if month == 12:
        date_to = f"{year + 1}-01-01"
    else:
        date_to = f"{year}-{month + 1:02d}-01"

    all_msgs = client.query(account, date_from=date_from, date_to=date_to, limit=50000)
    log.info(f"{year}-{month:02d}: 拉取 {len(all_msgs)} 条消息")

    analysis = analyze_contacts(all_msgs, account)

    # 加载上月数据做对比
    prev_analysis = None
    prev_msgs = []
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    try:
        prev_from = f"{prev_year}-{prev_month:02d}-01"
        prev_to = f"{prev_year}-{prev_month + 1:02d}-01" if prev_month < 12 else f"{prev_year + 1}-01-01"
        prev_msgs = client.query(account, date_from=prev_from, date_to=prev_to, limit=50000)
        prev_analysis = analyze_contacts(prev_msgs, account)
    except Exception as e:
        log.warning(f"加载上月数据失败: {e}")

    # 联系人变化检测
    changes = []
    if prev_analysis:
        changes = detect_changes(
            analysis["contacts"], prev_analysis["contacts"]
        )

    report = generate_relationship_report(analysis, prev_analysis, changes)
    return report


def main():
    parser = argparse.ArgumentParser(description="人际关系月报生成")
    parser.add_argument("--account", default="白晔峰")
    parser.add_argument("--month", help="月份 YYYY-MM 或 prev")
    parser.add_argument("--output", "-o", help="输出到文件")
    args = parser.parse_args()

    now = datetime.now()
    if args.month == "prev":
        first = now.replace(day=1) - timedelta(days=1)
        year, month = first.year, first.month
    elif args.month:
        parts = args.month.split("-")
        year, month = int(parts[0]), int(parts[1])
    else:
        year, month = now.year, now.month

    log.info(f"开始处理 {year}-{month:02d} 人际关系月报")
    report = process_month(args.account, year, month)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"报告已保存: {args.output}")
    else:
        print(report)

    log.info(f"人际关系月报 {year}-{month:02d} 完成")


if __name__ == "__main__":
    main()
