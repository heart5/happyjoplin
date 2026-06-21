# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     cell_metadata_filter: -all
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # SMS 短信财务解析模块
#
# 读取 HCX 端 sms_received.db，解析银行短信为统一财务记录格式，
# 支持与微信记录去重合并，生成综合月报。

# %%
"""
SMS 短信财务解析模块。

从 sms_received.db 读取原始短信，解析银行通知提取金额/卡号/商户，
输出与 wechat_finance 兼容的记录格式，支持跨源去重。

用法：
    python life/sms_finance.py --month 2026-06              # 只看短信
    python life/sms_finance.py --month 2026-06 --wechat     # 含微信去重
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pathmagic

JOPLINAI = Path(__file__).resolve().parent.parent.parent / "joplinai"
if str(JOPLINAI) not in sys.path:
    sys.path.insert(0, str(JOPLINAI))

with pathmagic.context():
    from func.logme import log

SMS_DB_CANDIDATES = [str(JOPLINAI / "data" / "sms_received.db"), str(Path.home() / "work" / "joplinai" / "data" / "sms_received.db")]

def _find_sms_db():
    for p in SMS_DB_CANDIDATES:
        if Path(p).exists():
            return p
    return SMS_DB_CANDIDATES[1]

SMS_DB = _find_sms_db()

log = logging.getLogger("sms_finance")

__all__ = [
    "parse_sms_messages",
    "dedup_against_wechat",
    "generate_sms_report",
    "process_sms_month",
]

# ── 银行短号映射 ──

BANK_SHORT_CODES = {
    "95555": "招商银行",
    "95588": "中国工商银行",
    "95599": "中国农业银行",
    "95566": "中国银行",
    "95559": "交通银行",
    "95533": "中国建设银行",
    "95508": "广发银行",
    "95595": "中国光大银行",
    "95568": "中国民生银行",
    "95528": "上海浦东发展银行",
    "95558": "中信银行",
    "95577": "华夏银行",
    "95561": "兴业银行",
    "95580": "中国邮政储蓄银行",
    "95511": "平安银行",
    "95522": "泰康保险",
    "106980095533": "中国建设银行",
    "106980095508": "广发银行",
    "106941995555": "招商银行",
    "10693495555": "招商银行",
    "1069058425850": "交通银行",
    "106911990095580": "广发银行",
}

# ── 正则模式 ──

# 卡号尾号
RE_CARD_SUFFIX = re.compile(r"尾[号账](\d{4})")

# 金额提取（银行通知通用）
RE_AMOUNT_CNY = re.compile(r"[人民币￥¥](\d+\.?\d*)")

# 消费/支出类关键词
_RE_EXPENSE_KW = ["消费", "支出", "转出", "支付", "快捷", "POS", "pos"]

# 收入/入账类关键词
_RE_INCOME_KW = ["收入", "入账", "存入", "转入", "转入资金", "银联入账", "汇款"]

# ── 银行专用模式 ──


def _detect_bank(number: str, body: str) -> str:
    """根据发件人号码和正文识别银行名。"""
    if number in BANK_SHORT_CODES:
        return BANK_SHORT_CODES[number]

    for prefix, name in sorted(BANK_SHORT_CODES.items(), key=lambda x: -len(x[0])):
        if number.startswith(prefix):
            return name

    m = re.search(r"[【](.+?)[】]", body)
    if m:
        name = m.group(1).strip()
        name = re.sub(r"(尊敬的|您好).*", "", name).strip()
        if name and len(name) < 20:
            return name

    return number


def _parse_sms_amount(body: str, bank: str) -> float:
    """从银行短信正文提取金额。"""
    if "招商银行" in bank or "招商" in bank:
        m = re.search(r"人民币(\d+\.?\d*)", body)
        if m:
            return float(m.group(1))

    if "交通银行" in bank:
        m = re.search(r"(?:转入资金|转出|消费|支付)(\d+\.?\d*)元", body)
        if m:
            return float(m.group(1))

    if "建设银行" in bank:
        m = re.search(r"(?:存入|消费|支付|支出)(\d+\.?\d*)元", body)
        if m:
            return float(m.group(1))

    if "农业银行" in bank or "农行" in bank:
        m = re.search(r"交易人民币(\d+\.?\d*)", body)
        if m:
            return float(m.group(1))

    if "光大银行" in bank:
        m = re.search(r"交易(\d+\.?\d*)", body)
        if m:
            return float(m.group(1))

    m = RE_AMOUNT_CNY.search(body)
    if m:
        return float(m.group(1))

    m = re.search(r"(\d+\.?\d*)元", body)
    if m:
        return float(m.group(1))

    return 0.0


def _detect_direction(body: str, bank: str) -> str:
    """判断交易方向。"""
    for kw in _RE_INCOME_KW:
        if kw in body:
            return "收入"
    for kw in _RE_EXPENSE_KW:
        if kw in body:
            return "支出"
    if "转至他行" in body:
        return "支出"
    if "转入资金" in body:
        return "收入"
    return "支出"


def _extract_merchant(body: str, bank: str) -> str:
    """从银行短信提取商户名。"""
    patterns = [
        (r"在(.+?)(?:消费|交易|支付)", 1),
        (r"交易商户[：:]\s*(.+)", 1),
        (r"消费[￥¥]?[\d.]+元[，,]\s*(.+)", 1),
        (r"向\s*(.+?)\s*转账", 1),
        (r"收款[人方](.+?)(?:\s|$)", 1),
        (r"对方[：:户名]?\s*(.+?)(?:\s|$)", 1),
        (r"-([一-鿿].+?)$", 1),
    ]
    for pat, group in patterns:
        m = re.search(pat, body)
        if m:
            merchant = m.group(group).strip().rstrip("。，")
            if len(merchant) > 1 and not re.match(r"^\d+$", merchant):
                return merchant
    return ""


def _extract_card_suffix(body: str) -> str:
    """从短信正文提取卡号尾号。"""
    m = re.search(r"尾[号账](\d{4})", body)
    if m:
        return m.group(1)
    m = re.search(r"账户(\d{4})", body)
    if m:
        return m.group(1)
    m = re.search(r"账户\*(\d{4})", body)
    if m:
        return m.group(1)
    return ""


def _parse_sms_record(msg: dict) -> dict:
    """将单条短信解析为财务记录。"""
    body = str(msg.get("body", ""))
    number = str(msg.get("number", ""))
    received = str(msg.get("received", ""))

    bank = _detect_bank(number, body)
    amount = _parse_sms_amount(body, bank)
    direction = _detect_direction(body, bank)
    merchant = _extract_merchant(body, bank) or bank
    card_suffix = _extract_card_suffix(body)

    return {
        "time": received,
        "amount": amount,
        "merchant": merchant,
        "direction": direction,
        "category": "未分类-其他",
        "payment_method": bank,
        "card_suffix": card_suffix,
        "source": "sms",
        "source_text": body[:120],
        "raw": {"number": number, "sms_id": msg.get("sms_id", "")},
    }


# ── 解析入口 ──


def parse_sms_messages(sms_list: list) -> list:
    """解析短信列表，返回财务记录。"""
    records = []
    for msg in sms_list:
        try:
            rec = _parse_sms_record(msg)
            if rec["amount"] <= 0:
                continue
            records.append(rec)
        except Exception as e:
            log.warning(f"解析短信失败: {e}")
            continue
    records.sort(key=lambda r: r.get("time") or "")
    return records


# ── 跨源去重 ──


def dedup_against_wechat(sms_records: list, wechat_records: list) -> dict:
    """短信记录 vs 微信记录：双向去重。

    返回：
    {
        "matched": [...],
        "sms_only": [...],
        "wechat_only": [...],
        "sms_total": N,
        "deduped": N,
    }
    """
    if not wechat_records:
        return {
            "matched": [],
            "sms_only": sms_records,
            "wechat_only": [],
            "sms_total": len(sms_records),
            "deduped": 0,
        }

    wx_index = defaultdict(list)
    for r in wechat_records:
        day = r.get("time", "")[:10]
        amt = round(r["amount"], 2)
        card = r.get("card_suffix", "")
        wx_index[(day, amt, card)].append(r)
        wx_index[(day, amt, "")].append(r)

    matched = []
    sms_only = []
    used_wx = set()

    for sms in sms_records:
        day = sms.get("time", "")[:10]
        amt = round(sms["amount"], 2)
        card = sms.get("card_suffix", "")
        candidates = wx_index.get((day, amt, card), [])

        found = False
        for i, wx in enumerate(candidates):
            if id(wx) not in used_wx:
                matched.append(wx)
                used_wx.add(id(wx))
                found = True; break

        if not found and not card:
            candidates = wx_index.get((day, amt, ""), [])
            for i, wx in enumerate(candidates):
                if id(wx) not in used_wx:
                    matched.append(wx)
                    used_wx.add(id(wx))
                    found = True
                    break

        if not found:
            sms_only.append(sms)

    wechat_only = [r for r in wechat_records if id(r) not in used_wx]

    return {
        "matched": matched,
        "sms_only": sms_only,
        "wechat_only": wechat_only,
        "sms_total": len(sms_records),
        "deduped": len(matched),
    }


# ── 报告生成 ──


def generate_sms_report(
    records: list, year: int, month: int,
    title: str = "短信财务月报",
    source_label: str = "手机短信",
) -> str:
    """生成月度短信财务报告。"""
    cat_totals = defaultdict(lambda: {"amount": 0.0, "count": 0})
    daily_expense = defaultdict(float)
    daily_income = defaultdict(float)
    total_expense = 0.0
    total_income = 0.0
    by_bank = defaultdict(lambda: {"expense": 0.0, "income": 0.0, "count": 0})

    for r in records:
        amt = r["amount"]
        cat = r["category"]
        bank = r.get("payment_method", "未知")
        day = r["time"][:10] if r["time"] else "unknown"

        if r["direction"] == "收入":
            total_income += amt
            daily_income[day] += amt
            by_bank[bank]["income"] += amt
        else:
            total_expense += amt
            cat_totals[cat]["amount"] += amt
            cat_totals[cat]["count"] += 1
            daily_expense[day] += amt
            by_bank[bank]["expense"] += amt
        by_bank[bank]["count"] += 1

    lines = []
    lines.append(f"# {title} — {year}年{month}月")
    lines.append("")
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 总支出 | ¥{total_expense:,.2f} |")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} |")
    lines.append(f"| 交易笔数 | {len(records)} 笔 |")
    if total_expense > 0:
        expense_days = len(daily_expense)
        lines.append(f"| 日均消费 | ¥{total_expense / max(1, expense_days):,.2f} |")
    lines.append("")

    lines.append("## 按银行统计")
    lines.append("")
    lines.append("| 银行 | 支出 | 收入 | 笔数 |")
    lines.append("|------|------|------|------|")
    for bank, data in sorted(by_bank.items(), key=lambda x: -x[1]["expense"]):
        lines.append(f"| {bank} | ¥{data['expense']:,.2f} | ¥{data['income']:,.2f} | {data['count']}笔 |")
    lines.append("")

    sorted_cats = sorted(cat_totals.items(), key=lambda x: -x[1]["amount"])
    if sorted_cats:
        lines.append("## 分类支出")
        lines.append("")
        lines.append("| 分类 | 金额 | 笔数 | 占比 |")
        lines.append("|------|------|------|------|")
        for cat, data in sorted_cats:
            pct = data["amount"] / total_expense * 100 if total_expense > 0 else 0
            lines.append(f"| {cat} | ¥{data['amount']:,.2f} | {data['count']}笔 | {pct:.1f}% |")
        lines.append("")

    lines.append("## 交易明细")
    lines.append("")
    lines.append("| 时间 | 银行 | 金额 | 商户 |")
    lines.append("|------|------|------|------|")
    for r in records:
        t = r["time"][:16] if r["time"] else "?"
        bank = r.get("payment_method", "?")
        amt = f"¥{r['amount']:,.2f}"
        mch = r["merchant"][:30]
        lines.append(f"| {t} | {bank} | {amt} | {mch} |")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append(f"*数据来源：{source_label}*")
    lines.append("")
    return "\n".join(lines)


def generate_combined_report(
    wx_records: list, sms_records: list,
    dedup_result: dict, year: int, month: int,
) -> str:
    """生成微信+短信综合月报。"""
    total_expense = 0.0
    total_income = 0.0

    lines = []
    lines.append(f"# 综合财务月报 — {year}年{month}月")
    lines.append("")
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 微信记录 | {len(wx_records)} 笔 |")
    lines.append(f"| 短信记录 | {dedup_result['sms_total']} 笔 |")
    lines.append(f"| 跨源去重 | {dedup_result['deduped']} 笔 |")
    lines.append(f"| 短信独有 | {len(dedup_result['sms_only'])} 笔 |")
    lines.append(f"| 合计交易 | {len(wx_records) + len(dedup_result['sms_only'])} 笔 |")
    lines.append("")

    all_records = wx_records + dedup_result["sms_only"]
    for r in all_records:
        if r["direction"] == "收入":
            total_income += r["amount"]
        else:
            total_expense += r["amount"]

    lines.append(f"| 总支出 | ¥{total_expense:,.2f} |")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} |")
    lines.append("")

    if dedup_result["sms_only"]:
        lines.append("## 短信补充记录（微信未覆盖）")
        lines.append("")
        lines.append("| 时间 | 银行 | 金额 | 卡号 | 商户 |")
        lines.append("|------|------|------|------|------|")
        for r in dedup_result["sms_only"]:
            t = r["time"][:16] if r["time"] else "?"
            bank = r.get("payment_method", "?")
            amt = f"¥{r['amount']:,.2f}"
            card = r.get("card_suffix", "")
            mch = r["merchant"][:30]
            lines.append(f"| {t} | {bank} | {amt} | 尾号{card} | {mch} |")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：微信聊天记录 + 手机短信*")
    lines.append("")
    return "\n".join(lines)


# ── CLI 主入口 ──


def _load_wechat_records(account: str, year: int, month: int) -> list:
    """调用 wechat_finance 加载微信端记录。"""
    try:
        from aimod.wechat_client import WeChatClient
        from life.wechat_finance import parse_finance_messages, classify_merchant, load_category_map
    except ImportError:
        log.warning("无法导入 wechat_finance，跳过微信加载")
        return []

    client = WeChatClient(mode="local", db_path=str(JOPLINAI / "data" / "wcitemsall_merged.db"))

    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"

    msgs = client.query(account, date_from=date_from, date_to=date_to, limit=50000)
    if not msgs:
        return []

    records = parse_finance_messages(msgs)
    cat_map = load_category_map()
    for r in records:
        if r["category"] == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)

    log.info(f"微信端: {len(msgs)}条消息 → {len(records)}条财务记录")
    return records


def process_sms_month(
    year: int, month: int,
    with_wechat: bool = False,
    account: str = "白晔峰",
    sms_db: str = None,
) -> str:
    """处理指定月份的短信数据，返回 Markdown 报告。"""
    if sms_db is None:
        sms_db = SMS_DB

    db_path = Path(sms_db)
    if not db_path.exists():
        return f"# SMS 数据不可用\n\n数据库不存在: {sms_db}\n"

    conn = sqlite3.connect(str(db_path))
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"

    rows = conn.execute(
        "SELECT sms_id, number, body, received FROM sms_messages WHERE received >= ? AND received < ? ORDER BY received",
        (date_from, date_to),
    ).fetchall()
    conn.close()

    if not rows:
        return f"# SMS 月报 — {year}年{month}月\n\n该月无短信记录。\n"

    sms_list = [{"_id": r[0], "number": r[1], "body": r[2], "received": r[3]} for r in rows]

    sms_records = parse_sms_messages(sms_list)
    log.info(f"SMS端: {len(rows)}条短信 → {len(sms_records)}条财务记录")

    if not with_wechat:
        return generate_sms_report(sms_records, year, month)

    wx_records = _load_wechat_records(account, year, month)
    if not wx_records:
        return generate_sms_report(sms_records, year, month, title="短信财务月报（微信数据不可用）")

    dedup_result = dedup_against_wechat(sms_records, wx_records)
    log.info(f"去重: 短信{dedup_result['sms_total']}条, 匹配{dedup_result['deduped']}条(已在微信), 独有{len(dedup_result['sms_only'])}条")

    return generate_combined_report(wx_records, sms_records, dedup_result, year, month)


def main():
    parser = argparse.ArgumentParser(description="SMS 短信财务月报")
    parser.add_argument("--month", default="", help="月份 YYYY-MM，默认上月")
    parser.add_argument("--wechat", action="store_true", help="加载微信记录做去重合并")
    parser.add_argument("--account", default="白晔峰")
    parser.add_argument("--output", "-o", help="输出到文件")
    args = parser.parse_args()

    now = datetime.now()
    if args.month:
        if args.month == "prev":
            first = now.replace(day=1) - timedelta(days=1)
            year, month = first.year, first.month
        else:
            year, month = int(args.month[:4]), int(args.month[5:7])
    else:
        first = now.replace(day=1) - timedelta(days=1)
        year, month = first.year, first.month

    report = process_sms_month(year, month, with_wechat=args.wechat, account=args.account)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"报告已写入: {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
