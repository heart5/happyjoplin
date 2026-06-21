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
# 通过 voice_api HTTP API 获取短信数据，解析银行通知提取金额/卡号/商户，
# 支持与微信记录去重合并，生成综合月报。
# 贷款类交易（放款/还款）单独归类，不计入常规收支。

# %%
"""
SMS 短信财务解析模块。

数据源：通过 voice_api HTTP API 获取（而非直读 SQLite）。
贷款处理：放款 = 借贷-放款（非收入），还款 = 借贷-还款（非支出）。

用法：
    python life/sms_finance.py --month 2026-06              # 只看短信
    python life/sms_finance.py --month 2026-06 --wechat     # 含微信去重
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
    from life.ledger.cloudcfg import (
        get_sms_api_url, get_wechat_api_url,
        get_bank_short_codes, get_loan_platforms,
        get_loan_disbursement_keywords, get_loan_repayment_keywords,
    )

log = logging.getLogger("sms_finance")

__all__ = [
    "parse_sms_records",
    "dedup_against_wechat",
    "generate_sms_report",
    "process_sms_month",
]

# ── API 配置 ──

SMS_API_URL = get_sms_api_url()
WECHAT_API_URL = get_wechat_api_url()
BANK_SHORT_CODES = get_bank_short_codes()
LOAN_PLATFORMS = get_loan_platforms()

# ── 正则模式 ──

RE_CARD_SUFFIX = re.compile(r"尾[号账](\d{4})")
RE_AMOUNT_CNY = re.compile(r"[人民币￥¥](\d+\.?\d*)")
# 确认还款：排除"如已成功还款"/"若已成功还款"等条件前缀
_RE_CONFIRM_REPAYMENT = re.compile(r"(?<![如若]已)成功还款")

# 交易失败关键词
_RE_FAILURE_KW = ["交易失败", "因额度不足", "因余额不足失败", "余额不足失败"]

# ── 银行/机构名识别 ──


def _detect_org(number: str, body: str) -> str:
    """识别短信发件机构名。"""
    if number in BANK_SHORT_CODES:
        return BANK_SHORT_CODES[number]

    for prefix, name in sorted(BANK_SHORT_CODES.items(), key=lambda x: -len(x[0])):
        if number.startswith(prefix):
            return name

    m = re.search(r"[【](.+?)[】]", body)
    if m:
        name = m.group(1).strip()
        name = re.sub(r"(尊敬的|您好).*", "", name).strip()
        if name and len(name) < 25:
            return name

    return number


def _is_loan_platform(org: str) -> bool:
    """判断是否贷款平台。"""
    for p in LOAN_PLATFORMS:
        if p in org:
            return True
    return False


def _is_loan_disbursement(body: str) -> bool:
    """是否贷款放款（资金进入账户）。"""
    # 先排除提醒/广告/失败类误匹配
    if any(kw in body for kw in ("预计", "申请", "评估", "可提现", "额度", "失败")):
        return False
    return any(kw in body for kw in get_loan_disbursement_keywords())


def _is_loan_repayment(body: str) -> bool:
    """是否贷款还款（资金从账户扣走）。仅确认已发生的还款，排除提醒/催收。

    注意：确认词优先于排除词——同一条短信可能同时含
    "已主动还款"和"未还清"，应判定为实际还款而非提醒。
    """
    # 1) 明确确认已发生的还款——优先级最高
    # "成功还款" 使用正则排除 "如已成功还款"/"若已成功还款" 条件前缀
    if _RE_CONFIRM_REPAYMENT.search(body):
        return True
    if any(kw in body for kw in ("还款成功",
                                   "已自动还款", "已主动还款",
                                   "自动还款成功")):
        return True

    # 2) "代扣"+"失败"已在上面 catch，来自贷款平台的代扣就是实际还款
    if "代扣" in body and "失败" not in body:
        return True

    # 3) 排除提醒/催收/失败类短信
    reminder_kw = ("应还", "需还款", "请还款", "还款提醒", "还款日为", "本期还款",
                   "已过期", "已逾期", "即将到期", "温馨提示",
                   "扣款失败", "还款失败",
                   "待还", "剩余待还",
                   "已还忽略", "已还请忽略", "如已还款请忽略",
                   "尽快")
    for kw in reminder_kw:
        if kw in body:
            return False

    return False


# ── 金额/商户/卡号解析 ──


def _parse_amount(body: str, org: str) -> float:
    """从短信提取金额。"""
    if "招商" in org or "银行" in org:
        m = re.search(r"人民币(\d+\.?\d*)", body)
        if m:
            return float(m.group(1))
    if "交通银行" in org or "建设银行" in org:
        m = re.search(r"(?:网络支付转入|支付转入|转入资金|转入|转出|消费|存入|支付)(\d+\.?\d*)元", body)
        if m:
            return float(m.group(1))
    if "农业银行" in org:
        m = re.search(r"交易人民币(\d+\.?\d*)", body)
        if m:
            return float(m.group(1))
    m = RE_AMOUNT_CNY.search(body)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+\.?\d*)元", body)
    if m:
        return float(m.group(1))
    return 0.0


def _extract_card_suffix(body: str) -> str:
    """提取卡号尾号。"""
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


def _extract_merchant(body: str) -> str:
    """提取商户名。"""
    patterns = [
        (r"在(.+?)(?:消费|交易|支付)", 1),
        (r"交易商户[：:]\s*(.+)", 1),
        (r"-([一-鿿].+)$", 1),
        (r"向\s*(.+?)\s*转账", 1),
        (r"收款[人方](.+?)(?:\s|$)", 1),
        (r"对方[：:户名]?\s*(.+?)(?:\s|$)", 1),
    ]
    for pat, group in patterns:
        m = re.search(pat, body)
        if m:
            merchant = m.group(group).strip().rstrip("。，")
            if len(merchant) > 1 and not re.match(r"^\d+$", merchant):
                return merchant
    return ""


def _detect_direction(body: str, org: str, is_loan: bool) -> str:
    """判断交易方向。"""
    if is_loan:
        return "收入" if _is_loan_disbursement(body) else "支出"
    # "收款" 是入账，但注意 "收款人" 表示收款方（支出交易中的对方信息）
    if re.search(r"收款[\d]", body):
        return "收入"
    for kw in ["收入", "入账", "存入", "转入资金", "银联入账", "汇款", "转入"]:
        if kw in body:
            return "收入"
    for kw in ["消费", "支出", "转出", "支付", "快捷", "POS", "pos", "转至他行"]:
        if kw in body:
            return "支出"
    return "支出"


# ── 单条解析 ──


def _parse_record(msg: dict) -> dict:
    """单条短信 → 财务记录。"""
    body = str(msg.get("body", ""))
    number = str(msg.get("number", ""))
    received = str(msg.get("received", ""))

    # 过滤失败交易（额度不足/余额不足等非实际交易）
    if any(kw in body for kw in _RE_FAILURE_KW):
        return {"amount": 0.0, "time": received, "_skip": True}

    org = _detect_org(number, body)
    is_loan = _is_loan_platform(org)
    amount = _parse_amount(body, org)
    direction = _detect_direction(body, org, is_loan)
    merchant = _extract_merchant(body) or org
    card_suffix = _extract_card_suffix(body)

    if is_loan:
        if _is_loan_disbursement(body):
            category = "借贷-放款"
        elif _is_loan_repayment(body):
            category = "借贷-还款"
        else:
            category = "借贷-其他"
    else:
        category = "未分类-其他"

    return {
        "time": received,
        "amount": amount,
        "merchant": merchant,
        "direction": direction,
        "category": category,
        "payment_method": org,
        "card_suffix": card_suffix,
        "is_loan": is_loan,
        "source": "sms",
        "source_text": body[:120],
    }


# ── API 加载 ──


def _fetch_sms_api(date_from: str, date_to: str) -> list:
    """通过 voice_api HTTP API 获取短信数据。"""
    import urllib.request, json, ssl

    url = f"{SMS_API_URL}?date_from={date_from}&date_to={date_to}&limit=50000"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        resp = urllib.request.urlopen(url, timeout=30, context=ctx)
        data = json.loads(resp.read().decode())
        return data.get("records", [])
    except Exception as e:
        log.warning(f"SMS API 调用失败: {e}")
        return []


def _fetch_wechat_api(account: str, date_from: str, date_to: str) -> list:
    """通过 voice_api HTTP API 获取微信聊天记录。"""
    import urllib.request, json, ssl
    from urllib.parse import quote

    url = f"{WECHAT_API_URL}?account={quote(account)}&date_from={date_from}&date_to={date_to}&limit=50000"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        resp = urllib.request.urlopen(url, timeout=30, context=ctx)
        data = json.loads(resp.read().decode())
        return data.get("records", [])
    except Exception as e:
        log.warning(f"微信 API 调用失败: {e}")
        return []


# ── 解析入口 ──


def parse_sms_records(sms_list: list) -> list:
    """解析短信列表为财务记录。"""
    records = []
    for msg in sms_list:
        try:
            rec = _parse_record(msg)
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
    """短信 vs 微信去重。"""
    if not wechat_records:
        return {"matched": [], "sms_only": sms_records, "wechat_only": [],
                "sms_total": len(sms_records), "deduped": 0}

    wx_index = defaultdict(list)
    for r in wechat_records:
        day = r.get("time", "")[:10]
        amt = round(r["amount"], 2)
        card = r.get("card_suffix", "")
        wx_index[(day, amt, card)].append(r)
        wx_index[(day, amt, "")].append(r)

    matched, sms_only = [], []
    used_wx = set()

    for sms in sms_records:
        day = sms.get("time", "")[:10]
        amt = round(sms["amount"], 2)
        card = sms.get("card_suffix", "")
        candidates = wx_index.get((day, amt, card), [])
        found = False
        for wx in candidates:
            if id(wx) not in used_wx:
                matched.append(wx)
                used_wx.add(id(wx))
                found = True
                break
        if not found and not card:
            for wx in wx_index.get((day, amt, ""), []):
                if id(wx) not in used_wx:
                    matched.append(wx)
                    used_wx.add(id(wx))
                    found = True
                    break
        if not found:
            sms_only.append(sms)

    wechat_only = [r for r in wechat_records if id(r) not in used_wx]

    return {"matched": matched, "sms_only": sms_only, "wechat_only": wechat_only,
            "sms_total": len(sms_records), "deduped": len(matched)}


# ── 报告生成 ──


def _is_regular(r: dict) -> bool:
    """是否为常规收支（非贷款）。"""
    return not r.get("is_loan", False)


def generate_sms_report(
    records: list, year: int, month: int,
    title: str = "短信财务月报",
    source_label: str = "手机短信",
) -> str:
    """生成月度短信财务报告。"""
    regular = [r for r in records if _is_regular(r)]
    loans = [r for r in records if r.get("is_loan", False)]

    total_expense = sum(r["amount"] for r in regular if r["direction"] == "支出")
    total_income = sum(r["amount"] for r in regular if r["direction"] == "收入")
    loan_in = sum(r["amount"] for r in loans if r["direction"] == "收入")
    loan_out = sum(r["amount"] for r in loans if r["direction"] == "支出")

    by_bank = defaultdict(lambda: {"expense": 0.0, "income": 0.0, "count": 0})
    for r in regular:
        bank = r.get("payment_method", "未知")
        if r["direction"] == "收入":
            by_bank[bank]["income"] += r["amount"]
        else:
            by_bank[bank]["expense"] += r["amount"]
        by_bank[bank]["count"] += 1

    lines = []
    lines.append(f"# {title} — {year}年{month}月")
    lines.append("")

    # 概要
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 总支出 | ¥{total_expense:,.2f} |")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} |")
    lines.append(f"| 常规交易 | {len(regular)} 笔 |")
    if loans:
        lines.append(f"| 贷款放款 | ¥{loan_in:,.2f} |")
        lines.append(f"| 贷款还款 | ¥{loan_out:,.2f} |")
    lines.append("")

    # 按银行
    if by_bank:
        lines.append("## 按银行统计")
        lines.append("")
        lines.append("| 银行 | 支出 | 收入 | 笔数 |")
        lines.append("|------|------|------|------|")
        for bank, data in sorted(by_bank.items(), key=lambda x: -x[1]["expense"]):
            lines.append(f"| {bank} | ¥{data['expense']:,.2f} | ¥{data['income']:,.2f} | {data['count']}笔 |")
        lines.append("")

    # 贷款
    if loans:
        lines.append("## 贷款活动")
        lines.append("")
        lines.append("| 平台 | 放款 | 还款 | 笔数 |")
        lines.append("|------|------|------|------|")
        loan_by_org = defaultdict(lambda: {"in": 0.0, "out": 0.0, "count": 0})
        for r in loans:
            org = r.get("payment_method", "未知")
            if r["direction"] == "收入":
                loan_by_org[org]["in"] += r["amount"]
            else:
                loan_by_org[org]["out"] += r["amount"]
            loan_by_org[org]["count"] += 1
        for org, data in sorted(loan_by_org.items(), key=lambda x: -(x[1]["in"] + x[1]["out"])):
            lines.append(f"| {org} | ¥{data['in']:,.2f} | ¥{data['out']:,.2f} | {data['count']}笔 |")
        lines.append("")

    # 明细
    lines.append("## 交易明细")
    lines.append("")
    lines.append("| 时间 | 机构 | 金额 | 分类 | 商户 |")
    lines.append("|------|------|------|------|------|")
    for r in records:
        t = r["time"][:16] if r["time"] else "?"
        org = r.get("payment_method", "?")
        amt = f"¥{r['amount']:,.2f}"
        cat = r.get("category", "")
        mch = r["merchant"][:25]
        lines.append(f"| {t} | {org} | {amt} | {cat} | {mch} |")
    lines.append("")

    lines.append("---")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append(f"*数据来源：{source_label}*")
    return "\n".join(lines)


def generate_combined_report(
    wx_records: list, sms_records: list,
    dedup_result: dict, year: int, month: int,
) -> str:
    """生成微信+短信综合月报。"""
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

    all_regular = [r for r in wx_records if _is_regular(r)]
    all_regular += [r for r in dedup_result["sms_only"] if _is_regular(r)]
    all_loans = [r for r in dedup_result["sms_only"] if r.get("is_loan", False)]

    total_expense = sum(r["amount"] for r in all_regular if r["direction"] == "支出")
    total_income = sum(r["amount"] for r in all_regular if r["direction"] == "收入")
    loan_in = sum(r["amount"] for r in all_loans if r["direction"] == "收入")
    loan_out = sum(r["amount"] for r in all_loans if r["direction"] == "支出")

    lines.append(f"| 总支出 | ¥{total_expense:,.2f} |")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} |")
    lines.append(f"| 常规笔数 | {len(all_regular)} 笔 |")
    if all_loans:
        lines.append(f"| 贷款放款 | ¥{loan_in:,.2f} |")
        lines.append(f"| 贷款还款 | ¥{loan_out:,.2f} |")
    lines.append("")

    # 短信独有明细
    sms_only_regular = [r for r in dedup_result["sms_only"] if _is_regular(r)]
    if sms_only_regular:
        lines.append("## 短信补充（常规交易）")
        lines.append("")
        lines.append("| 时间 | 银行 | 金额 | 卡号 | 商户 |")
        lines.append("|------|------|------|------|------|")
        for r in sms_only_regular:
            t = r["time"][:16] if r["time"] else "?"
            bank = r.get("payment_method", "?")
            amt = f"¥{r['amount']:,.2f}"
            card = r.get("card_suffix", "")
            mch = r["merchant"][:25]
            lines.append(f"| {t} | {bank} | {amt} | 尾号{card} | {mch} |")
        lines.append("")

    # 短信独有贷款
    sms_only_loans = [r for r in dedup_result["sms_only"] if r.get("is_loan", False)]
    if sms_only_loans:
        lines.append("## 短信补充（贷款活动）")
        lines.append("")
        lines.append("| 时间 | 平台 | 金额 | 分类 |")
        lines.append("|------|------|------|------|")
        for r in sms_only_loans:
            t = r["time"][:16] if r["time"] else "?"
            org = r.get("payment_method", "?")
            amt = f"¥{r['amount']:,.2f}"
            lines.append(f"| {t} | {org} | {amt} | {r['category']} |")
        lines.append("")

    lines.append("---")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：微信聊天记录 + 手机短信*")
    return "\n".join(lines)


# ── 主流程 ──


def _load_wechat_records(account: str, year: int, month: int) -> list:
    """通过 API 加载微信端财务记录。"""
    try:
        from life.wechat_finance import parse_finance_messages, classify_merchant, load_category_map
    except ImportError:
        log.warning("无法导入 wechat_finance")
        return []

    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"

    msgs = _fetch_wechat_api(account, date_from, date_to)
    if not msgs:
        log.info("微信 API 无数据")
        return []

    records = parse_finance_messages(msgs)
    cat_map = load_category_map()
    for r in records:
        if r.get("category", "") == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)
        r["is_loan"] = False

    log.info(f"微信端: {len(msgs)}条消息 → {len(records)}条财务记录")
    return records


def process_sms_month(
    year: int, month: int,
    with_wechat: bool = False,
    account: str = "白晔峰",
) -> str:
    """处理指定月份的短信数据。"""
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"

    sms_list = _fetch_sms_api(date_from, date_to)
    if not sms_list:
        return f"# SMS 月报 — {year}年{month}月\n\n该月无短信记录。\n"

    sms_records = parse_sms_records(sms_list)
    log.info(f"SMS端: {len(sms_list)}条 → {len(sms_records)}条记录")

    if not with_wechat:
        return generate_sms_report(sms_records, year, month)

    wx_records = _load_wechat_records(account, year, month)
    if not wx_records:
        return generate_sms_report(sms_records, year, month, title="短信月报（微信不可用）")

    dedup_result = dedup_against_wechat(sms_records, wx_records)
    log.info(f"去重: 短信{dedup_result['sms_total']}条, 匹配{dedup_result['deduped']}条, 独有{len(dedup_result['sms_only'])}条")

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
