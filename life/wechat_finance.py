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
# # 微信聊天记录财务解析与月报生成

# %%
"""从微信聊天记录中提取消费信息，生成月度财务报告。

数据源：
- 微信支付交易通知（sender 含 "微信支付"）
- 银行卡交易提醒（sender 含银行名或 content 含消费/支出）
- 转账/红包记录

用法：
    python life/wechat_finance.py --account 白晔峰 --month 2026-06
    python life/wechat_finance.py --account 白晔峰 --month prev
"""

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pathmagic

with pathmagic.context():
    from func.logme import log

from life.bank_tx_kind import (
    CARD_REPAYMENT,
    category_for_kind,
    classify_tx_kind,
    is_income_kind,
    strip_txn_suffix,
)

# 添加 joplinai 路径以便导入 WeChatClient
JOPLINAI = Path(__file__).resolve().parent.parent.parent / "joplinai"
if str(JOPLINAI) not in sys.path:
    sys.path.insert(0, str(JOPLINAI))

log = logging.getLogger("wechat_finance")

__all__ = [
    "parse_finance_messages",
    "classify_merchant",
    "load_category_map",
    "save_category_map",
    "generate_finance_report",
    "process_month",
]

# ── 正则模式（基于实际数据格式）──

# 微信支付 ¥XX.XX paid 消息 — 最干净的消费记录
RE_PAID = re.compile(r"[￥¥](\d+\.?\d*)\s*paid", re.IGNORECASE)

# 英文回执（工作号常见）：Weixin Pay Receipt[Paid by X￥20.00]
RE_EN_RECEIPT = re.compile(
    r"Weixin Pay Receipt.*?[￥¥](\d+\.?\d*)", re.IGNORECASE | re.DOTALL)

# 免密/自动扣费：Deducted: ¥88.00[Auto-renewed by CMB Debit Card(9929)￥88.00]
RE_DEDUCTED = re.compile(r"Deducted\s*[:：]\s*[￥¥](\d+\.?\d*)", re.IGNORECASE)

# 中文自动扣费：扣费通知[扣费金额￥12.00 ... 支付方式建设银行信用卡(9170)]
RE_DEDUCT = re.compile(r"扣费金额[：:]?\s*[￥¥](\d+\.?\d*)")

# 旧版支付成功：已支付¥90.00[使用零钱支付￥90.00]
RE_ALREADY_PAID = re.compile(r"已支付[：:]?\s*[￥¥](\d+\.?\d*)")

# 退款回执：Refund Amount¥7.00 / 退款金额￥X
RE_REFUND = re.compile(r"(?:Refund Amount|退款金额)\s*[：:]?\s*[￥¥]?\s*(\d+\.?\d*)",
                       re.IGNORECASE)

# 微信支付凭证 — 含商户名的消费记录（付款金额和商品名称之间换行分隔）
RE_WXPAY_RECEIPT = re.compile(
    r"付款金额[￥¥](\d+\.?\d*).*?商品名称(.+?)(?:\s*\||\n|$)", re.DOTALL
)

# 银行交易提醒中的金额和商户
RE_BANK_AMOUNT = re.compile(r"交易金额[：:]\s*([\d]+\.?\d*)")  # 交易金额：210.00
RE_BANK_MERCHANT = re.compile(r"交易商户[：:]\s*(.+?)(?:\n|$)")
RE_BANK_CARD = re.compile(
    r"(?:尾号|交易卡号[：:]?)\s*(?:[^\d\n]{0,6}[（(]?)?\s*(\d{4})"
    r"\s*(储蓄卡|借记卡|信用卡)?")
RE_BANK_TX_TYPE = re.compile(r"交易类型[：:]\s*([^\n\r]+)")
RE_BANK_SUMMARY = re.compile(r"摘\s*要[：:]\s*([^\n\r]+)")
RE_BANK_PARTY = re.compile(r"交易对象[：:]\s*([^\n\r]+)")

# 收到转账消息
RE_RECEIVED_TRANSFER = re.compile(r"收到转账([\d]+\.?\d*)元")

# 工作号导出的转账卡片（Sharing 类型）：[转账 ￥2000.00]
RE_TRANSFER_CARD = re.compile(r"^\[转账\s*[￥¥]?\s*([\d,]+(?:\.\d+)?)\s*\]")

# 银行卡消费/收入关键词
_RE_BANK_SENDER = re.compile(
    r"(银行|信用卡|信用卡还款|微众银行|网商银行|银盛支付|微信转账助手)"
)
_RE_BANK_CONSUME_KW = ["消费", "支出", "pos", "POS", "快捷"]
_RE_BANK_INCOME_KW = ["收入", "工资", "报销", "转入", "入账", "收益"]
_RE_BANK_SKIP_KW = ["还款", "最后还款日", "账单", "分期", "失败", "退款", "退税"]

# 微信支付排除关键词（非实际消费）
_RE_WXPAY_SKIP = ["零钱提现", "记账月报", "记账日报", "收款月报", "收款周报", "收款汇总"]

# 微信转账助手排除关键词（汇总/月报/周报）
_RE_TRANSFER_SKIP = ["收款月报", "收款周报", "收款日报", "转账收款汇总", "转账即将过期"]

# 标准金额提取
RE_AMOUNT = re.compile(r"[￥¥](\d+\.?\d*)")

# ── 默认商户分类映射 ──

_DEFAULT_CATEGORY_MAP = {
    # 餐饮-外卖
    "美团外卖": "餐饮-外卖", "饿了么": "餐饮-外卖", "百度外卖": "餐饮-外卖",
    # 餐饮-饮品
    "瑞幸": "餐饮-饮品", "星巴克": "餐饮-饮品", "蜜雪冰城": "餐饮-饮品",
    "库迪": "餐饮-饮品", "喜茶": "餐饮-饮品", "霸王茶姬": "餐饮-饮品",
    # 餐饮-正餐
    "海底捞": "餐饮-正餐", "西贝": "餐饮-正餐", "肯德基": "餐饮-正餐",
    "麦当劳": "餐饮-正餐", "必胜客": "餐饮-正餐",
    "冯校长老火锅": "餐饮-正餐", "大宅门夜宵": "餐饮-正餐", "美团收银": "餐饮-外卖",
    "逸富广场": "购物-其他",
    # 购物-便利店
    "美宜佳": "购物-便利店",
    # 交通-网约车
    "滴滴": "交通-网约车", "高德打车": "交通-网约车", "曹操出行": "交通-网约车",
    "T3出行": "交通-网约车", "花小猪": "交通-网约车",
    # 交通-公共交通
    "北京公交": "交通-公共交通", "地铁": "交通-公共交通",
    "铁路": "交通-公共交通", "12306": "交通-公共交通",
    # 交通-加油充电
    "中石化": "交通-加油", "中石油": "交通-加油",
    "特来电": "交通-充电", "星星充电": "交通-充电",
    # 购物-电商
    "京东": "购物-电商", "淘宝": "购物-电商", "天猫": "购物-电商",
    "拼多多": "购物-电商", "得物": "购物-电商", "闲鱼": "购物-电商",
    "唯品会": "购物-电商",
    # 购物-超市
    "永辉": "购物-超市", "盒马": "购物-超市", "物美": "购物-超市",
    "沃尔玛": "购物-超市", "山姆": "购物-超市", "Costco": "购物-超市",
    # 购物-其他
    "名创优品": "购物-其他", "无印良品": "购物-其他", "屈臣氏": "购物-其他",
    "MINISO": "购物-其他",
    # 居住
    "房租": "居住-房租", "物业": "居住-物业",
    "电费": "居住-水电", "水费": "居住-水电", "燃气": "居住-燃气",
    # 医疗
    "叮当快药": "医疗-药品", "药店": "医疗-药品",
    "医院": "医疗-就诊", "诊所": "医疗-就诊",
    # 社交
    "微信红包": "社交-红包", "转账": "社交-转账",
    # 收入
    "工资": "收入-工资", "报销": "收入-报销", "退款": "收入-退款",
}


def load_category_map(path: str = None) -> dict:
    """加载商户分类映射表。"""
    if path is None:
        path = str(Path(__file__).resolve().parent.parent / "data" / "finance_category_map.json")
    p = Path(path)
    if p.exists():
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return dict(_DEFAULT_CATEGORY_MAP)


def save_category_map(mapping: dict, path: str = None):
    if path is None:
        path = str(Path(__file__).resolve().parent.parent / "data" / "finance_category_map.json")
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def classify_merchant(merchant: str, category_map: dict = None) -> str:
    """商户名 → 分类。完全匹配优先，否则按匹配键长度降序模糊匹配（长键优先）。"""
    if not merchant:
        return "未分类-其他"
    if category_map is None:
        category_map = _DEFAULT_CATEGORY_MAP
    merchant = merchant.strip()
    # 完全匹配
    if merchant in category_map:
        return category_map[merchant]
    # 模糊匹配：商户名包含分类键，按键长度降序（避免"交通银行"误夺"交通银行信用卡"）
    for key, cat in sorted(category_map.items(), key=lambda x: -len(x[0])):
        if key in merchant or merchant in key:
            return cat
    return "未分类-其他"


def _parse_amount(text: str) -> float:
    """从文本中提取金额。"""
    nums = RE_AMOUNT.findall(text)
    for n in nums:
        try:
            return float(n.replace(",", ""))
        except ValueError:
            continue
    return 0.0


def _extract_merchant(content: str) -> str:
    """从微信支付凭证/回执中提取商户名。"""
    m = RE_WXPAY_RECEIPT.search(content)
    if m:
        return _normalize_merchant(m.group(2))
    for key in ("Merchant Name", "Shop Name", "服务商家", "Service商家", "Service",
                "商品名称", "扣费项目", "扣费服务", "扣费内容",
                "Product Details", "Item"):
        m = re.search(rf"{key}\s*([^|\]|\n]+)", content)
        if m and m.group(1).strip():
            return _normalize_merchant(m.group(1))
    return ""


# 卡组织缩写（大小写不敏感，长缩写优先匹配，避免 CMB 抢 CMBC）
_BANK_ABBR = {
    "CMBC": "民生银行", "CITIC": "中信银行", "PSBC": "邮储银行", "COMM": "交通银行",
    "SPDB": "浦发银行", "ICBC": "工商银行", "GDB": "广发银行", "CEB": "光大银行",
    "CMB": "招商银行", "CCB": "建设银行", "ABC": "农业银行", "BOC": "中国银行",
    "CIB": "兴业银行", "PAB": "平安银行",
}
_BANK_CN = (
    ("广发", "广发银行"), ("招商", "招商银行"), ("建设", "建设银行"),
    ("工商", "工商银行"), ("农业", "农业银行"), ("中国银行", "中国银行"),
    ("交通", "交通银行"), ("光大", "光大银行"), ("浦发", "浦发银行"),
    ("中信", "中信银行"), ("平安", "平安银行"), ("邮储", "邮储银行"),
    ("民生", "民生银行"), ("兴业", "兴业银行"),
)


def _detect_payment_method(content: str) -> str:
    """从 paid/凭证消息中检测支付方式（银行名 + 卡种）。"""
    upper = content.upper()
    bank = ""
    for abbr in sorted(_BANK_ABBR, key=len, reverse=True):
        if abbr in upper:
            bank = _BANK_ABBR[abbr]
            break
    if not bank:
        for kw, name in _BANK_CN:
            if kw in content:
                bank = name
                break
    if bank:
        credit = "CREDIT" in upper or "信用卡" in content
        debit = "DEBIT" in upper or "储蓄卡" in content or "借记卡" in content
        return bank + ("信用卡" if credit else ("" if debit else "信用卡"))
    if "Balance" in content or "零钱" in content:
        return "零钱"
    return "微信支付"


_RE_PAYMENT_METHOD = re.compile(
    r"(?:付款方式|支付方式)\s*[：:]?\s*([^\n\]|()（）]+?)\s*"
    r"(?:[(（]\s*(?:尾号)?\s*(\d{4})\s*[)）])?\s*(?=[\]|，,;；\n]|$)")
_RE_PAID_BY = re.compile(
    r"(?:Paid by|Auto-renewed by|Deducted by)\s+([^\n(￥¥]+?)\s*"
    r"(?:\((\d{4})\))?\s*(?=[￥¥\]\n]|$)", re.IGNORECASE)
_RE_USE_PAY = re.compile(r"使用([^\s￥¥\[|\n]+?)支付")


def _payment_info(content: str) -> tuple:
    """返回（支付方式, 卡号后四位）。"""
    m = _RE_PAYMENT_METHOD.search(content)
    if m and m.group(1).strip():
        raw = m.group(1).strip()
        method = _detect_payment_method(raw)
        if method == "微信支付" and raw:
            method = raw
        return method, (m.group(2) or "")
    m = _RE_PAID_BY.search(content)
    if m:
        raw = m.group(1).strip()
        suffix = m.group(2) or ""
        if raw.lower() == "balance":
            return "零钱", suffix
        return _detect_payment_method(raw), suffix
    m = _RE_USE_PAY.search(content)
    if m:
        raw = m.group(1).strip()
        if "零钱" in raw:
            return "零钱", ""
        return _detect_payment_method(raw), ""
    return _detect_payment_method(content), ""


def _refund_method(content: str) -> tuple:
    """退款去向：Refund to payment card(CEB 2621) / Refund to balance / Return to COMM (5631)。"""
    m = re.search(r"(?:Refund to|Return to)\s+([^\n\]|]+)", content, re.IGNORECASE)
    if not m:
        return "", ""
    raw = m.group(1).strip()
    if "balance" in raw.lower():
        return "零钱", ""
    suffix = re.search(r"[(（]\s*(?:\D{2,8}\s*)?(\d{4})\s*[)）]", raw)
    if suffix:
        return _detect_payment_method(raw), suffix.group(1)
    return _detect_payment_method(raw), ""


RE_BANK_MERCHANT_INLINE = re.compile(
    r"在([^\n，,。；;]{2,30}?)(?:消费|支出|扣款|付款|支付)[\d￥¥]?")
RE_BANK_MERCHANT_FIELD = re.compile(
    r"(?:商户名称|交易商户|商户)[：:]\s*([^\n|\]]+)")
RE_BANK_AMOUNT_FIELD = re.compile(
    r"交易金额[：:]\s*(?:人民币|￥|¥)?\s*(-?[\d,]+\.?\d*)")
RE_BANK_AGREEMENT = re.compile(r"从约定账户成功扣款人民币(\d+\.?\d*)元")

_SERVICE_AMOUNT_PATS = (
    re.compile(r"支付金额[：:]\s*(?:人民币|￥|¥)?\s*([\d,]+\.?\d*)"),
    re.compile(r"还款金额[：:]\s*(?:人民币|￥|¥)?\s*([\d,]+\.?\d*)"),
    re.compile(r"扣款人民币([\d,]+\.?\d*)元"),
    re.compile(r"缴费(?:金额)?[：:]?\s*(?:人民币|￥|¥)?\s*([\d,]+\.?\d*)元"),
)

_RE_MERCHANT_PREFIX = re.compile(r"^(?:支付宝-)+|^(?:财付通-|微信支付-)+|^（特约）")
_RE_MERCHANT_SUFFIX = re.compile(r"(?:-消费|-快捷支付|消费)$")


def _parse_bank_amount(content: str) -> float:
    """交易金额字段优先，避免误取可用额度/余额。"""
    m = RE_BANK_AMOUNT_FIELD.search(content)
    if m:
        return float(m.group(1).replace(",", ""))
    for pat in _SERVICE_AMOUNT_PATS:
        m = pat.search(content)
        if m:
            return float(m.group(1).replace(",", ""))
    return _parse_amount(content)


def _normalize_merchant(name: str) -> str:
    """去渠道前缀/后缀；渠道自身也作为商户兜底（支付宝/微信扫码）。"""
    if not name:
        return ""
    raw = name.strip()
    out = _RE_MERCHANT_PREFIX.sub("", raw)
    out = _RE_MERCHANT_SUFFIX.sub("", out).strip(" -—")
    if "扫二维码付款" in out or "扫二维码付款" in raw:
        return "微信扫码"
    if not out or out in ("消费", "财付通微信支付", "微信支付"):
        return "支付宝" if raw.startswith("支付宝") else ""
    return out


def _extract_bank_merchant(content: str) -> str:
    """从银行交易提醒/公众号通知中提取商户名。"""
    m = RE_BANK_MERCHANT_FIELD.search(content)
    if m and m.group(1).strip():
        return _normalize_merchant(m.group(1))
    m = RE_BANK_MERCHANT.search(content)
    if m:
        return _normalize_merchant(m.group(1))
    m = RE_BANK_MERCHANT_INLINE.search(content)
    if m:
        return _normalize_merchant(m.group(1))
    return ""


def _parse_time_seconds(time_str: str) -> float:
    """将时间字符串转为秒数（用于时间差计算）。"""
    if not time_str:
        return 0.0
    try:
        dt = datetime.strptime(time_str[:19], "%Y-%m-%d %H:%M:%S")
        return dt.timestamp()
    except (ValueError, OSError):
        return 0.0


def _is_bank_sender(sender: str) -> bool:
    """判断 sender 是否为银行/信用卡/金融类。"""
    if not sender:
        return False
    if sender in ("信用卡还款", "微信转账助手"):
        return True
    return bool(_RE_BANK_SENDER.search(sender))


_RE_REPAY_AMOUNT = re.compile(r"还款金额[:：]\s*[￥¥]?\s*([\d,]+\.?\d*)")
_RE_REPAY_FEE = re.compile(r"还款手续费[:：]\s*[￥¥]?\s*([\d,]+\.?\d*)")


_RE_PAID_TRANSFER = re.compile(
    r"other party has received the money|对方已收款|转账给", re.IGNORECASE)


def _is_paid_transfer(content: str) -> bool:
    """paid 回执备注「对方已收款」= 转给个人，不是商户消费。"""
    return bool(_RE_PAID_TRANSFER.search(content))


def _service_event(sender: str, content: str) -> dict:
    """服务号/公众号交易通知模板：命中返回事件字段，否则空 dict。"""
    # 美团钱包：支付成功提醒[..支付金额：23.10元..支付方式：浦发银行信用卡(3669)]
    if sender == "美团钱包" and "支付成功提醒" in content:
        amt = _parse_bank_amount(content)
        if amt > 0:
            pm, suffix = _payment_info(content)
            return {"amount": amt, "merchant": "美团", "payment_method": pm,
                    "card_suffix": suffix, "source": "paid"}
    return {}


def _is_repayment_paid(messages: list, idx: int, paid_amount: float) -> bool:
    """paid 消息后紧跟还款成功通知，且金额+手续费与 paid 一致 → 信用卡还款。"""
    for nxt in messages[idx + 1:idx + 4]:
        content = nxt.get("content") or ""
        if "还款成功通知" not in content:
            continue
        m = _RE_REPAY_AMOUNT.search(content)
        if not m:
            continue
        total = float(m.group(1).replace(",", ""))
        f = _RE_REPAY_FEE.search(content)
        if f:
            total += float(f.group(1).replace(",", ""))
        if abs(total - paid_amount) < 0.02:
            return True
    return False


def _collect_finance_events(messages: list) -> list:
    """Phase 1: 遍历所有消息，收集原始财务事件。

    每条事件含 time, amount, source(paid/voucher/bank_expense/bank_income/transfer),
    merchant, direction, payment_method 等字段。
    """
    events = []

    for idx, msg in enumerate(messages):
        sender = msg.get("sender", "")
        content = msg.get("content") or ""
        msg_time = msg.get("time", "")
        msg_type = msg.get("type", "")

        evt = {
            "time": msg_time,
            "amount": 0.0,
            "source": "",
            "merchant": "",
            "direction": "支出",
            "category": "未分类-其他",
            "payment_method": "",
            "source_text": content[:100],
            "raw": msg,
        }

        # ── 零钱提现/充值（微信余额↔银行卡互转）──
        # 发起与到账各有一条通知，只保留最终到账/成功的一条
        if ("零钱提现发起" in content or "零钱充值发起" in content):
            continue
        if "零钱提现" in content or "零钱充值" in content:
            m = RE_AMOUNT.search(content)
            if m:
                amt = float(m.group(1))
                if amt > 0:
                    kw = "零钱提现" if "零钱提现" in content else "零钱充值"
                    evt["amount"] = amt
                    evt["source"] = "paid"
                    evt["merchant"] = kw
                    evt["category"] = "内部-转账"
                    evt["payment_method"] = "零钱"
                    events.append(evt)
                    continue

        # ── 退款回执（退回银行卡/零钱）──
        if sender == "微信支付" and ("Refund Processed Notification" in content
                                     or "Expired Transfer Refund Alert" in content
                                     or "退款到账" in content):
            if "Refund Initiation" in content:
                continue
            m = RE_REFUND.search(content)
            if m:
                amt = float(m.group(1))
                if amt > 0:
                    pm, suffix = _refund_method(content)
                    if not pm and "Expired Transfer Refund" in content:
                        pm, suffix = _refund_method(
                            content.replace("Refund method", "Return to"))
                    evt["amount"] = amt
                    evt["source"] = "refund"
                    evt["direction"] = "收入"
                    evt["category"] = "收入-退款"
                    evt["merchant"] = _extract_merchant(content) or "退款"
                    evt["payment_method"] = pm
                    if suffix:
                        evt["card_suffix"] = suffix
                    events.append(evt)
                    continue

        # ── 微信支付消息 ──
        if sender == "微信支付":
            if any(kw in content for kw in _RE_WXPAY_SKIP):
                continue

            # 各版本扣款回执：paid / 英文回执 / Deducted / 扣费通知 / 旧版已支付
            amt = 0.0
            for pat in (RE_PAID, RE_EN_RECEIPT, RE_DEDUCTED, RE_DEDUCT,
                        RE_ALREADY_PAID):
                m = pat.search(content)
                if m:
                    amt = float(m.group(1))
                    break
            if amt > 0:
                evt["amount"] = amt
                if _is_repayment_paid(messages, idx, amt):
                    # 零钱还信用卡：paid 金额=还款金额+手续费，记为内部转账
                    evt["source"] = "repayment"
                    evt["merchant"] = "信用卡还款"
                    evt["category"] = "内部-转账"
                    evt["payment_method"] = "零钱"
                elif _is_paid_transfer(content):
                    # 转给个人：英文回执无商户，直接记转账（支出）
                    evt["source"] = "transfer_paid"
                    evt["direction"] = "支出"
                    evt["category"] = "社交-转账"
                    evt["merchant"] = "微信转账"
                    pm, suffix = _payment_info(content)
                    evt["payment_method"] = pm
                    if suffix:
                        evt["card_suffix"] = suffix
                else:
                    evt["source"] = "paid"
                    pm, suffix = _payment_info(content)
                    evt["payment_method"] = pm
                    if suffix:
                        evt["card_suffix"] = suffix
                    evt["merchant"] = _extract_merchant(content)
                events.append(evt)
                continue

            # 微信支付凭证
            if "微信支付凭证" in content:
                amt = _parse_amount(content)
                if amt > 0:
                    evt["amount"] = amt
                    evt["source"] = "voucher"
                    evt["merchant"] = _extract_merchant(content)
                    pm, suffix = _payment_info(content)
                    evt["payment_method"] = pm
                    if suffix:
                        evt["card_suffix"] = suffix
                    events.append(evt)
                    continue
            continue

        # ── 服务号/公众号交易通知（美团钱包等）──
        svc = _service_event(sender, content)
        if svc:
            evt.update(svc)
            evt["direction"] = "支出"
            events.append(evt)
            continue

        # ── 转账卡片（工作号 Sharing 消息，双侧同额各一条）──
        m = RE_TRANSFER_CARD.match(content.strip())
        if m:
            amt = float(m.group(1).replace(",", ""))
            if amt > 0:
                evt["amount"] = amt
                evt["source"] = "transfer"
                evt["direction"] = "支出" if msg.get("send") else "收入"
                evt["category"] = "社交-转账"
                events.append(evt)
                continue

        # ── 收到转账消息（个人对个人）──
        m = RE_RECEIVED_TRANSFER.search(content)
        if m:
            amt = float(m.group(1))
            if amt > 0:
                evt["amount"] = amt
                evt["source"] = "transfer"
                # send=1 表示我发起的转账（我转出），send=0 表示我接收的转账
                is_send = msg.get("send", False)
                evt["direction"] = "支出" if is_send else "收入"
                evt["category"] = "社交-转账"
                events.append(evt)
                continue

        # ── 微信转账助手汇总消息（排除）──
        if sender == "微信转账助手":
            if any(kw in content for kw in _RE_TRANSFER_SKIP):
                continue

        # ── 京东白条消息（格式不同：消费金额/服务商户而非交易金额/交易商户）──
        if sender == "京东白条":
            # 支付宝渠道还款被白条拒付：记还款线索，供账本把随后同额的
            # 银行卡扣款识别为花呗账单还款（白条失败后改走储蓄卡通道）。
            if "交易失败通知" in content and "账户已锁定" in content:
                m_amt = re.search(r"金额[：:]\s*([\d,]+\.?\d*)元", content)
                m_mer = re.search(r"商户[：:]\s*([^）)\n]*)", content)
                m_src = re.search(r"来源[：:]\s*([^，,\n]*)", content)
                merchant = m_mer.group(1).strip() if m_mer else ""
                source_ch = m_src.group(1).strip() if m_src else ""
                if (m_amt and merchant.startswith("还款")
                        and source_ch.startswith("支付宝")):
                    evt["amount"] = float(m_amt.group(1).replace(",", ""))
                    evt["source"] = "repayment_hint"
                    evt["merchant"] = "花呗"
                    evt["category"] = "借贷-还款"
                    evt["direction"] = "支出"
                    evt["payment_method"] = "京东白条"
                    events.append(evt)
                    continue
            # 失败通知/额度通知/非交易提醒 → 跳过
            if any(kw in content for kw in ("失败通知", "交易到账通知", "账单", "还款", "分期", "红包")):
                continue
            # 交易提醒 → 提取金额和商户
            if "交易提醒" in content:
                m = re.search(r"消费金额[：:](\d+\.?\d*)元", content)
                if m:
                    amt = float(m.group(1))
                else:
                    amt = _parse_amount(content)
                if amt > 0:
                    m2 = re.search(r"服务商户[：:]\s*(.+?)(?:\n|\)|$)", content)
                    merchant = m2.group(1).strip() if m2 else "京东白条"
                    evt["amount"] = amt
                    evt["source"] = "bank_expense"
                    evt["merchant"] = merchant
                    evt["payment_method"] = "京东白条"
                    events.append(evt)
                    continue
            continue

        # ── 银行/信用卡消息 ──
        if _is_bank_sender(sender):
            # 约定账户自动扣款 = 信用卡还款的账户侧（含“还款”字样，须先于过滤）
            m_agree = RE_BANK_AGREEMENT.search(content)
            if m_agree:
                evt["amount"] = float(m_agree.group(1))
                evt["direction"] = "收入"
                evt["source"] = "bank_income"
                evt["org"] = sender
                evt["merchant"] = "信用卡还款"
                evt["category"] = "内部-转账"
                m_card = RE_BANK_CARD.search(content)
                if m_card:
                    evt["card_suffix"] = m_card.group(1)
                events.append(evt)
                continue

            # “交易类型”字段优先；否则才用摘要/交易对象/正文关键词。
            # 交易对象里的“消费金融”不能把收入通知误判成消费。
            m_type = RE_BANK_TX_TYPE.search(content)
            explicit = m_type.group(1).strip() if m_type else ""
            m_sum = RE_BANK_SUMMARY.search(content)
            summary = m_sum.group(1).strip() if m_sum else ""
            m_party = RE_BANK_PARTY.search(content)
            party = m_party.group(1).strip() if m_party else ""
            party = re.sub(r"^\d+\*+\d+\s*", "", party)

            kind, _kw = classify_tx_kind(explicit or summary or party)
            evt["tx_kind"] = kind or ""
            is_refund = kind == "refund"
            is_cc_repay = kind == CARD_REPAYMENT
            is_transfer = kind == "transfer"
            debit_acct = any(k in content for k in (
                "储蓄卡", "借记卡", "储蓄账户", "您账户"))
            credit_acct = "信用卡" in content

            # 交易金额字段优先，避免误取“可用额度/余额”；农行等把收支
            # 符号写在金额里（负数=支出），须先于关键词判定方向。
            m_amt_field = RE_BANK_AMOUNT_FIELD.search(content)
            amt_signed = (float(m_amt_field.group(1).replace(",", ""))
                          if m_amt_field else None)

            # 提醒/账单/失败类跳过；但带“交易金额”的还款/退款是真实交易
            is_skip = any(kw in content for kw in _RE_BANK_SKIP_KW)
            if is_skip and not (
                    kind in (CARD_REPAYMENT, "refund") and amt_signed is not None):
                continue

            if amt_signed is not None and amt_signed < 0:
                is_income, is_expense = False, True
            elif kind:
                is_income = is_cc_repay or is_income_kind(kind) or is_transfer
                is_expense = not is_income
            else:
                is_income = any(kw in content for kw in _RE_BANK_INCOME_KW)
                is_expense = (not is_income) and any(
                    kw in content for kw in _RE_BANK_CONSUME_KW)
                # 借记卡“交易金额”为正且无其它线索 → 入账
                if (not (is_income or is_expense) and amt_signed is not None
                        and amt_signed > 0 and debit_acct and not credit_acct):
                    is_income = True
            if is_cc_repay:
                # 还款腿方向：储蓄账户出、信用卡入
                if any(k in content for k in ("还款支出", "支出人民币")):
                    is_income, is_expense = False, True
                elif debit_acct and not credit_acct:
                    is_income, is_expense = False, True
                else:
                    is_income, is_expense = True, False
            if not (is_income or is_expense):
                continue

            m_card = RE_BANK_CARD.search(content)
            if m_card:
                evt["card_suffix"] = m_card.group(1)

            amt = (abs(amt_signed) if amt_signed is not None
                   else _parse_bank_amount(content))
            if amt <= 0:
                continue
            evt["amount"] = amt
            evt["direction"] = "收入" if is_income else "支出"
            evt["source"] = "bank_income" if is_income else "bank_expense"
            evt["org"] = sender

            merchant = _extract_bank_merchant(content)
            if kind == "income" and (not merchant or merchant == sender):
                merchant = strip_txn_suffix(summary or explicit) or merchant
            evt["merchant"] = merchant or sender

            transfer_hint = any(k in (summary + party) for k in (
                "转账", "汇入", "银联入账", "提现", "转存"))
            loan_hint = any(k in (summary + party) for k in (
                "放款", "消费金融", "借款到账", "贷款发放"))

            kind_cat = category_for_kind(kind, evt["merchant"])
            if is_cc_repay:
                evt["category"] = "内部-转账"
                evt["merchant"] = "信用卡还款"
            elif is_refund:
                evt["category"] = "收入-退款"
            elif is_transfer:
                evt["category"] = (
                    "内部-转账" if "白晔峰" in content else "社交-转账")
            elif kind == "income":
                if kind_cat == "收入-工资" or not transfer_hint:
                    evt["category"] = kind_cat
                else:
                    evt["category"] = (
                        "内部-转账" if "白晔峰" in content else "社交-转账")
                    evt["merchant"] = party or evt["merchant"]
            elif kind_cat:
                evt["category"] = kind_cat
            elif is_income:
                if loan_hint:
                    evt["category"] = "借贷-放款"
                    evt["merchant"] = party or evt["merchant"]
                elif transfer_hint:
                    evt["category"] = (
                        "内部-转账" if "白晔峰" in content else "社交-转账")
                    evt["merchant"] = party or evt["merchant"]
                else:
                    evt["category"] = "收入-其他"
            elif "白晔峰" in content:
                evt["category"] = "内部-转账"
            elif is_transfer or transfer_hint:
                evt["category"] = "社交-转账"
            events.append(evt)
            continue

        # 红包不在这里解析：真实收发红包走微信支付回执/银行短信，
        # 聊天里的「红包」多是公众号营销（「最高￥400交通补贴」之类），
        # 按关键词取金额会造出假的支出流水。

    # 按时间排序（空时间放最后）
    events.sort(key=lambda e: e["time"] or "9999")
    return events


_MERGE_WINDOW = 30  # 归并时间窗口（秒）


def _find_companions(events: list, start_idx: int, used: list) -> list:
    """在已排序 events 中，查找与 start_idx 事件配对的事件。

    条件：时间差 < _MERGE_WINDOW 秒 + 金额完全匹配。
    返回匹配事件的索引列表。"""
    evt = events[start_idx]
    base_ts = _parse_time_seconds(evt["time"])
    if base_ts <= 0:
        return []

    companions = []
    n = len(events)

    # 银行通知常先于微信回执到达，须双向查找同一窗口内的事件
    window = []
    j = start_idx - 1
    while j >= 0:
        ts = _parse_time_seconds(events[j]["time"])
        if ts > 0 and base_ts - ts > _MERGE_WINDOW:
            break
        if ts > 0:
            window.append(j)
        j -= 1
    j = start_idx + 1
    while j < n:
        ts = _parse_time_seconds(events[j]["time"])
        if ts > 0 and ts - base_ts > _MERGE_WINDOW:
            break
        if ts > 0:
            window.append(j)
        j += 1
    window.sort()

    for j in window:
        if used[j]:
            continue
        other = events[j]
        if abs(other["amount"] - evt["amount"]) >= 0.01:
            continue

        # 回执/凭证/银行短信之间互相补充（双向，避免顺序不同导致重复计账）
        if evt["source"] in ("paid", "voucher"):
            if other["source"] in ("bank_expense", "voucher", "paid"):
                pms = (evt.get("payment_method", ""), other.get("payment_method", ""))
                if pms[0] and pms[1] and pms[0] != pms[1]:
                    continue
                companions.append(j)
        # 微信退款回执 + 银行退款通知 = 同一笔退款
        elif evt["source"] == "refund" and other["source"] == "bank_income":
            companions.append(j)

    return companions


def _build_record(primary: dict, companions: list) -> dict:
    """将主事件和它的 companion 事件合并为一条消费记录。

    优先级：
    - 金额/方向/时间从 primary 取
    - 商户名：bank_expense > voucher > primary 的 merchant
    """
    record = {
        "time": primary["time"],
        "amount": primary["amount"],
        "merchant": primary["merchant"] or "",
        "direction": primary["direction"],
        "category": primary["category"],
        "source": primary.get("source", ""),
        "payment_method": primary.get("payment_method", ""),
        "card_suffix": primary.get("card_suffix", ""),
        "source_text": primary.get("source_text", ""),
        "raw": primary.get("raw", {}),
        "source_ids": [primary.get("raw", {}).get("id")],
    }

    for c in companions:
        cid = c.get("raw", {}).get("id")
        if cid is not None:
            record["source_ids"].append(cid)
        # 优先用银行通知的商户名（通常比微信凭证更规范）
        if c["source"] == "bank_expense" and c["merchant"]:
            record["merchant"] = c["merchant"]
        # 其次用凭证/回执的商户名
        elif (c["source"] in ("voucher", "paid") and c["merchant"]
              and not record["merchant"]):
            record["merchant"] = c["merchant"]
        # 补充支付方式/卡尾号（比“微信支付”更具体的优先）
        if c.get("payment_method") and (
                not record["payment_method"]
                or record["payment_method"] in ("微信支付",)):
            record["payment_method"] = c["payment_method"]
        if c.get("card_suffix") and not record["card_suffix"]:
            record["card_suffix"] = c["card_suffix"]

    # 如果仍然没有商户名，用支付方式代替
    if not record["merchant"]:
        pm = record.get("payment_method") or "微信支付"
        if pm not in ("零钱", "微信支付"):
            record["merchant"] = pm

    return record


def parse_finance_messages(messages: list) -> list:
    """从消息列表中提取去重后的财务记录。

    去重策略（Phase 2）：
    - bank_expense 不独立产生记录，只作为已存在记录的商户名补充
    - 同一笔交易的多个通知（paid + voucher + bank）归并为一条记录
    - 归并条件：时间差 < 30秒 + 金额精确匹配
    - 转账按 (sender, amount, 日期) 分组去重，避免合并库双侧数据重复
    """
    events = _collect_finance_events(messages)

    # 主号合并库会把同一通知存成多行（不同 id），先按内容去重
    seen = set()
    uniq = []
    for evt in events:
        key = (evt["time"], evt["source"], round(evt["amount"], 2),
               evt["direction"], evt["merchant"], evt.get("card_suffix", ""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(evt)
    events = uniq

    # 转账卡片/退款回执的时间表：同一笔交易的两条通知只记一次
    card_transfer_times = [
        (round(e["amount"], 2), e["time"])
        for e in events if e["source"] == "transfer"
    ]
    refund_receipt_times = [
        (round(e["amount"], 2), e["time"])
        for e in events if e["source"] == "refund"
    ]

    def _has_near(times: list, evt: dict) -> bool:
        amt = round(evt["amount"], 2)
        ts = _parse_time_seconds(evt["time"])
        for c_amt, c_time in times:
            if c_amt != amt:
                continue
            c_ts = _parse_time_seconds(c_time)
            if ts <= 0 or c_ts <= 0 or abs(ts - c_ts) <= 600:
                return True
        return False

    def _is_card_transfer(evt: dict) -> bool:
        return _has_near(card_transfer_times, evt)

    n = len(events)
    used = [False] * n
    records = []

    for i in range(n):
        if used[i]:
            continue
        evt = events[i]

        # bank_expense 不独立产生记录（由对应的 paid/voucher 承载）
        if evt["source"] == "bank_expense":
            continue

        # 还款线索不是交易本身，原样传给账本做银行卡扣款匹配
        if evt["source"] == "repayment_hint":
            records.append({
                "time": evt["time"], "amount": evt["amount"],
                "merchant": evt["merchant"], "direction": "支出",
                "category": evt["category"], "source": "repayment_hint",
                "payment_method": evt.get("payment_method", ""),
                "source_text": evt.get("source_text", ""), "raw": evt["raw"],
                "source_ids": [evt["raw"].get("id")],
            })
            used[i] = True
            continue

        # 零钱还信用卡：独立记一条内部转账（付款侧）
        if evt["source"] == "repayment":
            records.append({
                "time": evt["time"], "amount": evt["amount"],
                "merchant": evt["merchant"] or "信用卡还款",
                "direction": "支出", "category": "内部-转账",
                "payment_method": evt.get("payment_method", "零钱"),
                "source_text": evt.get("source_text", ""), "raw": evt["raw"],
                "source_ids": [evt["raw"].get("id")],
            })
            used[i] = True
            continue

        # 微信 paid 转账回执（“对方已收款”无商户）：记转账而非消费；
        # 与转账卡片同额同笔时以卡片为准，避免双计
        if evt["source"] == "transfer_paid":
            used[i] = True
            if _is_card_transfer(evt):
                continue
            records.append({
                "time": evt["time"], "amount": evt["amount"],
                "merchant": evt["merchant"] or "微信转账",
                "direction": "支出", "category": "社交-转账",
                "source": "transfer_paid",
                "payment_method": evt.get("payment_method", ""),
                "card_suffix": evt.get("card_suffix", ""),
                "source_text": evt.get("source_text", ""), "raw": evt["raw"],
                "source_ids": [evt["raw"].get("id")],
            })
            continue

        # transfer 由 pass 2 按 (sender, amount, 日期) 分组去重
        if evt["source"] == "transfer":
            continue

        # bank_income 直接产生收入记录（无对应微信支付通知）；
        # 银行退款通知与微信退款回执同笔时以回执为准，避免双计
        if evt["source"] == "bank_income":
            if (evt.get("category") == "收入-退款"
                    and _has_near(refund_receipt_times, evt)):
                used[i] = True
                continue
            records.append({
                "time": evt["time"], "sender": evt["raw"].get("sender", ""),
                "amount": evt["amount"], "merchant": evt["merchant"] or evt["raw"].get("sender", ""),
                "direction": "收入", "category": evt["category"],
                "source": "bank_income",
                "org": evt["raw"].get("sender", ""),
                "card_suffix": evt.get("card_suffix", ""),
                "source_text": evt.get("source_text", ""), "raw": evt["raw"],
                "source_ids": [evt["raw"].get("id")],
            })
            used[i] = True
            continue

        # 查找 companion 事件
        companions_idx = _find_companions(events, i, used)
        companion_events = [events[j] for j in companions_idx]
        record = _build_record(evt, companion_events)

        records.append(record)
        used[i] = True
        for j in companions_idx:
            used[j] = True

    # ── Pass 1.5: 未被 paid/voucher 消耗的 bank_expense → 独立支出记录 ──
    # 未绑定微信的银行卡只有银行短信通知，无微信支付消息，
    # 这些 bank_expense 没有 companion，需要独立产生记录
    for i in range(n):
        if not used[i] and events[i]["source"] == "bank_expense":
            evt = events[i]
            records.append({
                "time": evt["time"],
                "amount": evt["amount"],
                "merchant": evt["merchant"] or evt["raw"].get("sender", ""),
                "direction": "支出",
                "category": evt["category"],
                "source": "bank_expense",
                "org": evt["raw"].get("sender", ""),
                "payment_method": evt["raw"].get("sender", ""),
                "card_suffix": evt.get("card_suffix", ""),
                "source_text": evt.get("source_text", ""),
                "raw": evt["raw"],
                "source_ids": [evt["raw"].get("id")],
            })
            used[i] = True

    # ── Pass 2: 转账去重 ──
    # 同一笔转账在合并库中可能出现两次（双侧手机导出），
    # 按 (sender, amount, 日期) 分组，只保留一条记录
    transfer_groups = defaultdict(list)
    for i, evt in enumerate(events):
        if not used[i] and evt["source"] == "transfer":
            key = (evt["raw"]["sender"], round(evt["amount"], 2), evt["time"][:10])
            transfer_groups[key].append(i)

    for key, indices in transfer_groups.items():
        for i in indices:
            used[i] = True
        sender, amount, _ = key

        # 确定方向：取最早事件的 send 标志
        # 收入场景：先收到通知 (send=0) → 后再确认 (send=1)
        # 支出场景：我先发起 (send=1) → 对方确认 (send=0)
        earliest_idx = min(indices, key=lambda i: events[i]["time"])
        is_income = not events[earliest_idx]["raw"].get("send")  # send=0 为收入
        primary = events[earliest_idx]

        direction = "收入" if is_income else "支出"

        records.append({
            "time": primary["time"],
            "amount": amount,
            "merchant": sender,
            "direction": direction,
            "category": "社交-转账",
            "source_text": primary.get("source_text", ""),
            "raw": primary["raw"],
            "source_ids": [events[i]["raw"].get("id") for i in indices],
        })

    # 排序：按时间
    records.sort(key=lambda r: r.get("time") or "")

    return records


def _trend_icon(current: float, previous: float) -> str:
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


def generate_finance_report(records: list, year: int, month: int, prev_records: list = None) -> str:
    """生成月度财务报告 Markdown。"""
    cat_totals = defaultdict(lambda: {"amount": 0.0, "count": 0})
    merchant_totals = defaultdict(lambda: {"amount": 0.0, "count": 0})
    daily_expense = defaultdict(float)
    daily_income = defaultdict(float)
    total_expense = 0.0
    total_income = 0.0

    for r in records:
        amt = r["amount"]
        cat = r["category"]
        mch = r["merchant"]
        day = r["time"][:10] if r["time"] else "unknown"
        if r["direction"] == "收入":
            total_income += amt
            daily_income[day] += amt
        else:
            total_expense += amt
            cat_totals[cat]["amount"] += amt
            cat_totals[cat]["count"] += 1
            merchant_totals[mch]["amount"] += amt
            merchant_totals[mch]["count"] += 1
            daily_expense[day] += amt

    prev_total = sum(r["amount"] for r in prev_records if r["direction"] != "收入") if prev_records else 0

    lines = []
    lines.append(f"# 微信消费月报 — {year}年{month}月")
    lines.append("")

    # 概要
    lines.append("## 概要")
    lines.append("")
    lines.append("| 指标 | 数值 | 环比 |")
    lines.append("|------|------|------|")
    lines.append(f"| 总支出 | ¥{total_expense:,.2f} | {_trend_icon(total_expense, prev_total)} |")
    lines.append(f"| 总收入 | ¥{total_income:,.2f} | — |")
    lines.append(f"| 交易笔数 | {len(records)} 笔 | — |")
    expense_days = len(daily_expense)
    lines.append(f"| 日均消费 | ¥{total_expense / max(1, expense_days):,.2f} | — |")
    if total_expense > 0:
        max_amt = max(r["amount"] for r in records if r["direction"] != "收入")
        max_mch = next((r["merchant"] for r in records if r["amount"] == max_amt and r["direction"] != "收入"), "")
        lines.append(f"| 最大单笔 | ¥{max_amt:,.2f} ({max_mch}) | — |")
    lines.append("")

    # 分类排行
    lines.append("## 分类支出排行")
    lines.append("")
    sorted_cats = sorted(cat_totals.items(), key=lambda x: x[1]["amount"], reverse=True)
    lines.append("| 分类 | 金额 | 占比 | 笔数 |")
    lines.append("|------|------|------|------|")
    for cat, vals in sorted_cats:
        pct = vals["amount"] / total_expense * 100 if total_expense > 0 else 0
        lines.append(f"| {cat} | ¥{vals['amount']:,.2f} | {pct:.1f}% | {vals['count']} |")
    lines.append("")

    # 商户排行（仅支出）
    lines.append("## 商户消费排行 Top 10")
    lines.append("")
    sorted_mchs = sorted(merchant_totals.items(), key=lambda x: x[1]["amount"], reverse=True)[:10]
    lines.append("| 排名 | 商户 | 金额 | 笔数 | 分类 |")
    lines.append("|------|------|------|------|------|")
    for i, (mch, vals) in enumerate(sorted_mchs, 1):
        cat = "未分类"
        for r in records:
            if r["merchant"] == mch:
                cat = r["category"]
                break
        lines.append(f"| {i} | {mch} | ¥{vals['amount']:,.2f} | {vals['count']} | {cat} |")
    lines.append("")

    # 日支出分布
    lines.append("## 日支出分布")
    lines.append("")
    sorted_days = sorted(daily_expense.items())
    if sorted_days:
        max_daily = max(v for _, v in sorted_days)
        lines.append("```")
        for day, amt in sorted_days:
            bar_len = max(1, int(amt / max(max_daily, 1) * 30))
            bar = "█" * bar_len
            lines.append(f"  {day[-5:]}  {bar}  ¥{amt:,.2f}")
        lines.append("```")
    lines.append("")

    # 未分类交易
    uncategorized = [r for r in records if r["category"].startswith("未分类")]
    if uncategorized:
        lines.append("## 未分类交易（待确认）")
        lines.append("")
        lines.append("| 时间 | 商户 | 金额 | 原文 |")
        lines.append("|------|------|------|------|")
        for r in uncategorized[:20]:
            lines.append(f"| {r['time'][:10]} | {r['merchant']} | ¥{r['amount']:,.2f} | {r['source_text'][:40]} |")
        if len(uncategorized) > 20:
            lines.append(f"| ... | 还有 {len(uncategorized) - 20} 条 | ... | ... |")
        lines.append("")

    # 分析建议
    lines.append("## 分析与建议")
    lines.append("")
    if sorted_cats:
        top_cat = sorted_cats[0]
        lines.append(f"- 本月 **{top_cat[0]}** 支出最高，达 ¥{top_cat[1]['amount']:,.2f}，"
                     f"占总支出的 {top_cat[1]['amount']/total_expense*100:.1f}%")
    if total_expense > 0 and prev_total > 0:
        change = (total_expense - prev_total) / prev_total * 100
        if abs(change) > 10:
            direction = "增长" if change > 0 else "下降"
            lines.append(f"- 总支出环比{direction} {abs(change):.1f}%")
    if uncategorized:
        lines.append(f"- 发现 {len(uncategorized)} 笔未分类交易，建议补充商户分类映射")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("*数据来源：微信聊天记录合并库*")
    lines.append("")

    return "\n".join(lines)


def process_month(account: str, year: int, month: int, client=None) -> str:
    """处理指定月份，返回 Markdown 报告。"""
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

    records = parse_finance_messages(all_msgs)
    log.info(f"{year}-{month:02d}: 识别 {len(records)} 条财务记录")

    cat_map = load_category_map()
    for r in records:
        if r["category"] == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)

    prev_records = []
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    try:
        prev_from = f"{prev_year}-{prev_month:02d}-01"
        prev_to = f"{prev_year}-{prev_month + 1:02d}-01" if prev_month < 12 else f"{prev_year + 1}-01-01"
        prev_msgs = client.query(account, date_from=prev_from, date_to=prev_to, limit=50000)
        prev_records = parse_finance_messages(prev_msgs)
        for r in prev_records:
            if r["category"] == "未分类-其他":
                r["category"] = classify_merchant(r["merchant"], cat_map)
    except Exception as e:
        log.warning(f"加载上月数据失败: {e}")

    report = generate_finance_report(records, year, month, prev_records)
    return report


def main():
    parser = argparse.ArgumentParser(description="微信消费月报生成")
    parser.add_argument("--account", default="白晔峰")
    parser.add_argument("--month", help="月份 YYYY-MM 或 prev")
    parser.add_argument("--output", "-o", help="输出到文件（默认 stdout）")
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

    log.info(f"开始处理 {year}-{month:02d} 财务月报")
    report = process_month(args.account, year, month)

    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
        print(f"报告已保存: {args.output}")
    else:
        print(report)

    log.info(f"财务月报 {year}-{month:02d} 完成")


if __name__ == "__main__":
    main()
