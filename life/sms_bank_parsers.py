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
# # 银行短信专用解析器
#
# 按银行分别实现短信解析，精确提取金额、卡号尾号、商户、交易方向。
# 通过 dispatch() 主入口按 number 分派到对应银行的 parser。
#
# 支持的银行：招商、交通、建设、农业、广发、光大、浦发

# %%
"""
银行短信专用解析器。

每个银行一个 _parse_xxx() 函数，注册到 _PARSERS 列表。
dispatch() 遍历 _PARSERS，匹配 number 前缀后调用对应 parser。

用法：
    from life.sms_bank_parsers import dispatch
    result = dispatch(number, body, received)
    if result:
        # result.amount, result.direction, result.card_suffix, ...

扩展：
    新增银行只需写一个 _parse_xxx() 函数并在 `_register_bank()` 中注册。
"""

import re
from dataclasses import dataclass, field
from typing import Optional

import pathmagic

with pathmagic.context():
    from func.logme import log

logger = log

__all__ = ["ParsedSMS", "dispatch", "register_bank", "register_fallback"]

# ── 返回数据结构 ──


@dataclass
class ParsedSMS:
    """结构化银行短信解析结果。"""
    amount: float               # 正数金额
    direction: str              # "收入" / "支出"
    card_suffix: str            # 卡号尾号
    merchant: str               # 商户名/用途
    counterparty: str           # 对方（人或机构）
    bank_name: str              # 银行标准名
    category: str = "未分类-其他"  # 分类（兼容 sms_finance）
    is_loan: bool = False       # 是否贷款相关
    tx_type_detail: str = ""    # 交易类型细分：transfer/consumption/loan_repayment/insurance/...
    tx_time: str = ""           # 交易时间（原短信 received）
    source_text: str = ""       # 原文截断
    _skip: bool = False         # True = 该条应跳过（失败交易等）


# ── 解析器注册 ──

# 每个元素: (number_prefix_regex, parser_fn(body, number) -> ParsedSMS | None)
_PARSERS = []


def register_bank(number_prefix: str):
    """装饰器：注册银行解析器，按 number 前缀匹配。

    匹配规则：number 以 number_prefix 开头。
    长前缀优先匹配，注册时已按长度降序排序。

    用法：
        @register_bank("95555")
        def _parse_cmb(body, number):
            ...
    """
    def decorator(fn):
        _PARSERS.append((number_prefix, fn))
        # 按前缀长度降序排列，长前缀优先匹配
        _PARSERS.sort(key=lambda x: -len(x[0]))
        return fn
    return decorator


def register_fallback(fn):
    """注册兜底解析器（所有前面 parser 都不匹配时调用）。"""
    _PARSERS.append(("", fn))
    return fn


# ── 金额工具 ──

_RE_CNY = re.compile(r"人民币?(\d+\.?\d*)")
_RE_YUAN = re.compile(r"(\d+\.?\d*)元")


def _extract_cny(body: str) -> Optional[float]:
    """提取 人民币X.XX / 人民币X.XX元 格式的金额。"""
    m = _RE_CNY.search(body)
    if m:
        return abs(float(m.group(1)))
    return None


def _extract_yuan(body: str) -> Optional[float]:
    """提取 X.XX元 格式的金额（无 人民币 前缀时）。"""
    m = _RE_YUAN.search(body)
    if m:
        return abs(float(m.group(1)))
    return None


def _extract_amount(body: str) -> float:
    """统一金额提取：先试 人民币X.XX，回落 X.XX元。"""
    amt = _extract_cny(body)
    if amt is not None:
        return amt
    amt = _extract_yuan(body)
    if amt is not None:
        return amt
    return 0.0


# ── 通用工具 ──

# 交易失败/退款/非实际交易关键词
_FAILURE_KW = ["交易失败", "因额度不足失败", "因余额不足失败",
               "余额不足失败", "未成功", "额度不足交易失败",
               "存在风险", "交易未成功"]
# 退款/退税 —— 是实际发生的交易（方向为收入）
_REFUND_KW = ["退款", "退税"]

# 信用卡还款（固额还款给信用卡）关键词
_CREDIT_CARD_REPAY_KW = ["向白晔峰信用卡卡号还款支出"]


def _is_failure(body: str) -> bool:
    return any(kw in body for kw in _FAILURE_KW)


def _is_refund(body: str) -> bool:
    return any(kw in body for kw in _REFUND_KW)


def _strip_trailing(text: str) -> str:
    return text.rstrip("。，.， \t\n\r").strip()


def _extract_body_org(body: str) -> Optional[str]:
    """从正文提取银行名：【招商银行】 / [建设银行]"""
    m = re.search(r"[【[](.+?)[】\]]", body)
    if m:
        return m.group(1).strip()
    return None


# ══════════════════════════════════════════
#  招商银行 (95555)
# ══════════════════════════════════════════

@register_bank("95555")
def _parse_cmb(body: str, number: str) -> Optional[ParsedSMS]:
    """招商银行短信解析。

    B 格式1：您账户9929于MM月DD日HH:MM实时转至他行人民币X.XX元，收款人XXX
    B 格式2：您账户9929于YYYY年MM月DD日HH:MM:SS扣款人民币X.XX，商户：XXX
    B 格式3：您账户9929于MM月DD日HH:MM在【XXX】快捷支付X.XX元
    A 格式4：您账户9929于MM月DD日他行实时转入人民币X.XX，付方XXX
    B 格式5：您的账户9929于YYYY年MM月DD日HH:MM:SS扣款归还个贷，人民币X.XX
    A 格式6：您账户9929于MM月DD日收到本行转入人民币X.XX，付方XXX（XXXX）
    A 格式7：您账户9929于MM月DD日HH:MM收款XXX.XX元，备注：XXX
    A 格式8：您账户9929于MM月DD日银联入账人民币X.XX元（XXX）
    A 格式9：您账户9929于MM月DD日HH:MM入账XXX.XX元（XXX）
    B 格式10：您账户9929于MM月DD日HH:MM转账汇款人民币X.XX，收款人：XXX
    B 格式11：您账户9929于MM月DD日HH:MM信用卡还款交易人民币X.XX
    B 格式12：您账户9929于MM月DD日HH:MM在【XXX】发生XX扣款人民币X.XX
    B 格式13：您账户9929于MM月DD日HH:MM本行ATM无卡取款人民币X.XX
    SKIP 格式14：您账户9929将于YYYY年MM月DD日扣款人民币X.XX（未来预告）
    SKIP 格式15：信用卡到期续发、自动还款关联无余额等非交易提醒
    """
    if "【招商银行】" not in body:
        return None
    if _is_failure(body):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="招商银行",
                         _skip=True)

    # 跳过非交易类短信
    _SKIP_KW = ["验证码", "对账单", "闪电贷", "招捷贷", "招企贷",
                 "惠企利民", "拒收请回复"]
    if any(kw in body for kw in _SKIP_KW):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="招商银行",
                         _skip=True)
    # 跳过营销类额度广告
    if "预授信" in body:
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="招商银行",
                         _skip=True)

    # 跳过信用卡到期续卡、自动还款失败提醒等非交易短信
    if "信用卡" in body:
        if any(kw in body for kw in ("到期", "续发", "续卡", "自动还款关联")):
            return ParsedSMS(amount=0, direction="支出", card_suffix="",
                             merchant="", counterparty="", bank_name="招商银行",
                             _skip=True)

    # 跳过扣款预告（将于…扣款，非实际交易）
    if "将于" in body and "扣款" in body:
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="招商银行",
                         _skip=True)

    card_suffix = ""
    m = re.search(r"账户(\d{4})", body)
    if m:
        card_suffix = m.group(1)

    # ══════════════ 收入类 ══════════════

    # 格式4 + 格式6：他行实时转入 / 本行转入 → 收入
    m_in = re.search(r"他行实时转入人民币(\d+\.?\d*).*?付方(.+?)(?:[。.，\s]|关闭|收益|砸|领|$)", body)
    if not m_in:
        m_in = re.search(r"收到本行转入人民币(\d+\.?\d*).*?付方(.+?)(?:[。.，\s]|备注|$)", body)
    if m_in:
        amount = float(m_in.group(1))
        counterparty = _strip_trailing(m_in.group(2))
        return ParsedSMS(
            amount=amount, direction="收入", card_suffix=card_suffix,
            merchant="他行转入" if "他行" in m_in.group(0) else "本行转入",
            counterparty=counterparty, bank_name="招商银行",
            category="收入-转账", tx_type_detail="transfer_in",
        )

    # 格式7：收款XXX.XX元，备注：XXX → 收入（微信零钱提现/支付宝转账）
    m_recv = re.search(r"收款(\d+\.?\d*)元.*?备注[：:]\s*(.+?)$", body)
    if m_recv:
        return ParsedSMS(
            amount=float(m_recv.group(1)), direction="收入", card_suffix=card_suffix,
            merchant=m_recv.group(2).strip(), counterparty="", bank_name="招商银行",
            category="收入-转账", tx_type_detail="transfer_in",
        )

    # 格式8：银联入账人民币X.XX元（XXX）→ 收入
    m_union = re.search(r"银联入账人民币(\d+\.?\d*)元（(.+?)）", body)
    if m_union:
        return ParsedSMS(
            amount=float(m_union.group(1)), direction="收入", card_suffix=card_suffix,
            merchant=m_union.group(2).strip(), counterparty="", bank_name="招商银行",
            category="收入-转账", tx_type_detail="transfer_in",
        )

    # 格式9：入账XXX.XX元（XXX）→ 收入
    m_in2 = re.search(r"入账(\d+\.?\d*)元（(.+?)）", body)
    if m_in2:
        return ParsedSMS(
            amount=float(m_in2.group(1)), direction="收入", card_suffix=card_suffix,
            merchant=m_in2.group(2).strip(), counterparty="", bank_name="招商银行",
            category="收入-转账", tx_type_detail="transfer_in",
        )

    # ══════════════ 贷款还款 ══════════════

    # 格式5：扣款归还个贷 → 贷款还款
    if "扣款归还个贷" in body:
        amount = _extract_cny(body) or 0.0
        return ParsedSMS(
            amount=amount, direction="支出", card_suffix=card_suffix,
            merchant="个贷还款", counterparty="招商银行", bank_name="招商银行",
            is_loan=True, category="借贷-还款", tx_type_detail="loan_repayment",
        )

    # 格式11：信用卡还款交易 → 内部转账（储蓄卡→信用卡），非实际支出
    if "信用卡还款交易" in body:
        amount = _extract_cny(body) or _extract_yuan(body) or 0.0
        return ParsedSMS(
            amount=amount, direction="支出", card_suffix=card_suffix,
            merchant="信用卡还款", counterparty="招商银行信用卡", bank_name="招商银行",
            category="内部-转账", tx_type_detail="transfer",
        )

    # ══════════════ 支出类 ══════════════

    # 格式12：在【XXX】发生XX扣款 → 贷款还款或消费
    m_deduct_merchant = re.search(r"在【(.+?)】发生.*?扣款人民币(\d+\.?\d*)", body)
    if m_deduct_merchant:
        amount = float(m_deduct_merchant.group(2))
        merchant = m_deduct_merchant.group(1)
        is_loan = any(kw in merchant for kw in ("金融", "消金", "贷款", "借呗", "还"))
        return ParsedSMS(
            amount=amount, direction="支出", card_suffix=card_suffix,
            merchant=merchant, counterparty=merchant, bank_name="招商银行",
            is_loan=is_loan,
            category="借贷-还款" if is_loan else "未分类-其他",
            tx_type_detail="loan_repayment" if is_loan else "consumption",
        )

    # 格式1：实时转至他行 → 转账支出
    m_transfer = re.search(r"实时转至他行人民币(\d+\.?\d*).*?收款人(.+?)$", body)
    if m_transfer:
        amount = float(m_transfer.group(1))
        counterparty = _strip_trailing(m_transfer.group(2))
        return ParsedSMS(
            amount=amount, direction="支出", card_suffix=card_suffix,
            merchant="跨行转账", counterparty=counterparty, bank_name="招商银行",
            category="未分类-其他", tx_type_detail="transfer_out",
        )

    # 格式10：转账汇款人民币X.XX，收款人：XXX → 转账支出
    m_transfer2 = re.search(r"转账汇款人民币(\d+\.?\d*).*?收款人[：:]\s*(.+?)$", body)
    if m_transfer2:
        return ParsedSMS(
            amount=float(m_transfer2.group(1)), direction="支出", card_suffix=card_suffix,
            merchant="转账汇款", counterparty=_strip_trailing(m_transfer2.group(2)),
            bank_name="招商银行", tx_type_detail="transfer_out",
        )

    # 格式2：扣款人民币，商户：XXX → 保险/其他支出
    m_deduct = re.search(r"扣款人民币(\d+\.?\d*).*?商户[：:]\s*(.+?)$", body)
    if m_deduct:
        amount = float(m_deduct.group(1))
        merchant = _strip_trailing(m_deduct.group(2))
        is_loan = "个贷" in body or "贷款" in body
        return ParsedSMS(
            amount=amount, direction="支出", card_suffix=card_suffix,
            merchant=merchant, counterparty=merchant, bank_name="招商银行",
            is_loan=is_loan,
            category="借贷-还款" if is_loan else "未分类-其他",
            tx_type_detail="deduction",
        )

    # 扣收 → 支出（短信费/账户管理费）
    m_deduct2 = re.search(r"扣收.*?人民币(\d+\.?\d*)", body)
    if m_deduct2:
        return ParsedSMS(
            amount=float(m_deduct2.group(1)), direction="支出", card_suffix=card_suffix,
            merchant="账户费用", counterparty="招商银行", bank_name="招商银行",
            tx_type_detail="fee",
        )

    # 格式3：在【XX】快捷支付 → 消费支出
    m_quick = re.search(r"在【(.+?)】快捷支付(\d+\.?\d*)元", body)
    if m_quick:
        amount = float(m_quick.group(2))
        merchant = m_quick.group(1)
        return ParsedSMS(
            amount=amount, direction="支出", card_suffix=card_suffix,
            merchant=merchant, counterparty=merchant, bank_name="招商银行",
            category="未分类-其他", tx_type_detail="consumption",
        )

    # 格式13：本行ATM无卡取款
    m_atm = re.search(r"本行ATM无卡取款人民币(\d+\.?\d*)", body)
    if m_atm:
        return ParsedSMS(
            amount=float(m_atm.group(1)), direction="支出", card_suffix=card_suffix,
            merchant="ATM取款", counterparty="", bank_name="招商银行",
            tx_type_detail="withdrawal",
        )

    # 兜底：匹配任意金额，带收入关键词检测
    amount = _extract_amount(body)
    if amount > 0:
        if "转至他行" in body or "转出" in body:
            direction, td = "支出", "transfer_out"
        elif "收款" in body or "入账" in body or "转入" in body:
            direction, td = "收入", "transfer_in"
        else:
            direction, td = "支出", "consumption"
        return ParsedSMS(
            amount=amount, direction=direction, card_suffix=card_suffix,
            merchant="", counterparty="", bank_name="招商银行",
            tx_type_detail=td,
        )

    return None


# ══════════════════════════════════════════
#  交通银行 (95559)
# ══════════════════════════════════════════

@register_bank("95559")
def _parse_comm(body: str, number: str) -> Optional[ParsedSMS]:
    """交通银行短信解析。

    格式1：贵账户*5631于YYYY年MM月DD日HH:MM在XXX跨行汇款转入资金X.XX元，...对方户名：XXX
    格式2：贵账户*5631于YYYY年MM月DD日HH:MM转出X.XX元，...摘要：XXX
    格式3：您尾号2349交行信用卡DD日HH时MM分成功消费人民币X.XX元
    格式4：您尾号*5631的卡于MM月DD日HH:MM网络支付转入X.XX元
    格式5：您尾号*5631的卡于MM月DD日HH:MM手机银行跨行汇款转出X.XX元
    格式6：您尾号*5631的卡于MM月DD日HH:MM在XXX网上支付X.XX元
    """
    if "【交通银行】" not in body and body.endswith("【交通银行】"):
        pass  # 交通银行的短信都会带 【交通银行】 后缀
    if "【交通银行】" not in body:
        # 短信末尾可能有【交通银行】，也可能没有（如验证码短信）
        if "交通银行" not in body and "交行" not in body:
            return None

    if _is_failure(body):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="交通银行",
                         _skip=True)

    # 跳过额度通知（"已超过信用额度"）非交易类提醒
    if "超过信用额度" in body:
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="交通银行",
                         _skip=True)

    # --- 信用卡消费：您尾号2349交行信用卡DD日HH时MM分成功消费人民币X.XX元 ---
    m_cc_consume = re.search(r"您尾号(\d{4})交行信用卡.*?成功消费人民币(\d+\.?\d*)元", body)
    if m_cc_consume:
        return ParsedSMS(
            amount=float(m_cc_consume.group(2)), direction="支出",
            card_suffix=m_cc_consume.group(1),
            merchant="信用卡消费", counterparty="", bank_name="交通银行",
            category="未分类-其他", tx_type_detail="consumption",
        )

    # --- 格式4：网络支付转入 → 收入 ---
    m_net_in = re.search(r"您尾号\*?(\d{4})的卡于.*?网络支付转入(\d+\.?\d*)元", body)
    if m_net_in:
        return ParsedSMS(
            amount=float(m_net_in.group(2)), direction="收入",
            card_suffix=m_net_in.group(1),
            merchant="网络支付转入", counterparty="", bank_name="交通银行",
            category="收入-转账", tx_type_detail="transfer_in",
        )

    # --- 格式1：跨行汇款转入 → 收入 ---
    m_cross_in = re.search(r"贵账户\*?(\d{4})于.*?跨行汇款转入资金(\d+\.?\d*)元.*?对方户名[：:]?(.*?)(?:，|$)", body)
    if m_cross_in:
        return ParsedSMS(
            amount=float(m_cross_in.group(2)), direction="收入",
            card_suffix=m_cross_in.group(1),
            merchant="跨行汇款转入", counterparty=_strip_trailing(m_cross_in.group(3)),
            bank_name="交通银行",
            category="收入-转账", tx_type_detail="transfer_in",
        )

    # --- 格式5：手机银行跨行汇款转出 / 转出 → 支出 ---
    m_out = re.search(r"您尾号\*?(\d{4})的卡于.*?(?:手机银行跨行汇款转出|转出)(\d+\.?\d*)元", body)
    if m_out:
        return ParsedSMS(
            amount=float(m_out.group(2)), direction="支出",
            card_suffix=m_out.group(1),
            merchant="跨行汇款转出", counterparty="", bank_name="交通银行",
            tx_type_detail="transfer_out",
        )

    # --- 格式2：贵账户转出 → 支出 ---
    m_cross_out = re.search(r"贵账户\*?(\d{4})于.*?转出(\d+\.?\d*)元.*?摘要[：:]?\s*(.*?)(?:。|$)", body)
    if m_cross_out:
        amt = float(m_cross_out.group(2))
        summary = _strip_trailing(m_cross_out.group(3))
        is_loan_repay = any(kw in summary for kw in ("金融还款", "还款"))
        return ParsedSMS(
            amount=amt, direction="支出",
            card_suffix=m_cross_out.group(1),
            merchant=summary or "转出",
            counterparty=summary, bank_name="交通银行",
            is_loan=is_loan_repay,
            category="借贷-还款" if is_loan_repay else "未分类-其他",
            tx_type_detail="loan_repayment" if is_loan_repay else "transfer_out",
        )

    # --- 格式6：网上支付 → 支出 ---
    m_online = re.search(r"您尾号\*?(\d{4})的卡于.*?在(.+?)网上支付(\d+\.?\d*)元", body)
    if m_online:
        return ParsedSMS(
            amount=float(m_online.group(3)), direction="支出",
            card_suffix=m_online.group(1),
            merchant=_strip_trailing(m_online.group(2)),
            counterparty=_strip_trailing(m_online.group(2)),
            bank_name="交通银行",
            tx_type_detail="consumption",
        )

    # 兜底
    amount = _extract_amount(body)
    card_suffix = ""
    m = re.search(r"[尾账户号]\*?\d{4}", body)
    # 提取更精确的卡号
    m_card = re.search(r"[尾账户号][\*#]?(\d{4})", body)
    if m_card:
        card_suffix = m_card.group(1) or ""
    m_card2 = re.search(r"账户\*(\d{4})", body)
    if m_card2:
        card_suffix = m_card2.group(1)

    if amount > 0:
        direction = "收入" if any(kw in body for kw in ["转入", "存入", "汇入"]) else "支出"
        return ParsedSMS(
            amount=amount, direction=direction,
            card_suffix=card_suffix,
            merchant="", counterparty="", bank_name="交通银行",
        )

    return None


# ══════════════════════════════════════════
#  建设银行 (106980095533 等含 95533 前缀的号码)
# ══════════════════════════════════════════

@register_bank("106980095533")
def _parse_ccb_via_long(body: str, number: str) -> Optional[ParsedSMS]:
    """建设银行 106980095533 长号码，转发到 95533 parser。"""
    return _parse_ccb(body, "95533")


@register_bank("95533")
def _parse_ccb(body: str, number: str) -> Optional[ParsedSMS]:
    """建设银行短信解析。

    建设银行使用多个号码：95533（客服）、106980095533（交易提醒）等。
    通过 body 中的 [建设银行] 或 龙卡信用卡/储蓄卡 识别。

    格式1（借记卡转账）：您尾号7939的储蓄卡MM月DD日HH时MM分向XXX跨行转出支出人民币X.XX元
    格式2（借记卡消费）：您尾号7939的储蓄卡MM月DD日HH时MM分消费支出人民币X.XX元
    格式3（借记卡存入）：XXX向您尾号7939的储蓄卡存入人民币X.XX元
    格式4（借记卡现金存入）：您尾号7939的储蓄卡MM月DD日HH时ATM存款收入人民币X.XX元
    格式5（借记卡支出）：您尾号7939的储蓄卡MM月DD日HH时MM分支出人民币X.XX元
    格式6（信用卡消费）：您尾号9170的龙卡信用卡MM月DD日HH:MM消费X.XX元
    格式7（信用卡还款）：您尾号9170龙卡信用卡MM月DD日HH:MM存入X.XX元
    格式8（信用卡还款-借记卡）：您尾号7939的储蓄卡MM月DD日HH时MM分向白晔峰信用卡卡号还款支出X.XX元
    """
    if "建设银行" not in body and "建行" not in body \
       and "龙卡" not in body and "储蓄卡" not in body:
        return None

    if _is_failure(body):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="建设银行",
                         _skip=True)

    # 跳过营销/广告类短信（预授信额度通知等）
    if any(kw in body for kw in ("预授信", "拒收请回复")):
        # 检查是否为转账/消费通知等实际交易
        is_transaction = any(kw in body for kw in ("转出", "消费", "存入", "收入", "支出", "还款"))
        if not is_transaction:
            return ParsedSMS(amount=0, direction="支出", card_suffix="",
                             merchant="", counterparty="", bank_name="建设银行",
                             _skip=True)

    # --- 格式8：借记卡 → 信用卡还款 ---
    m_card_repay = re.search(r"您尾号(\d{4})的储蓄卡.*?向白晔峰信用卡卡号还款支出人民币(\d+\.?\d*)元", body)
    if m_card_repay:
        return ParsedSMS(
            amount=float(m_card_repay.group(2)), direction="支出",
            card_suffix=m_card_repay.group(1),
            merchant="信用卡还款", counterparty="建设银行", bank_name="建设银行",
            tx_type_detail="transfer",  # 储蓄卡→信用卡，内部转账
        )

    # --- 格式3：存入 → 借记卡收入 ---
    m_deposit = re.search(r"向您尾号(\d{4})的储蓄卡存入人民币(\d+\.?\d*)元", body)
    if m_deposit:
        # 提取存款人
        depositor_m = re.search(r"^(.+?)\d+", body)
        depositor = ""
        if depositor_m:
            depositor = _strip_trailing(depositor_m.group(1).strip())
        return ParsedSMS(
            amount=float(m_deposit.group(2)), direction="收入",
            card_suffix=m_deposit.group(1),
            merchant="存入", counterparty=depositor or "", bank_name="建设银行",
            category="收入-转账", tx_type_detail="deposit",
        )

    # --- 格式4：现金/ATM存入 → 借记卡收入 ---
    m_cash_in = re.search(r"您尾号(\d{4})的储蓄卡.*?(?:ATM存款收入|现金存入收入)人民币(\d+\.?\d*)元", body)
    if m_cash_in:
        return ParsedSMS(
            amount=float(m_cash_in.group(2)), direction="收入",
            card_suffix=m_cash_in.group(1),
            merchant="现金存入", counterparty="", bank_name="建设银行",
            category="收入-其他", tx_type_detail="cash_deposit",
        )

    # --- 格式1：跨行转出 → 借记卡支出 ---
    m_transfer_out = re.search(r"您尾号(\d{4})的储蓄卡.*?向(.+?)跨行转出支出人民币(\d+\.?\d*)元", body)
    if m_transfer_out:
        return ParsedSMS(
            amount=float(m_transfer_out.group(3)), direction="支出",
            card_suffix=m_transfer_out.group(1),
            merchant="跨行转账",
            counterparty=_strip_trailing(m_transfer_out.group(2)),
            bank_name="建设银行",
            tx_type_detail="transfer_out",
        )

    # --- 格式2 + 格式5：消费支出 / 支出 ---
    m_expense = re.search(r"您尾号(\d{4})的储蓄卡.*?(?:消费支出|支出)人民币(\d+\.?\d*)元", body)
    if m_expense:
        amt = float(m_expense.group(2))
        # 判断是否贷款相关
        has_loan_kw = any(kw in body for kw in ("金融还款", "中邮", "消金", "消费金融", "宜享花"))
        return ParsedSMS(
            amount=amt, direction="支出",
            card_suffix=m_expense.group(1),
            merchant=_extract_merchant_from_ccb(body), counterparty="",
            bank_name="建设银行",
            is_loan=has_loan_kw,
            category="借贷-还款" if has_loan_kw else "未分类-其他",
            tx_type_detail="loan_repayment" if has_loan_kw else "consumption",
        )

    # --- 格式7：信用卡存入（还款入账） ---
    m_cc_in = re.search(r"您尾号(\d{4})(?:龙卡信用卡|信用卡).*?存入(\d+\.?\d*)元", body)
    if m_cc_in:
        return ParsedSMS(
            amount=float(m_cc_in.group(2)), direction="收入",
            card_suffix=m_cc_in.group(1),
            merchant="信用卡还款入账", counterparty="", bank_name="建设银行",
            tx_type_detail="repayment_in",
        )

    # --- 格式6：信用卡消费 ---
    m_cc_consume = re.search(r"您尾号(\d{4})(?:的)?(?:龙卡信用卡|信用卡).*?消费(\d+\.?\d*)元", body)
    if m_cc_consume:
        return ParsedSMS(
            amount=float(m_cc_consume.group(2)), direction="支出",
            card_suffix=m_cc_consume.group(1),
            merchant="信用卡消费", counterparty="", bank_name="建设银行",
            category="未分类-其他", tx_type_detail="consumption",
        )

    # 刷脸取款
    m_withdraw = re.search(r"您尾号(\d{4})的储蓄卡.*?刷脸取款支出人民币(\d+\.?\d*)元", body)
    if m_withdraw:
        return ParsedSMS(
            amount=float(m_withdraw.group(2)), direction="支出",
            card_suffix=m_withdraw.group(1),
            merchant="取款", counterparty="", bank_name="建设银行",
            category="未分类-其他", tx_type_detail="withdrawal",
        )

    return None


def _extract_merchant_from_ccb(body: str) -> str:
    """提取建设银行借记卡支出中的商户/用途。"""
    # 附言：XXX
    m = re.search(r"附言[：:]?\s*(.+?)(?:[。，.]|$)", body)
    if m:
        return _strip_trailing(m.group(1))
    # 在XXX之后
    m = re.search(r"在(.+?)(?:消费|支出|支付)", body)
    if m:
        return _strip_trailing(m.group(1))
    return ""


# ══════════════════════════════════════════
#  农业银行 (95599)
# ══════════════════════════════════════════

@register_bank("95599")
def _parse_abc(body: str, number: str) -> Optional[ParsedSMS]:
    """农业银行短信解析。

    格式1：您尾号8574账户MM月DD日HH:MM完成银联入账交易人民币X.XX，余额XXX
    格式2：您尾号8574账户MM月DD日HH:MM向XXX完成转支交易人民币-X.XX，余额XXX
    格式3：您尾号8574账户MM月DD日HH:MM完成XXX交易人民币X.XX，余额XXX
    格式4：XXX于MM月DD日HH:MM向您尾号8574账户完成转存交易人民币X.XX，余额XXX
    格式5：XXX于MM月DD日HH:MM向您尾号8574账户完成代付交易人民币X.XX，余额XXX
    格式6：您尾号8574账户MM月DD日HH:MM向XXX完成XXX交易人民币X.XX，余额XXX
    """
    if "中国农业银行" not in body and "农业银行" not in body:
        return None
    if _is_failure(body):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="农业银行",
                         _skip=True)

    card_suffix = ""
    m = re.search(r"尾号(\d{4})账户", body)
    if m:
        card_suffix = m.group(1)

    # 排除非交易类短信
    if "签约" in body or "验证码" in body:
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="农业银行",
                         _skip=True)

    # 跳过营销/广告类短信（储备金额度广告、拒收回复等）
    if any(kw in body for kw in ("储备金", "拒收请回复")):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="农业银行",
                         _skip=True)

    # --- 格式4 + 5：转存 / 代付 → 收入 ---
    m_in = re.search(r"向您尾号(\d{4})账户完成(?:转存|代付|代收)交易人民币(-?\d+\.?\d*)", body)
    if m_in:
        # 提取对方名
        counterparty_m = re.search(r"^(.+?)(?:于|向)", body)
        counterparty = _strip_trailing(counterparty_m.group(1)) if counterparty_m else ""
        return ParsedSMS(
            amount=abs(float(m_in.group(2))), direction="收入",
            card_suffix=m_in.group(1),
            merchant="代付入账" if "代付" in body else "转存入账",
            counterparty=counterparty, bank_name="农业银行",
            category="收入-转账", tx_type_detail="transfer_in",
        )

    # --- 格式1：银联入账/入账/奖金/代付 → 收入/支出 ---
    m_income = re.search(r"您尾号(\d{4})账户.*?完成(.+?)交易人民币(-?\d+\.?\d*)", body)
    if m_income:
        amt = abs(float(m_income.group(3)))
        txn_type = m_income.group(2)
        # 判断方向：含"向XXX完成"表示为转出
        if "向" in body and "完成" in body:
            direction = "支出"
            detail_type = "transfer_out"
        else:
            direction = "收入"
            detail_type = "salary" if "奖金" in txn_type else "income"
        return ParsedSMS(
            amount=amt, direction=direction,
            card_suffix=m_income.group(1),
            merchant=txn_type, counterparty="", bank_name="农业银行",
            category="收入-工资" if detail_type == "salary" else "未分类-其他",
            tx_type_detail=detail_type,
        )

    # --- 格式2 + 6：转支 / 支出 ---
    m_out = re.search(r"您尾号(\d{4})账户.*?向(.+?)完成(.+?)交易人民币(-?\d+\.?\d*)", body)
    if m_out:
        amt = abs(float(m_out.group(4)))
        counterparty = _strip_trailing(m_out.group(2))
        txn_type = m_out.group(3)
        has_loan_kw = any(kw in counterparty for kw in ("宜享花", "金融", "消金", "美团金融"))
        return ParsedSMS(
            amount=amt, direction="支出",
            card_suffix=m_out.group(1),
            merchant=txn_type, counterparty=counterparty,
            bank_name="农业银行",
            is_loan=has_loan_kw,
            category="借贷-还款" if has_loan_kw else "未分类-其他",
            tx_type_detail="loan_repayment" if has_loan_kw else "transfer_out",
        )

    # 兜底
    amount = _extract_amount(body)
    if amount > 0:
        direction = "收入" if "入账" in body or "转入" in body or "代付" in body else "支出"
        return ParsedSMS(
            amount=amount, direction=direction,
            card_suffix=card_suffix,
            merchant="", counterparty="", bank_name="农业银行",
        )

    return None


# ══════════════════════════════════════════
#  广发银行 (106980095508 / 95508)
# ══════════════════════════════════════════

@register_bank("95508")
def _parse_cgb(body: str, number: str) -> Optional[ParsedSMS]:
    """广发银行短信解析。

    格式1：您尾号7717信用卡DD日HH:MM还款人民币X.XX元（还款入账）
    格式2：您尾号7717广发卡DD日HH:MM消费人民币X.XX元
    格式3：您尾号7717广发卡DD日HH:MM消费人民币X.XX元，交易商户:XXX
    """
    if "广发银行" not in body and "广发卡" not in body:
        return None
    if _is_failure(body):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="广发银行",
                         _skip=True)

    # 排除非交易类短信（验证码等），保留还款/消费类
    skip_kw = ("验证码", "额度调升", "续卡", "激活", "申请未通过")
    if any(kw in body for kw in skip_kw):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="广发银行",
                         _skip=True)
    # 账单通知（不包含"已还清"或"还款"的账单类短信才是通知）
    if "账单" in body and "已还清" not in body and "还款" not in body:
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="广发银行",
                         _skip=True)
    if "到期" in body and "续卡" in body:
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="广发银行",
                         _skip=True)

    # --- 格式1：还款入账 ---
    m_repay = re.search(r"您尾号(\d{4})信用卡.*?还款人民币(\d+\.?\d*)元", body)
    if m_repay:
        return ParsedSMS(
            amount=float(m_repay.group(2)), direction="收入",
            card_suffix=m_repay.group(1),
            merchant="信用卡还款入账", counterparty="", bank_name="广发银行",
            tx_type_detail="repayment_in",
        )

    # --- 格式2+3：消费 ---
    m_consume = re.search(r"您尾号(\d{4})(?:广发卡|信用卡).*?消费人民币(\d+\.?\d*)元", body)
    if m_consume:
        merchant = ""
        m_merchant = re.search(r"交易商户[：:](.+?)$", body)
        if m_merchant:
            merchant = _strip_trailing(m_merchant.group(1))
        return ParsedSMS(
            amount=float(m_consume.group(2)), direction="支出",
            card_suffix=m_consume.group(1),
            merchant=merchant or "广发卡消费",
            counterparty=merchant or "", bank_name="广发银行",
            category="未分类-其他", tx_type_detail="consumption",
        )

    return None


# ── 956098? No, 106980095508 also comes from 广发 but different prefix ---
# 注册 10698 开头的广发号码
@register_bank("106980095508")
def _parse_cgb_long(body: str, number: str) -> Optional[ParsedSMS]:
    # 代理到 95508 parser
    return _parse_cgb(body, number)


# ══════════════════════════════════════════
#  光大银行 (95595)
# ══════════════════════════════════════════

@register_bank("95595")
def _parse_ceb(body: str, number: str) -> Optional[ParsedSMS]:
    """光大银行短信解析。

    格式1：您尾号3624的阳光标准信用卡在XXX境外网上支付交易X.XX美元
    格式2：您光大尾号2621的卡于MM月DD日消费XXX元
    格式3：尾号2621的绿色零碳信用卡可用额度不足（跳过）
    """
    if "光大银行" not in body:
        return None
    if _is_failure(body) or "可用额度不足" in body:
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="光大银行",
                         _skip=True)

    # 排除非交易
    if any(kw in body for kw in ("验证码", "感谢您的来电")):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name="光大银行",
                         _skip=True)

    # 格式1：境外网上支付
    m_foreign = re.search(r"您尾号(\d{4})[^\d]*在(.+?)境外网上支付交易(\d+\.?\d*)(?:美元|港币)", body)
    if m_foreign:
        return ParsedSMS(
            amount=float(m_foreign.group(3)), direction="支出",
            card_suffix=m_foreign.group(1),
            merchant=_strip_trailing(m_foreign.group(2)),
            counterparty="", bank_name="光大银行",
            tx_type_detail="consumption",
        )

    # 格式2：消费
    m_consume = re.search(r"您光大尾号(\d{4})的卡于.*?消费(\d+)元", body)
    if m_consume:
        return ParsedSMS(
            amount=float(m_consume.group(2)), direction="支出",
            card_suffix=m_consume.group(1),
            merchant="光大信用卡消费", counterparty="", bank_name="光大银行",
            tx_type_detail="consumption",
        )

    return None


# ══════════════════════════════════════════
#  浦发银行 (95528)
# ══════════════════════════════════════════

@register_bank("95528")
def _parse_spdb(body: str, number: str) -> Optional[ParsedSMS]:
    """浦发银行短信解析。"""
    if "浦发" not in body:
        return None
    # 目前只有账单和签约通知，无实际交易流水
    return ParsedSMS(amount=0, direction="支出", card_suffix="",
                     merchant="", counterparty="", bank_name="浦发银行",
                     _skip=True)


# ── 兜底解析器 ──

@register_fallback
def _parse_fallback(body: str, number: str) -> Optional[ParsedSMS]:
    """兜底：尝试从正文提取银行名、金额。

    适用于未注册的银行号码，使用通用模式匹配。
    """
    # 跳过验证码/营销类短信
    if any(kw in body for kw in ("验证码", "验证码为", "拒收请回复")):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name=_extract_body_org(body) or number,
                         _skip=True)

    if _is_failure(body):
        return ParsedSMS(amount=0, direction="支出", card_suffix="",
                         merchant="", counterparty="", bank_name=_extract_body_org(body) or number,
                         _skip=True)

    amount = _extract_amount(body)
    if amount <= 0:
        return None

    bank_name = _extract_body_org(body) or number
    card_suffix = ""
    m = re.search(r"[尾账户号][\*#]?(\d{4})", body)
    if m:
        card_suffix = m.group(1)

    _income_kw = ["收入", "入账", "存入", "转入", "汇款", "到账", "代付"]
    direction = "收入" if any(kw in body for kw in _income_kw) else "支出"

    return ParsedSMS(
        amount=amount, direction=direction,
        card_suffix=card_suffix,
        merchant="", counterparty="", bank_name=bank_name,
        tx_type_detail="unknown",
    )


# ── 主入口 ──


def dispatch(number: str, body: str, received: str = "") -> Optional[ParsedSMS]:
    """主入口：按 number 前缀分派到对应银行的解析器。

    参数：
        number: 短信发件人号码
        body:   短信正文
        received: 接收时间（ISO格式）

    返回：
        ParsedSMS | None（无法解析时）
    """
    for prefix, parser in _PARSERS:
        if not prefix:
            continue  # skip fallback in first pass
        if number.startswith(prefix):
            try:
                result = parser(body, number)
                if result is not None:
                    result.tx_time = received
                    result.source_text = body[:120]
                    return result
            except Exception as e:
                logger.warning(f"[{prefix}] 解析失败: {e}")
                continue

    # 所有注册 parser 都不匹配，尝试 fallback
    for prefix, parser in _PARSERS:
        if not prefix:
            try:
                result = parser(body, number)
                if result is not None:
                    result.tx_time = received
                    result.source_text = body[:120]
                    return result
            except Exception:
                pass

    return None


def parse_to_dict(result: ParsedSMS) -> dict:
    """将 ParsedSMS 转为 sms_finance 兼容的 dict 格式。"""
    if result is None:
        return {"amount": 0.0, "time": "", "_skip": True}

    return {
        "amount": result.amount,
        "time": result.tx_time,
        "direction": result.direction,
        "merchant": result.merchant,
        "payment_method": result.bank_name,
        "card_suffix": result.card_suffix,
        "category": result.category,
        "is_loan": result.is_loan,
        "source": "sms",
        "source_text": result.source_text,
        "_skip": result._skip,
        "tx_type_detail": result.tx_type_detail,
        "counterparty": result.counterparty,
    }
