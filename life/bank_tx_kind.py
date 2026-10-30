"""银行/服务号通知的交易类型词表。

银行短信与公众号交易的「交易类型」多为自由文本（工资交易、代付交易、
约定账户扣款、在支付宝-还款快捷支付……），按词判断方向与账本口径，
避免把工资、奖励记成消费。

优先级：退款 > 卡还款 > 手续费 > 收入 > 转账 > 消费。
"""

import re

__all__ = [
    "REFUND", "CARD_REPAYMENT", "FEE", "INCOME", "TRANSFER", "CONSUMPTION",
    "classify_tx_kind", "category_for_kind", "tx_detail_for_kind",
    "is_income_kind", "strip_txn_suffix",
]

REFUND = "refund"
CARD_REPAYMENT = "card_repayment"
FEE = "fee"
INCOME = "income"
TRANSFER = "transfer"
CONSUMPTION = "consumption"

_RULES = (
    (REFUND, ("退款", "退货", "冲正", "退税")),
    (CARD_REPAYMENT, (
        "约定账户扣款", "约定还款", "自动还款", "信用卡还款", "还款入账",
        "还款成功", "最低还款", "在支付宝-还款", "在财付通-还款", "还款支出",
        "一键还款", "还款",
    )),
    (FEE, ("手续费", "年费", "短信服务费", "账户费用")),
    (INCOME, (
        "工资", "奖金", "奖励", "绩效", "报销", "补贴", "利息", "收益",
        "入账", "代发", "分红", "差旅费",
    )),
    (TRANSFER, (
        "转账", "转存", "代付", "汇入", "汇出", "汇款", "提现", "转支",
        "转入", "转出", "收款",
    )),
    (CONSUMPTION, (
        "消费", "快捷支付", "POS", "pos", "扣款", "扣费", "支付", "支出",
    )),
)

# 词表命中后的默认分类；None 表示由调用方按原有规则决定
_CATEGORY = {
    REFUND: "收入-退款",
    FEE: "金融-手续费",
    INCOME: "收入-其他",
}

# 词表命中后的账本 detail（router 的 tx_type_detail）
_DETAIL = {
    REFUND: "income",
    CARD_REPAYMENT: "repayment_in",
    FEE: "fee",
    INCOME: "income",
    TRANSFER: "transfer_out",
    CONSUMPTION: "consumption",
}

_SALARY_KW = ("工资", "奖金", "奖励", "绩效", "补贴")
_RE_TXN_SUFFIX = re.compile(r"(?:交易|提醒|通知)$")


def classify_tx_kind(text: str) -> tuple:
    """返回 (kind, 命中的关键词)；无法判定时 (None, "")。"""
    if not text:
        return None, ""
    for kind, words in _RULES:
        for w in words:
            if w in text:
                return kind, w
    return None, ""


def is_income_kind(kind: str) -> bool:
    return kind in (INCOME, REFUND)


def strip_txn_suffix(text: str) -> str:
    """『12月工资交易』→『12月工资』。"""
    return _RE_TXN_SUFFIX.sub("", (text or "").strip()).strip()


def category_for_kind(kind: str, merchant: str = "") -> str:
    """词表口径对应的分类；工资/奖励类归入 收入-工资。"""
    if not kind:
        return ""
    if kind == INCOME and any(k in (merchant or "") for k in _SALARY_KW):
        return "收入-工资"
    return _CATEGORY.get(kind, "")


def tx_detail_for_kind(kind: str, direction: str = "支出") -> str:
    if kind == TRANSFER and direction == "收入":
        return "transfer_in"
    return _DETAIL.get(kind, "")
