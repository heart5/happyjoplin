from life.bank_tx_kind import (
    CARD_REPAYMENT, CONSUMPTION, INCOME, REFUND, TRANSFER,
    category_for_kind, classify_tx_kind, strip_txn_suffix,
    tx_detail_for_kind,
)


def test_income_keywords():
    assert classify_tx_kind("12月工资")[0] == INCOME
    assert classify_tx_kind("代发")[0] == INCOME
    assert classify_tx_kind("差旅费")[0] == INCOME


def test_card_repayment_keywords():
    assert classify_tx_kind("约定账户扣款")[0] == CARD_REPAYMENT
    assert classify_tx_kind("尾号2133信用卡还款")[0] == CARD_REPAYMENT
    assert classify_tx_kind("在支付宝-还款快捷支付")[0] == CARD_REPAYMENT


def test_transfer_and_consumption():
    assert classify_tx_kind("转支")[0] == TRANSFER
    assert classify_tx_kind("平安代付")[0] == TRANSFER
    assert classify_tx_kind("消费")[0] == CONSUMPTION


def test_priority_refund_over_consumption():
    kind, kw = classify_tx_kind("退款 消费")
    assert (kind, kw) == (REFUND, "退款")


def test_priority_repayment_over_income():
    # “还款入账”必须先判为卡还款，不能被“入账”抢成收入
    assert classify_tx_kind("还款入账")[0] == CARD_REPAYMENT


def test_unknown_text():
    assert classify_tx_kind("") == (None, "")
    assert classify_tx_kind("请查收账单") == (None, "")


def test_category_for_kind():
    assert category_for_kind(INCOME, "12月工资") == "收入-工资"
    assert category_for_kind(INCOME, "奖金发放") == "收入-工资"
    assert category_for_kind(INCOME, "差旅费") == "收入-其他"
    assert category_for_kind(REFUND) == "收入-退款"
    assert category_for_kind(TRANSFER) == ""


def test_detail_for_kind():
    assert tx_detail_for_kind(TRANSFER, "收入") == "transfer_in"
    assert tx_detail_for_kind(TRANSFER, "支出") == "transfer_out"
    assert tx_detail_for_kind(CARD_REPAYMENT) == "repayment_in"


def test_strip_txn_suffix():
    assert strip_txn_suffix("12月工资交易") == "12月工资"
    assert strip_txn_suffix("交易提醒") == "交易"
