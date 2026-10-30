from life.ledger.reports.credit_cards import _signals_from_body


def test_bocom_bill_statement():
    body = ("【交通银行信用卡】您尾号2349交行卡09/20-10/19账单："
            "应还款￥32074.55，最低还款￥15085.84，到期还款日11月13日")
    assert _signals_from_body(body) == {
        "minimum": 15085.84, "due_date": "11月13日"}


def test_ccb_remaining_due():
    body = ("【建设银行】截至10月19日，您的10月龙卡信用卡账单还需还款4715.65元，"
            "请您在到期还款日10月25日上午7点前将还款金额存入约定还款账户。")
    assert _signals_from_body(body) == {
        "remaining_due": 4715.65, "due_date": "10月25日"}


def test_citic_minimum_and_overdue():
    body = ("【中信银行信用卡】白晔峰先生：您尾号为7544的信用卡目前账户欠款"
            "人民币25954.42元，最低还款额为人民币2726.38元，目前已逾期。")
    assert _signals_from_body(body)["minimum"] == 2726.38


def test_bocom_remaining_after_repayment():
    body = ("您尾号2349信用卡还款人民币74.55元，当期剩余应还人民币32000.00元，"
            "可用额度人民币-2572.26元，详询买单吧APP。【交通银行】")
    assert _signals_from_body(body) == {
        "remaining_due": 32000.0, "available": -2572.26}
