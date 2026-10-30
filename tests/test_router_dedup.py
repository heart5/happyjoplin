import json

import pytest

from life.ledger.accounts import AccountManager
from life.ledger.db import Database
from life.ledger.importers.router import FlowRouter
from life.ledger.models import AccountFlow


@pytest.fixture
def router(tmp_path):
    db = Database(str(tmp_path / "router.db"))
    row = db.fetchone(
        "SELECT id FROM accounts WHERE book='personal'"
        " AND type='credit_card' ORDER BY id LIMIT 1")
    if row is None:
        account_id = db.insert("accounts", {
            "book": "personal", "name": "测试信用卡", "type": "credit_card",
            "card_suffix": "8256", "identity_key": "test-cc-8256",
        })
    else:
        account_id = row["id"]
    return FlowRouter(db, AccountManager(db), book="personal"), account_id


def _flow(account_id, **kw):
    return AccountFlow(**{
        "book": "personal", "tx_date": "2026-10-22",
        "tx_time": "2026-10-22 11:59:30", "amount": 99.79,
        "account_id": account_id, "direction": "outflow",
        "tx_type": "consumption", "merchant": "中国电信", **kw})


def test_same_card_two_wechat_accounts_merged(router):
    r, account_id = router
    a = _flow(account_id, source="wechat_main", source_event_id="wxmain:1",
              raw_data=json.dumps({"wechat": "交易时间：10月22日 11时59分"}, ensure_ascii=False))
    b = _flow(account_id, source="wechat_work", source_event_id="wx2:2",
              raw_data=json.dumps({"wechat": "交易时间：10月22日 11时59分"}, ensure_ascii=False))
    out = r._merge_duplicate_flows([a, b])
    assert len(out) == 1
    assert sorted(out[0].source_event_id.split("|")) == ["wx2:2", "wxmain:1"]


def test_late_sms_merged_by_body_time(router):
    r, account_id = router
    a = _flow(account_id, source="wechat_main", source_event_id="wxmain:1",
              raw_data=json.dumps({"wechat": "交易时间：10月22日 11时59分"}, ensure_ascii=False))
    b = _flow(account_id, source="sms", source_event_id="sms:9",
              tx_time="2026-10-22 21:30:00",
              raw_data=json.dumps({"sms": "您尾号8256卡10月22日11:59消费人民币99.79元"}, ensure_ascii=False))
    out = r._merge_duplicate_flows([a, b])
    assert len(out) == 1
    assert out[0].source == "wechat_main"


def test_transfer_legs_not_merged(router):
    r, account_id = router
    a = _flow(account_id, source="wechat_main", tx_type="transfer")
    b = _flow(account_id, source="sms", tx_type="transfer")
    assert len(r._merge_duplicate_flows([a, b])) == 2


def test_different_amounts_not_merged(router):
    r, account_id = router
    a = _flow(account_id, source="wechat_main", amount=99.79)
    b = _flow(account_id, source="wechat_work", amount=89.79)
    assert len(r._merge_duplicate_flows([a, b])) == 2


def test_same_source_not_merged(router):
    r, account_id = router
    a = _flow(account_id, source="wechat_main", source_event_id="wxmain:1")
    b = _flow(account_id, source="wechat_main", source_event_id="wxmain:2")
    assert len(r._merge_duplicate_flows([a, b])) == 2
