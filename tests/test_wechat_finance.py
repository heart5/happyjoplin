import json
from pathlib import Path

import pytest

from life.wechat_finance import _normalize_merchant, parse_finance_messages

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "finance_messages.json")
    .read_text(encoding="utf-8"))


def _wechat_cases():
    return pytest.mark.parametrize(
        "case", FIXTURES["wechat_cases"], ids=lambda c: c["name"])


@_wechat_cases()
def test_wechat_fixture(case):
    messages = [{**m, "source": "fixture"} for m in case["messages"]]
    records = parse_finance_messages(messages)

    assert len(records) == len(case["expect"]), [
        (r["time"], r["amount"], r["merchant"]) for r in records]
    for record, expect in zip(records, case["expect"]):
        for key, value in expect.items():
            assert record.get(key) == value, (key, record.get(key), value, record)


def test_source_ids_merged_when_pairing():
    case = next(c for c in FIXTURES["wechat_cases"]
                if c["name"] == "meituan_pair_merged")
    messages = [{**m, "source": "fixture"} for m in case["messages"]]
    record = parse_finance_messages(messages)[0]
    assert sorted(record["source_ids"]) == [4, 5]


def test_refund_pair_keeps_receipt_fields():
    case = next(c for c in FIXTURES["wechat_cases"]
                if c["name"] == "refund_receipt_and_bank_notice")
    messages = [{**m, "source": "fixture"} for m in case["messages"]]
    record = parse_finance_messages(messages)[0]
    assert record["payment_method"] == "光大银行信用卡"


@pytest.mark.parametrize("raw,expected", [
    ("（特约）美团-美团App巧八角", "美团-美团App巧八角"),
    ("支付宝-支付宝-消费", "支付宝"),
    ("财付通-微信支付-扫二维码付款", "微信扫码"),
    ("财付通-二十四客", "二十四客"),
    ("", ""),
])
def test_normalize_merchant(raw, expected):
    assert _normalize_merchant(raw) == expected
