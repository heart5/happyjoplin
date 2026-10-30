import json
from pathlib import Path

import pytest

from life.sms_bank_parsers import dispatch, parse_to_dict

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "finance_messages.json")
    .read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", FIXTURES["sms_cases"], ids=lambda c: c["name"])
def test_sms_fixture(case):
    parsed = parse_to_dict(dispatch(case["number"], case["body"],
                                    case.get("received", "")))
    for key, value in case["expect"].items():
        assert parsed.get(key) == value, (key, parsed.get(key), value, parsed)


def test_unrelated_sender_skipped():
    parsed = parse_to_dict(dispatch("13800000000", "晚上一起吃饭", ""))
    assert parsed["_skip"] is True
