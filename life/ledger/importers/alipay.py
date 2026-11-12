# 支付宝官方交易明细（CSV）→ 账本事件
#
# 数据来源：支付宝 App「交易明细」导出的 CSV（UTF-8 BOM，10 列）：
#   交易时间,交易分类,交易对方,商品说明,收/支,金额,收/付款方式,交易状态,交易订单号,商家订单号
#
# 口径约定（2026-11-12 接入）：
#   * 只取「支出 / 收入」；「不计收支」是内部流转（余额宝转入转出、花呗还款、
#     信用借还、投资理财等），入账会与银行卡短信/微信回执双计，故跳过。
#   * 交易关闭 / 还款失败 / 未领取已退回 不是真实动账，跳过。
#   * 付款方式决定资金账户：
#       花呗           → 花呗（loan，消费当下就能看到，过去完全缺失）
#       账户余额/余额宝 → 支付宝（payment 钱包）
#       招商银行储蓄卡(9929) → 按 银行+尾号 匹配/新建账户
#     银行卡付款的行会与银行短信/微信回执重复，由 router 的同卡同额配对去重。

import csv
import hashlib
import re
from pathlib import Path

__all__ = ["parse_alipay_csv", "parse_alipay_records", "method_account_hint"]

_SKIP_STATUS = {"交易关闭", "还款失败", "未领取，已退回"}
_KEEP_DIRECTIONS = {"支出", "收入"}

_RE_SUFFIX = re.compile(r"\((\d{4})\)")
_RE_FULL_SUFFIX = re.compile(r"（(\d{4})）")

# 支付宝交易分类 → 账本分类
_CATEGORY_MAP = {
    "餐饮美食": "餐饮-正餐",
    "交通出行": "交通-其他",
    "日用百货": "购物-其他",
    "服饰装扮": "购物-其他",
    "充值缴费": "通讯-话费",
    "酒店旅游": "住宿-酒店",
    "医疗健康": "医疗-就诊",
    "文化休闲": "娱乐-其他",
    "爱车养车": "交通-加油",
    "美容美发": "娱乐-其他",
    "商业服务": "金融-其他",
    "转账红包": "社交-转账",
    "保险": "保险-人寿",
    "生活服务": "娱乐-其他",
    "亲友代付": "亲友代付",
    "其他": "未分类-其他",
}

# 商品说明里的更细线索（优先于交易分类，按顺序命中即止）
_DETAIL_RULES = (
    ("外卖", "餐饮-外卖"),
    ("打车", "交通-网约车"),
    ("滴滴", "交通-网约车"),
    ("地铁", "交通-公共交通"),
    ("公交", "交通-公共交通"),
    ("单车", "交通-公共交通"),
    ("充电", "交通-充电"),
    ("加油", "交通-加油"),
    ("停车", "交通-其他"),
    ("话费", "通讯-话费"),
    ("电费", "居住-水电"),
    ("水费", "居住-水电"),
    ("燃气", "居住-燃气"),
    ("物业", "居住-物业"),
    ("房租", "居住-房租"),
    ("药", "医疗-药品"),
    ("医院", "医疗-就诊"),
    ("便利店", "购物-便利店"),
    ("超市", "购物-超市"),
    ("咖啡", "餐饮-饮品"),
    ("奶茶", "餐饮-饮品"),
    ("会员", "娱乐-其他"),
    ("话费充值", "通讯-话费"),
)

_LOAN_METHODS = ("花呗", "借呗")
_WALLET_METHODS = ("账户余额", "余额宝", "余额", "")


def _clean(row: dict) -> dict:
    return {(k or "").strip(): (v or "").strip() for k, v in row.items()}


def method_account_hint(method: str) -> dict:
    """把「收/付款方式」拆成资金账户提示。

    返回 {"kind": loan|payment|bank|credit_card, "name": 银行/平台名,
          "suffix": 卡尾号, "raw": 原文}
    """
    raw = (method or "").strip()
    m = raw.split("&")[0].strip()          # 去掉「&花呗立减」这类后缀
    mm = _RE_SUFFIX.search(m) or _RE_FULL_SUFFIX.search(m)
    suffix = mm.group(1) if mm else ""
    name = _RE_SUFFIX.sub("", _RE_FULL_SUFFIX.sub("", m)).strip()

    if name.startswith(_LOAN_METHODS):
        return {"kind": "loan", "name": "花呗", "suffix": "", "raw": raw}
    if name in _WALLET_METHODS or not name:
        return {"kind": "payment", "name": "支付宝", "suffix": "", "raw": raw}
    if suffix:
        kind = "credit_card" if "信用卡" in name else "bank"
        return {"kind": kind, "name": name, "suffix": suffix, "raw": raw}
    return {"kind": "payment", "name": "支付宝", "suffix": "", "raw": raw}


def _category_for(tx_cat: str, desc: str, direction: str = "支出") -> str:
    # 信用借还的「支出」是还给贷款平台的还款（花呗还款本身是「不计收支」已跳过）
    if tx_cat == "信用借还" and direction == "支出":
        return "借贷-还款"
    # 亲情卡：家人用我的卡消费，钱确实从我的账户出，按「亲友代付」单列
    if tx_cat == "亲友代付":
        return "亲友代付"
    text = f"{tx_cat} {desc}"
    for kw, cat in _DETAIL_RULES:
        if kw in text:
            return cat
    return _CATEGORY_MAP.get(tx_cat, "未分类-其他")


def parse_alipay_records(rows: list) -> list:
    """CSV 行（已清洗）→ 路由事件。"""
    events = []
    seen = set()
    for row in rows:
        direction_ch = row.get("收/支") or ""
        if direction_ch not in _KEEP_DIRECTIONS:
            continue
        if (row.get("交易状态") or "") in _SKIP_STATUS:
            continue
        try:
            amount = float(row.get("金额") or 0)
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        tx_time = (row.get("交易时间") or "").strip()
        if len(tx_time) < 10:
            continue

        order_id = (row.get("交易订单号") or "").strip()
        merchant = (row.get("交易对方") or "").strip()
        desc = (row.get("商品说明") or "").strip()
        hint = method_account_hint(row.get("收/付款方式") or "")

        # 事件 id：优先支付宝订单号（跨设备/重复导出稳定）
        sid = order_id or hashlib.sha1(
            f"{tx_time}|{amount:.2f}|{merchant}|{desc}".encode()
        ).hexdigest()
        if sid in seen:
            continue
        seen.add(sid)

        direction = direction_ch
        cat = _category_for(row.get("交易分类") or "", desc, direction)
        if cat == "借贷-还款":
            detail = "loan_repayment"
        elif cat in ("社交-转账",):
            detail = "transfer_out" if direction == "支出" else "transfer_in"
        elif direction == "支出":
            detail = "consumption"
        else:
            detail = "income"
        events.append({
            "time": tx_time[:19],
            "amount": round(amount, 2),
            "direction": direction,
            "merchant": merchant or desc,
            "category": cat,
            "payment_method": hint["name"],
            "card_suffix": hint["suffix"],
            "source": "alipay",
            "source_event_id": f"alipay:{sid}",
            "tx_type_detail": detail,
            "counterparty": merchant,
            "source_text": " | ".join(x for x in (row.get("交易分类"), desc,
                                                   row.get("收/付款方式")) if x),
            "_alipay": hint,
            "raw": {
                "交易时间": tx_time, "交易分类": row.get("交易分类"),
                "交易对方": merchant, "商品说明": desc,
                "收/付款方式": row.get("收/付款方式"),
                "交易状态": row.get("交易状态"),
                "_渠道": hint["kind"],
            },
        })
    events.sort(key=lambda e: e["time"])
    return events


def parse_alipay_csv(path: str) -> tuple:
    """读 CSV → (事件列表, 原始行数, 内容 md5)。

    md5 只基于参与入账的行（不影响其他行的编辑不会触发重建）。
    """
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"支付宝明细不存在：{p}")
    rows = []
    with open(p, encoding="utf-8-sig", newline="") as f:
        for raw in csv.DictReader(f):
            rows.append(_clean(raw))
    events = parse_alipay_records(rows)
    digest = hashlib.md5(
        "|".join(f"{e['source_event_id']}:{e['amount']}:{e['time']}"
                 for e in events).encode()
    ).hexdigest()
    return events, len(rows), digest
