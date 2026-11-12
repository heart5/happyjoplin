# 微信支付官方账单（xlsx）→ 账本事件
#
# 来源：微信 App「我 → 服务 → 钱包 → 账单 → 常见问题 → 下载账单」，导出为 xlsx。
# 表头在第 17 行左右（前面是昵称/时间范围/汇总/注），11 列：
#   交易时间,交易类型,交易对方,商品,收/支,金额(元),支付方式,当前状态,交易单号,商户单号,备注
#
# 用途：工作号（wx2）聊天记录只能看到「有推送回执」的支付，官方账单才是全量。
# 实测 2024-01~2026-11：账单 515 笔，而工作账套只有 249 笔。
#
# 口径（2026-11-12 接入）：
#   * 「支出 / 收入」入账；「/」是中性交易（零钱充值/提现/零钱通存取/司法冻结），
#     属账户内部搬运，入账会重复计算，跳过。
#   * 资金渠道按「支付方式」判定：
#       零钱 / 零钱通        → 工作账套的零钱（微信账单里工作号零钱付款）
#       各银行/信用卡(尾号)  → 归**个人账套**（与现有设计一致：工作号卡付并入个人）
#     因此银行卡付款的行不会重复进工作账套，而是交给个人账套的去重链处理。
#   * 退款在账单里是独立的「收入」行（类型含「-退款」，或状态「已退款/已全额退款」），
#     记 `收入-退款`，月报会冲减消费 —— 否则支出保留原额会虚增消费。
#   * 「已全额退款」的支出行仍然入账（保留当时的支出），由同额退款行冲减，
#     与既有口径一致（不丢时间线）。

import glob
import hashlib
import re
from datetime import datetime
from pathlib import Path

__all__ = ["parse_wechat_bill_dir", "parse_wechat_bill_files",
           "method_hint", "is_refund"]

_SKIP_DIRECTIONS = {"/", ""}
_BAD_STATUS = {"冻结中", "已撤销"}

_RE_SUFFIX = re.compile(r"\((\d{4})\)")
_RE_REFUND_STATUS = re.compile(r"已退款.*?¥?(\d+\.?\d*)?")

# 交易类型 → 账本分类（未命中再由商户/商品关键词细化）
_TYPE_CATEGORY = {
    "商户消费": "未分类-其他",
    "扫二维码付款": "未分类-其他",
    "转账": "社交-转账",
    "信用卡还款": "内部-转账",
    "零钱提现": None,          # 中性，不会走到这里
    "零钱充值": None,
    "微信红包（单发）": "社交-红包",
    "微信红包（群红包）": "社交-红包",
    "微信红包": "社交-红包",
    "企业微信红包": "社交-红包",
}

_DETAIL_RULES = (
    ("出行", "交通-网约车"),
    ("打车", "交通-网约车"),
    ("地铁", "交通-公共交通"),
    ("公交", "交通-公共交通"),
    ("单车", "交通-公共交通"),
    ("加油", "交通-加油"),
    ("充电", "交通-充电"),
    ("停车", "交通-其他"),
    ("话费", "通讯-话费"),
    ("流量", "通讯-话费"),
    ("电费", "居住-水电"),
    ("水费", "居住-水电"),
    ("燃气", "居住-燃气"),
    ("物业", "居住-物业"),
    ("房租", "居住-房租"),
    ("药", "医疗-药品"),
    ("医院", "医疗-就诊"),
    ("体检", "医疗-就诊"),
    ("便利店", "购物-便利店"),
    ("超市", "购物-超市"),
    ("咖啡", "餐饮-饮品"),
    ("奶茶", "餐饮-饮品"),
    ("餐", "餐饮-正餐"),
    ("饭", "餐饮-正餐"),
    ("美食", "餐饮-正餐"),
    ("酒店", "住宿-酒店"),
    ("客栈", "住宿-酒店"),
    ("会员", "娱乐-其他"),
    ("电影", "娱乐-其他"),
    ("速运", "交通-其他"),
    ("快递", "交通-其他"),
    ("物流", "交通-其他"),
    ("水电", "居住-水电"),
    ("烟酒", "购物-便利店"),
    ("酒业", "购物-便利店"),
    ("伴手礼", "购物-其他"),
    ("包子", "餐饮-正餐"),
    ("豆花", "餐饮-正餐"),
    ("烤鱼", "餐饮-正餐"),
    ("米粉", "餐饮-正餐"),
    ("面馆", "餐饮-正餐"),
    ("火锅", "餐饮-正餐"),
    ("深度求索", "技术-云服务"),
    ("凯辉", "购物-便利店"),
    ("便利店", "购物-便利店"),
)


def method_hint(payment_method: str) -> dict:
    """支付方式 → 资金渠道。

    kind: wallet（工作号零钱/零钱通，入工作账套）
          bank / credit_card（归个人账套）
          unknown
    """
    raw = (payment_method or "").strip()
    if raw in ("零钱", "零钱通", "零钱通(零钱)", ""):
        return {"kind": "wallet", "name": "工作号微信零钱", "suffix": "",
                "raw": raw}
    mm = _RE_SUFFIX.search(raw)
    suffix = mm.group(1) if mm else ""
    name = _RE_SUFFIX.sub("", raw).strip()
    if "信用卡" in name:
        return {"kind": "credit_card", "name": name, "suffix": suffix, "raw": raw}
    if "储蓄卡" in name or "银行" in name:
        return {"kind": "bank", "name": name, "suffix": suffix, "raw": raw}
    return {"kind": "unknown", "name": name or raw, "suffix": suffix, "raw": raw}


def is_refund(row: dict) -> bool:
    ttype = row.get("交易类型") or ""
    status = str(row.get("当前状态") or "")
    if "退款" in ttype:
        return True
    return "退款" in status


def _category_for(row: dict, direction: str) -> str:
    ttype = row.get("交易类型") or ""
    goods = " ".join(str(row.get(k) or "") for k in ("商品", "交易对方", "备注"))
    if is_refund(row) and direction == "收入":
        return "收入-退款"
    if ttype == "信用卡还款":
        return "内部-转账"
    if "红包" in ttype:
        return "社交-红包"
    if ttype == "转账":
        return "社交-转账"
    for kw, cat in _DETAIL_RULES:
        if kw in goods:
            return cat
    return _TYPE_CATEGORY.get(ttype) or "未分类-其他"


def _tx_type_detail(row: dict, direction: str) -> str:
    ttype = row.get("交易类型") or ""
    if is_refund(row) and direction == "收入":
        return "income"
    if ttype == "信用卡还款":
        return "repayment_out" if direction == "支出" else "repayment_in"
    if ttype == "转账" or "红包" in ttype:
        return "transfer_out" if direction == "支出" else "transfer_in"
    return "consumption" if direction == "支出" else "income"


def _clean_row(d: dict) -> dict:
    return {(str(k).strip() if k is not None else ""):
            (v.strip() if isinstance(v, str) else v) for k, v in d.items()}


def _iter_rows(path: str):
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        data = list(wb.active.iter_rows(values_only=True))
    finally:
        wb.close()
    hi = None
    for i, r in enumerate(data):
        if r and str(r[0]).strip() == "交易时间":
            hi = i
            break
    if hi is None:
        return
    hdr = [str(x).strip() if x is not None else "" for x in data[hi]]
    for r in data[hi + 1:]:
        if not r or r[0] is None:
            continue
        if not isinstance(r[0], datetime):
            continue
        yield _clean_row(dict(zip(hdr, r)))


def parse_wechat_bill_files(paths: list) -> list:
    """账单 xlsx 列表 → 账本事件（已按 orders 去重）。"""
    events = []
    seen = set()
    for p in paths:
        for row in _iter_rows(p):
            direction_ch = str(row.get("收/支") or "").strip()
            if direction_ch in _SKIP_DIRECTIONS:
                continue
            if direction_ch not in ("支出", "收入"):
                continue
            if str(row.get("当前状态") or "").strip() in _BAD_STATUS:
                continue
            try:
                amount = float(row.get("金额(元)") or 0)
            except (TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            tx_time = row.get("交易时间")
            if not isinstance(tx_time, datetime):
                continue

            order = str(row.get("交易单号") or "").strip()
            sid = order or hashlib.sha1(
                f"{tx_time}|{amount:.2f}|{row.get('交易对方')}".encode()
            ).hexdigest()
            if sid in seen:
                continue
            seen.add(sid)

            hint = method_hint(row.get("支付方式"))
            if hint["kind"] == "unknown" and direction_ch == "收入":
                # 收款类账单「支付方式」为 /，钱进工作号零钱
                hint = {"kind": "wallet", "name": "工作号微信零钱",
                        "suffix": "", "raw": row.get("支付方式") or "/"}
            cat = _category_for(row, direction_ch)
            merchant = str(row.get("交易对方") or "").strip() or \
                str(row.get("商品") or "").strip()

            events.append({
                "time": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
                "amount": round(amount, 2),
                "direction": direction_ch,
                "merchant": merchant,
                "category": cat,
                "payment_method": hint["name"],
                "card_suffix": hint["suffix"],
                "source": "wechat_bill",
                "source_event_id": f"wcbill:{sid}",
                "tx_type_detail": _tx_type_detail(row, direction_ch),
                "counterparty": merchant,
                "source_text": " | ".join(str(row.get(k) or "") for k in
                                           ("交易类型", "商品", "支付方式")
                                           if row.get(k)),
                "_wechat_bill": hint,
                "raw": {
                    "交易时间": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "交易类型": row.get("交易类型"),
                    "交易对方": row.get("交易对方"),
                    "商品": row.get("商品"),
                    "支付方式": row.get("支付方式"),
                    "当前状态": row.get("当前状态"),
                    "_渠道": hint["kind"],
                },
            })
    events.sort(key=lambda e: e["time"])
    return events


def parse_wechat_bill_dir(dir_path: str) -> tuple:
    """目录下所有 xlsx → (事件, 原始行数, md5)。"""
    d = Path(dir_path)
    if not d.is_dir():
        raise RuntimeError(f"微信账单目录不存在：{d}")
    paths = sorted(glob.glob(str(d / "*.xlsx")))
    if not paths:
        raise RuntimeError(f"微信账单目录没有 xlsx：{d}")
    events = parse_wechat_bill_files(paths)
    digest = hashlib.md5(
        "|".join(f"{e['source_event_id']}:{e['amount']}:{e['time']}"
                 for e in events).encode()
    ).hexdigest()
    return events, len(events), digest
