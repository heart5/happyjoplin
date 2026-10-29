# 个人财务系统 — 手机通知数据源（HCX /inbox/notification/query）
#
# 通知只用于两类用途：
#   1. 高置信度交易通知 → 账本流水（source=notification），与短信按金额+尾号+时间窗去重
#   2. 还款/额度提醒 → notification_signals（只观察，不入账）

import hashlib
import json
import re
import ssl
import urllib.error
import urllib.request
from datetime import datetime

from .cloudcfg import get_notification_api_url
from .transactions import month_bounds
from life.sms_finance import _get_sms_api_key

__all__ = [
    "fetch_notifications",
    "parse_notification_records",
    "extract_signals",
    "EXCLUDED_PACKAGES",
]

_PAGE_SIZE = 500
_MAX_PAGES = 100
_NOTIF_DEDUP_WINDOW = 1800

EXCLUDED_PACKAGES = (
    "com.android.mms",          # 短信镜像
    "com.android.systemui",
    "com.termux",
    "com.termux.api",
    "com.twitter.android",
    "com.jingdong.",
    "com.jd.",
    "com.android.email",
    "com.google.android.gm",
)

_PACKAGE_BANKS = (
    ("com.chinamworld.main", "建设银行"),      # 建行手机银行
    ("com.chinamworld.bocmbci", "中国银行"),   # 中行手机银行
    ("com.chinamworld.", "中国银行"),
    ("com.citiccard.", "中信银行"),
    ("com.cmbchina.", "招商银行"),
    ("com.icbc.", "工商银行"),
    ("com.ccb.", "建设银行"),
    ("com.bankcomm.", "交通银行"),
    ("com.spdb.", "浦发银行"),
    ("com.pingan.", "平安银行"),
)

_LOAN_KW = ("经营贷", "借呗", "花呗", "白条", "月付", "借钱", "消费贷",
            "网商贷", "贷款", "分期乐", "微粒贷")
_MARKETING_KW = ("验证码", "动态码", "红包", "优惠券", "立减", "满减",
                 "积分", "抽奖", "签到", "退订")

_AMT = r"((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?)"

_RE_REPAY = (
    re.compile(rf"一笔\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元的还款"),
    re.compile(rf"收到还款\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元"),
    re.compile(rf"还款成功\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元"),
    re.compile(rf"成功还款\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元"),
    re.compile(rf"已还款\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元"),
)
_RE_OUTFLOW = re.compile(
    rf"(?:支出|消费|支付|扣款|转出|取现)\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元")
_RE_INFLOW = re.compile(
    rf"(?:入账|存入|转入|到账|收款|收入)\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元")
_RE_SUFFIX = re.compile(
    r"(?:尾号|尾号为|末四位|卡号末四位|卡号后四位|账户尾号)[^0-9]{0,3}(\d{4})")

_RE_AVAILABLE = re.compile(
    rf"(?:当前可用额度|可用额度|剩余可用额度)\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元?")
_RE_REMAINING = re.compile(
    rf"(?:剩余应还(?:金额)?|本期仍需还款|剩余未还|待还(?:金额)?|尚未还清|未还(?:金额)?)"
    rf"\s*(?:为)?\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元")
_RE_UNPAID = re.compile(rf"{_AMT}\s*元[^。；\n]{{0,12}}?(?:未还|待还)")
_RE_MINIMUM = re.compile(
    rf"最低还款(?:额|金额)?\s*(?:为)?\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元")
_RE_LIMIT = re.compile(
    rf"(?:信用额度|总额度)\s*(?:为)?\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元")
_RE_PAID = re.compile(
    rf"已还(?:金额|款)?\s*(?:人民币|￥|¥)?\s*{_AMT}\s*元")
_RE_DUE = re.compile(
    r"(?:到期还款日|最后还款日)(?:为|变为|是|：|:)?\s*(?:(\d{4})[-年])?\s*"
    r"(\d{1,2})月(\d{1,2})[日号]?")


def _md5(obj) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _norm_time(*candidates) -> str:
    for value in candidates:
        text = str(value or "").strip().replace("T", " ")
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text):
            return text[:19]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
            return text + " 00:00:00"
    return ""


def _epoch(text: str) -> float:
    try:
        return datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S").timestamp()
    except ValueError:
        return 0.0


def _amount(text: str) -> float:
    return float(text.replace(",", ""))


def _fetch_json(url: str, key: str) -> dict:
    req = urllib.request.Request(url, headers={"X-API-Key": key})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except ssl.SSLError:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
            return json.loads(resp.read().decode())


def _package_bank(package: str) -> str:
    for prefix, bank in _PACKAGE_BANKS:
        if package.startswith(prefix):
            return bank
    return ""


def _institution(rec: dict, package: str) -> str:
    text = f"{rec.get('title') or ''} {rec.get('content') or ''}"
    for kw in ("美团月付", "美团借钱", "抖音月付", "京东白条", "支付宝经营贷"):
        if kw in text:
            return kw
    if "经营贷" in text:
        return "网商银行经营贷"
    return _package_bank(package)


def _is_loan_text(text: str) -> bool:
    return any(kw in text for kw in _LOAN_KW)


def _excluded(rec: dict) -> bool:
    package = str(rec.get("package") or "")
    if any(package == p or package.startswith(p) for p in EXCLUDED_PACKAGES):
        return True
    if package == "com.tencent.mm":
        title = str(rec.get("title") or "")
        return not ("微信支付" in title or "微信收款助手" in title)
    return False


def _parse_event(rec: dict):
    if _excluded(rec):
        return None
    title = str(rec.get("title") or "")
    content = str(rec.get("content") or "")
    text = f"{title} {content}".strip()
    if not text:
        return None
    if any(kw in text for kw in _MARKETING_KW):
        return None

    package = str(rec.get("package") or "")
    bank = _package_bank(package)
    institution = _institution(rec, package)
    amount = None
    detail = ""
    category = "未分类-其他"
    direction = ""
    is_loan = False

    for pattern in _RE_REPAY:
        m = pattern.search(content)
        if m:
            amount = _amount(m.group(1))
            break
    if amount is not None:
        is_credit_card = ("信用卡" in text or "消费卡" in text
                          or package.startswith(("com.citiccard.",
                                                 "com.cmbchina.ccd")))
        if is_credit_card:
            detail = "repayment_in"
            direction = "收入"
            category = "内部-转账"
        elif _is_loan_text(text):
            is_loan = True
            direction = "支出"
            category = "借贷-还款"
        else:
            return None
    else:
        m = _RE_OUTFLOW.search(content)
        if m:
            amount = _amount(m.group(1))
            direction = "支出"
            category = "借贷-还款" if _is_loan_text(text) else "未分类-其他"
            is_loan = _is_loan_text(text)
        else:
            m = _RE_INFLOW.search(content)
            if m:
                amount = _amount(m.group(1))
                direction = "收入"
                category = "收入-其他"

    if not amount or amount <= 0 or not direction:
        # 还款成功但未给出本次金额（如“已最低还款成功 剩余应还631.16元”）：
        # 记零额线索，由账本按平台+时间匹配同笔银行卡扣款
        if not (_is_loan_text(text) and "还款成功" in text):
            return None
        amount = 0.0
        direction = "支出"
        category = "借贷-还款"
        is_loan = True
        detail = "repayment_hint"
    if not bank and not institution:
        return None

    tx_time = _norm_time(rec.get("when"), rec.get("updated_at"),
                         rec.get("uploaded_at"))
    if not tx_time:
        return None

    tail = _RE_SUFFIX.search(text)
    card_suffix = tail.group(1) if tail else ""
    payment_method = bank or institution
    if is_loan and package.startswith("com.eg.android.AlipayGphone"):
        # 支付宝经营贷只记贷款腿，不推断从哪张银行卡扣款
        payment_method = "支付宝"

    if detail == "repayment_in":
        merchant = "信用卡还款入账"
    elif is_loan:
        merchant = institution
    else:
        merchant = f"{payment_method}动账"

    sid = f"notif:{rec['id']}" if rec.get("id") is not None else \
        "notif:h" + hashlib.sha1(
            f"{rec.get('content_hash')}|{text}".encode()).hexdigest()[:16]

    return {
        "time": tx_time,
        "amount": round(amount, 2),
        "direction": direction,
        "merchant": merchant,
        "category": category,
        "payment_method": payment_method,
        "card_suffix": card_suffix,
        "source_text": text[:1000],
        "source_event_id": sid,
        "is_loan": is_loan,
        "tx_type_detail": detail,
        "counterparty": payment_method,
        "source": "repayment_hint" if detail == "repayment_hint" else "",
    }


def _dedup_events(events: list) -> list:
    events = sorted(
        events,
        key=lambda e: (-(1 if e.get("card_suffix") else 0),
                       -len(e.get("source_text") or ""),
                       e.get("time") or ""),
    )
    kept = []
    for evt in events:
        if evt.get("source") == "repayment_hint":
            kept.append(evt)
            continue
        duplicate = False
        for other in kept:
            if abs(evt["amount"] - other["amount"]) > 0.01:
                continue
            if evt["direction"] != other["direction"]:
                continue
            if (evt.get("card_suffix") and other.get("card_suffix")
                    and evt["card_suffix"] != other["card_suffix"]):
                continue
            if abs(_epoch(evt["time"]) - _epoch(other["time"])) > _NOTIF_DEDUP_WINDOW:
                continue
            duplicate = True
            break
        if not duplicate:
            kept.append(evt)
    kept.sort(key=lambda e: e.get("time") or "")
    return kept


def parse_notification_records(records: list) -> list:
    events = []
    for rec in records:
        evt = _parse_event(rec)
        if evt:
            events.append(evt)
    return _dedup_events(events)


def _due_date(text: str, observed_at: str):
    m = _RE_DUE.search(text)
    if not m:
        return None
    year_str, month_str, day_str = m.group(1), m.group(2), m.group(3)
    month, day = int(month_str), int(day_str)
    if not 1 <= month <= 12 or not 1 <= day <= 31:
        return None
    try:
        observed = datetime.strptime(observed_at[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        observed = datetime.now()
    year = int(year_str) if year_str else observed.year
    if not year_str and month < observed.month:
        year += 1
    return f"{year:04d}-{month:02d}-{day:02d}"


def _make_signal(rec: dict, signal_type: str, amount=None, value_text=""):
    text = f"{rec.get('title') or ''} {rec.get('content') or ''}".strip()
    observed_at = _norm_time(rec.get("when"), rec.get("updated_at"),
                             rec.get("uploaded_at"))
    package = str(rec.get("package") or "")
    tail = _RE_SUFFIX.search(text)
    source_event_id = (f"notif:{rec['id']}" if rec.get("id") is not None
                       else "notif:h" + hashlib.sha1(
                           f"{rec.get('content_hash')}|{text}".encode()).hexdigest()[:16])
    return {
        "book": "personal",
        "source_event_id": source_event_id,
        "notif_id": rec.get("id"),
        "device": rec.get("device"),
        "package": package,
        "bank": _institution(rec, package),
        "card_suffix": tail.group(1) if tail else "",
        "signal_type": signal_type,
        "amount": amount,
        "value_text": value_text,
        "content_hash": rec.get("content_hash") or "",
        "observed_at": observed_at,
        "raw_text": text[:500],
    }


def extract_signals(records: list) -> list:
    signals = []
    for rec in records:
        if _excluded(rec):
            continue
        content = str(rec.get("content") or "")
        text = f"{rec.get('title') or ''} {content}".strip()
        if not text:
            continue

        # 部分 App 把金额放标题（如美团月付），信号读取标题+正文
        for pattern, kind in ((_RE_AVAILABLE, "available_credit"),
                              (_RE_MINIMUM, "minimum_payment"),
                              (_RE_LIMIT, "credit_limit"),
                              (_RE_PAID, "paid_amount")):
            m = pattern.search(text)
            if m:
                signals.append(_make_signal(rec, kind, _amount(m.group(1))))

        m = _RE_REMAINING.search(text) or _RE_UNPAID.search(text)
        if m:
            signals.append(_make_signal(rec, "remaining_due", _amount(m.group(1))))

        observed = _norm_time(rec.get("when"), rec.get("updated_at"),
                              rec.get("uploaded_at"))
        due = _due_date(text, observed)
        if due:
            signals.append(_make_signal(rec, "due_date", value_text=due))
    return signals


def fetch_notifications(month_key: str) -> tuple:
    """返回 (交易事件, 原始条数, digest, 信号)。"""
    start, end = month_bounds(month_key)
    base = get_notification_api_url()
    key = _get_sms_api_key()
    if not key:
        raise RuntimeError("SMS API 密钥未配置（[sms_collector] api_key）")

    records = []
    after_id = 0
    for _ in range(_MAX_PAGES):
        url = (f"{base}?kind=finance&date_from={start}&date_to={end}"
               f"&limit={_PAGE_SIZE}")
        if after_id:
            url += f"&after_id={after_id}"
        try:
            data = _fetch_json(url, key)
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"通知 API HTTP {e.code}") from e
        except Exception as e:
            raise RuntimeError(f"通知 API 请求失败：{e}") from e
        page = data.get("records", [])
        records.extend(page)
        if len(page) < _PAGE_SIZE:
            break
        next_after = max(int(r.get("id") or 0) for r in page)
        if next_after <= after_id:
            break
        after_id = next_after

    digest = _md5([
        (r.get("id"), r.get("device"), r.get("package"), r.get("title"),
         r.get("content"), r.get("lines"), r.get("when"),
         r.get("content_hash"))
        for r in records
    ])
    return (parse_notification_records(records), len(records), digest,
            extract_signals(records))
