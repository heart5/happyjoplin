# 个人财务系统 — 信用卡状态报告

import json
import re
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta

from ..db import Database
from ..transactions import TransactionManager
from .common import money, now_text

__all__ = ["generate_credit_cards_report"]

_RE_TAIL = re.compile(r"尾号\s*(\d{4})")
_RE_AVAILABLE = re.compile(
    r"当前可用额度(?:人民币)?\s*(-?\d+(?:\.\d+)?)\s*元")
_RE_OVER = re.compile(r"已超过信用额度\s*(\d+(?:\.\d+)?)\s*元")


def _recent_months(anchor: datetime, count: int = 3) -> list:
    out = []
    y, m = anchor.year, anchor.month
    for _ in range(count):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return out


def _available_credit(days: int = 45) -> dict:
    """从最近短信提取各卡最新可用额度（尽力而为，失败返回空）。"""
    try:
        from ..cloudcfg import get_sms_api_url
        from life.sms_finance import _get_sms_api_key

        key = _get_sms_api_key()
        if not key:
            return {}
        start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        end = datetime.now().strftime("%Y-%m-%d")
        url = (f"{get_sms_api_url()}?date_from={start}&date_to={end}"
               "&limit=50000&kind=finance&merge=1")
        req = urllib.request.Request(url, headers={"X-API-Key": key})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except Exception:
        return {}

    latest = {}
    over = {}
    for msg in sorted(data.get("records", []),
                      key=lambda m: m.get("received") or "", reverse=True):
        body = msg.get("body") or ""
        tail = _RE_TAIL.search(body)
        if not tail:
            continue
        suffix = tail.group(1)
        if "可用额度" in body:
            m = _RE_AVAILABLE.search(body)
            if m and suffix not in latest:
                latest[suffix] = {
                    "available": float(m.group(1)),
                    "as_of": (msg.get("received") or "")[:10],
                }
        m = _RE_OVER.search(body)
        if m and suffix not in over:
            over[suffix] = {
                "over": float(m.group(1)),
                "as_of": (msg.get("received") or "")[:10],
            }
    for suffix, info in over.items():
        latest.setdefault(suffix, {}).update(info)
    return latest


def generate_credit_cards_report(db: Database, as_of: str = None,
                                 with_sms: bool = True) -> str:
    anchor = datetime.strptime(as_of, "%Y-%m-%d") if as_of else datetime.now()
    months = _recent_months(anchor)
    year, month = anchor.year, anchor.month

    tx_mgr = TransactionManager(db, book="personal")
    tx_mgr.calculate_all_balances(year, month, book="personal")

    cards = db.fetchall(
        """SELECT a.*, ab.closing_balance FROM accounts a
           LEFT JOIN account_balances ab ON ab.account_id = a.id
                AND ab.year=? AND ab.month=?
           WHERE a.book='personal' AND a.type='credit_card' AND a.is_active=1
           ORDER BY a.bank, a.card_suffix""",
        (year, month),
    )

    placeholders = ",".join("?" * len(months))
    repay = defaultdict(float)
    spend = defaultdict(float)
    last_repay = {}
    for r in db.fetchall(
        f"""SELECT account_id, direction, tx_type, tx_date, amount
            FROM account_flows
            WHERE book='personal' AND account_id IS NOT NULL
              AND substr(tx_date,1,7) IN ({placeholders})""",
        months,
    ):
        if r["tx_type"] == "credit_card_repayment" and r["direction"] == "inflow":
            repay[r["account_id"]] += r["amount"]
            last_repay[r["account_id"]] = max(
                last_repay.get(r["account_id"], ""), r["tx_date"])
        elif r["tx_type"] == "consumption" and r["direction"] == "outflow":
            spend[r["account_id"]] += r["amount"]

    credit = _available_credit() if with_sms else {}
    next_month = (f"{year + 1:04d}-01-01" if month == 12
                  else f"{year:04d}-{month + 1:02d}-01")

    lines = [f"# 信用卡状态（自动） — {as_of or anchor.strftime('%Y-%m-%d')}", ""]
    lines.append(f"> 还款来自账本；欠款仅列已设账单锚点的卡；"
                 f"可用额度来自最近短信 ｜ 生成：{now_text()}")
    lines.append("")

    lines += ["## 卡片概览", "",
              "| 卡 | 近三月消费 | 近三月还款 | 期末欠款 | 最近还款 |",
              "|------|------|------|------|------|"]
    for c in cards:
        tail = c["card_suffix"] or "?"
        name = f"{c['bank'] or ''} {tail}".strip()
        anchored = (c["opening_date"] and c["opening_date"] < next_month)
        debt = money(c["closing_balance"] or 0) if anchored else "—（未锚定）"
        lines.append(
            f"| {name} | {money(spend.get(c['id'], 0))} | "
            f"{money(repay.get(c['id'], 0))} | "
            f"{debt} | {last_repay.get(c['id'], '—')} |")
    lines.append("")

    if credit:
        lines += ["## 短信可用额度", "",
                  "| 尾号 | 可用额度 | 超限金额 | 短信日期 |",
                  "|------|------|------|------|"]
        for c in cards:
            tail = c["card_suffix"]
            info = credit.get(tail)
            if not info:
                continue
            avail = info.get("available")
            over = info.get("over")
            lines.append(
                f"| {tail} | {money(avail) if avail is not None else '—'} | "
                f"{money(over) if over else '—'} | {info.get('as_of', '—')} |")
        lines.append("")
        lines.append("*可用额度取最近一条含「当前可用额度」的短信；负数或超限表示已超信用额度。*")
        lines.append("")
    else:
        lines += ["## 短信可用额度", "",
                  "未获取到额度短信（可稍后重试或检查短信 API）。", ""]

    lines += ["---", f"*生成时间：{now_text()} ｜ 数据来源：finance.db*", ""]
    return "\n".join(lines)
