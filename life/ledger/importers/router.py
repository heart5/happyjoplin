# 自动归类引擎 — 微信+SMS 归并、账户匹配、路由决策

import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ..db import Database
from ..accounts import AccountManager
from ..cloudcfg import get_merge_window, get_loan_platforms, get_bank_names
from ..models import Account, AccountFlow

__all__ = ["FlowRouter"]

MERGE_WINDOW = get_merge_window()
TRANSFER_MERGE_WINDOW = 1800
NOTIF_FLOW_WINDOW = 600
NOTIF_SMS_WINDOW = 21600

_GENERIC_TRANSFER_KW = (
    "跨行", "他行", "转账", "汇款", "转入", "转出",
    "财付通", "支付宝", "微信", "零钱",
)

_OVERRIDE_CACHE = {"mtime": None, "rules": []}


def load_flow_overrides() -> list:
    """人工确认过的归类规则（data/finance_flow_rules.json）。

    证据不足、无法自动推断的少数流水（如充值工作号零钱、个人往来款）
    在这里按 账套+日期+方向+金额 固定下来，整月重建后仍然生效。
    """
    try:
        from func.first import getdirmain
        path = Path(str(getdirmain())) / "data" / "finance_flow_rules.json"
        mtime = path.stat().st_mtime
    except Exception:
        return []
    if _OVERRIDE_CACHE["mtime"] == mtime:
        return _OVERRIDE_CACHE["rules"]
    try:
        rules = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rules, list):
            rules = []
    except Exception:
        rules = []
    _OVERRIDE_CACHE.update(mtime=mtime, rules=rules)
    return rules


class FlowRouter:
    """将原始财务事件路由为 account_flows。

    1. 归并：同笔交易的微信通知 + 银行短信（同额、时间窗内）
    2. 账户匹配：按 book 隔离，支付方式/银行+卡尾号
    3. 路由：单腿收支，或双蹆（贷款、信用卡还款、零钱充值/提现）
    """

    def __init__(self, db: Database, acct_mgr: AccountManager, book: str = "personal"):
        self.db = db
        self.acct_mgr = acct_mgr
        self.book = book
        self._cat_cache = {}

    _LOAN_HINTS = (
        ("小赢", "小赢卡贷"),
        ("白条", "京东白条"),
        ("京东金融", "京东金融"),
        ("马上消费", "马上消费金融"),
        ("中邮", "中邮消金"),
        ("安逸花", "马上消费金融"),
        ("招联", "招联金融"),
        ("个贷", "招商银行个贷"),
        ("经营贷", "网商银行经营贷"),
    )

    @classmethod
    def _detect_loan_platform(cls, text: str):
        if not text:
            return None
        for p in get_loan_platforms():
            if p in text:
                return p
        for kw, platform in cls._LOAN_HINTS:
            if kw in text:
                return platform
        return None

    @staticmethod
    def _flow_dir(evt: dict) -> str:
        return "outflow" if evt.get("direction", "支出") == "支出" else "inflow"

    @staticmethod
    def _is_repayment_hint(evt: dict) -> bool:
        return (evt.get("source") == "repayment_hint"
                or evt.get("tx_type_detail") == "repayment_hint")

    @staticmethod
    def _is_zero_wallet_evt(evt: dict) -> bool:
        return (evt.get("category") == "内部-转账"
                and evt.get("merchant") in ("零钱提现", "零钱充值"))

    @staticmethod
    def _is_wallet_bank_sms(sms: dict) -> bool:
        text = sms.get("source_text") or ""
        direction = sms.get("direction", "支出")
        return (("零钱提现" in text and direction == "收入")
                or ("零钱充值" in text and direction == "支出"))

    @staticmethod
    def _transfer_match_ok(wx_evt: dict, sms: dict) -> bool:
        wm = (wx_evt.get("merchant") or "").strip()
        sm = (sms.get("merchant") or "").strip()
        if wm == sm:
            return True
        generic = lambda m: (not m) or any(k in m for k in _GENERIC_TRANSFER_KW)
        return generic(wm) or generic(sm)

    # ── 主入口 ──

    def route_events(self, wechat_events: list, sms_records: list,
                     notification_events: list = None) -> list:
        flows = []
        notification_events = notification_events or []
        hints = [e for e in list(wechat_events) + notification_events
                 if self._is_repayment_hint(e)]
        sms_records = self._dedup_sms(sms_records)
        used_sms = set()
        used_wx = set()

        wx_index = defaultdict(list)
        for i, evt in enumerate(wechat_events):
            if self._is_repayment_hint(evt):
                continue
            date = (evt.get("time") or "")[:10]
            amt = round(evt.get("amount", 0), 2)
            if amt > 0:
                wx_index[(date, amt)].append((evt, i))

        # Phase 1: SMS 找微信归并
        for si, sms in enumerate(sms_records):
            date = (sms.get("time") or "")[:10]
            amt = round(sms.get("amount", 0), 2)
            if amt <= 0:
                continue

            sms_dir = self._flow_dir(sms)
            wallet_sms = self._is_wallet_bank_sms(sms)
            matched = None
            for wx_evt, wi in wx_index.get((date, amt), []):
                if wi in used_wx:
                    continue
                # 零钱提现/充值：短信已给银行腿+钱包腿时，丢弃微信钱包侧通知
                if self._is_zero_wallet_evt(wx_evt):
                    if wallet_sms and self._time_diff_seconds(
                            wx_evt.get("time", ""), sms.get("time", "")) <= TRANSFER_MERGE_WINDOW:
                        matched = (wx_evt, wi, "wallet")
                        break
                    continue
                if wx_evt.get("category") == "内部-转账":
                    continue
                if self._flow_dir(wx_evt) != sms_dir:
                    continue
                transfer_like = (wx_evt.get("category") or "") in (
                    "社交-转账", "收入-转账")
                window = TRANSFER_MERGE_WINDOW if transfer_like else MERGE_WINDOW
                if self._time_diff_seconds(
                        wx_evt.get("time", ""), sms.get("time", "")) > window:
                    continue
                if transfer_like and not self._transfer_match_ok(wx_evt, sms):
                    continue
                matched = (wx_evt, wi, "merge")
                break

            if matched:
                wx_evt, wi, kind = matched
                used_sms.add(si)
                if kind == "wallet":
                    for other, oi in wx_index.get((date, amt), []):
                        if (self._is_zero_wallet_evt(other)
                                and other.get("merchant") == wx_evt.get("merchant")):
                            used_wx.add(oi)
                    self._extend(flows, self._route_single_sms(sms))
                else:
                    used_wx.add(wi)
                    flows.append(self._merge_event(wx_evt, sms))
            else:
                self._extend(flows, self._route_single_sms(sms))
                used_sms.add(si)

        # Phase 2: 未归并的微信事件
        for i, evt in enumerate(wechat_events):
            if i in used_wx or self._is_repayment_hint(evt):
                continue
            if round(evt.get("amount", 0), 2) <= 0:
                continue
            f = self._route_single_wechat(evt)
            if f:
                flows.append(f)

        # Phase 3: 未处理的 SMS（兜底）
        for si, sms in enumerate(sms_records):
            if si in used_sms:
                continue
            self._extend(flows, self._route_single_sms(sms))

        # Phase 4: 通知独有交易（微信/短信优先；通知不自动新建银行账户）
        for evt in self._dedup_notifications(
                notification_events or [], sms_records, flows):
            self._extend(flows, self._route_single_sms(
                evt, source="notification", allow_create=False))

        flows = self._merge_duplicate_flows(flows)
        # 只去除同一来源事件的精确重复，保留同日同额的不同交易
        flows = self._dedup(flows)
        self._reclassify_internal_transfers(flows)
        self._reclassify_loan_bank_legs(flows)
        self._reclassify_bank_repayments(flows, hints)
        self._reclassify_wallet_topups(flows)
        self._apply_flow_overrides(flows)
        flows.sort(key=lambda f: (f.tx_date or "", f.tx_time or ""))
        return flows

    def _dedup_notifications(self, notifications: list, sms_records: list,
                             routed_flows: list) -> list:
        """通知与已路由流水/SMS 同笔去重。

        微信支付成功提醒常与银行 App 动账通知成对出现（时间差数十秒），
        故对已路由流水用 ±10 分钟；短信可能延迟，仍用 ±6 小时。
        """
        def same_tx(amount: float, direction: str, suffix: str,
                    time: str, window: float) -> bool:
            if abs(round(amount, 2) - amt) > 0.01:
                return False
            if direction != evt_dir:
                return False
            if suffix and evt_suffix and suffix != evt_suffix:
                return False
            t1, t2 = evt_time, time or ""
            if not t1 or not t2:
                return False
            return self._time_diff_seconds(t1, t2) <= window

        kept = []
        for evt in notifications:
            amt = round(evt.get("amount", 0), 2)
            if amt <= 0:
                continue
            evt_dir = self._flow_dir(evt)
            evt_suffix = evt.get("card_suffix") or ""
            evt_time = evt.get("time") or ""
            # 贷款/卡还款通知是账户侧另一腿，可能与付款腿同额同时，不算重复
            account_leg = (evt.get("is_loan")
                           or evt.get("tx_type_detail") == "repayment_in")

            if not account_leg and any(
                    same_tx(f.amount, f.direction, "", f.tx_time,
                            NOTIF_FLOW_WINDOW) for f in routed_flows):
                continue
            if any(same_tx(sms.get("amount", 0), self._flow_dir(sms),
                           sms.get("card_suffix") or "", sms.get("time") or "",
                           NOTIF_SMS_WINDOW)
                   and self._same_notif_leg(evt, sms) for sms in sms_records):
                continue
            kept.append(evt)
        return kept

    @staticmethod
    def _same_notif_leg(evt: dict, other: dict) -> bool:
        """贷款/卡还款通知只与同类型账户腿去重，不与付款腿互斥。"""
        if evt.get("is_loan") or other.get("is_loan"):
            return bool(evt.get("is_loan")) and bool(other.get("is_loan"))
        detail = evt.get("tx_type_detail")
        if detail == "repayment_in" or other.get("tx_type_detail") == "repayment_in":
            return detail == other.get("tx_type_detail")
        return True

    def _reclassify_internal_transfers(self, flows: list):
        """银行卡支出提醒 + 同日同额他行转入短信 = 本人账户间转账。"""
        transfers_in = [
            f for f in flows
            if f.source == "sms" and f.direction == "inflow"
            and f.tx_type == "transfer" and f.account_id
        ]
        for f in flows:
            if (not self._is_wechat_source(f.source) or f.direction != "outflow"
                    or f.tx_type != "consumption" or not f.account_id):
                continue
            for inc in transfers_in:
                if (f.tx_date == inc.tx_date
                        and abs(f.amount - inc.amount) <= 0.01
                        and f.account_id != inc.account_id
                        and self._time_diff_seconds(f.tx_time, inc.tx_time)
                        <= TRANSFER_MERGE_WINDOW):
                    f.tx_type = "transfer"
                    f.category_id = self._get_category_id("内部-转账")
                    f.counterparty = inc.merchant or f.counterparty
                    break

    def _reclassify_loan_bank_legs(self, flows: list):
        """贷款平台还款确认 + 同日同额银行卡支出提醒 = 贷款还款的银行腿。"""
        loan_legs = [
            f for f in flows
            if f.source in ("sms", "notification")
            and f.tx_type == "loan_repayment" and f.direction == "inflow"
        ]
        for f in flows:
            if (not self._is_wechat_source(f.source) or f.direction != "outflow"
                    or f.tx_type != "consumption" or not f.account_id):
                continue
            row = self.db.fetchone(
                "SELECT type FROM accounts WHERE id=?", (f.account_id,))
            if not row or row["type"] not in ("bank", "credit_card"):
                continue
            mer = f.merchant or ""
            if not (any(b in mer for b in get_bank_names())
                    or any(k in mer for k in ("金融", "消费金融", "还款"))):
                continue
            for leg in loan_legs:
                if (f.tx_date == leg.tx_date
                        and abs(f.amount - leg.amount) <= 0.01
                        and self._time_diff_seconds(f.tx_time, leg.tx_time)
                        <= 43200):
                    f.tx_type = "loan_repayment"
                    f.category_id = self._get_category_id("借贷-还款")
                    # 与贷款腿共用 pair_key，报告按配对去重、不重复计总额
                    f.pair_key = leg.pair_key
                    break

    def _reclassify_bank_repayments(self, flows: list, hints: list):
        """无商户银行卡扣款 + 同笔还款线索（白条拒付改道、平台还款成功）= 贷款还款。

        线索来自两类消息：京东白条“交易失败通知（来源支付宝、商户还款）”
        和平台“已最低还款成功”通知。金额明确时要求同额，无金额时只按
        平台与 5 分钟内时间窗匹配。
        """
        if not hints:
            return
        for f in flows:
            if (f.direction != "outflow" or f.tx_type != "consumption"
                    or not f.account_id):
                continue
            mer = f.merchant or ""
            if not any(b in mer for b in get_bank_names()):
                continue
            for h in hints:
                platform = h.get("merchant") or ""
                if not platform:
                    continue
                amt = round(h.get("amount") or 0, 2)
                if amt > 0 and abs(amt - f.amount) > 0.01:
                    continue
                window = 600 if amt > 0 else 300
                if (self._time_diff_seconds(f.tx_time, h.get("time") or "")
                        > window):
                    continue
                f.tx_type = "loan_repayment"
                f.category_id = self._get_category_id("借贷-还款")
                f.merchant = platform
                f.counterparty = platform
                break

    def _reclassify_wallet_topups(self, flows: list):
        """本账套银行卡扣款 + 另一账套同额“零钱”入账 = 充值微信零钱。

        工作号只采微信聊天，零钱充值通知可能缺失；有入账记录时即可配对，
        无入账证据时保留原分类，避免臆测。
        """
        for f in flows:
            if (f.direction != "outflow" or f.tx_type != "consumption"
                    or not f.account_id):
                continue
            row = self.db.fetchone(
                "SELECT type FROM accounts WHERE id=?", (f.account_id,))
            if not row or row["type"] not in ("bank", "credit_card"):
                continue
            if not any(b in (f.merchant or "") for b in get_bank_names()):
                continue
            hit = self.db.fetchone(
                "SELECT merchant FROM account_flows"
                " WHERE book!=? AND tx_date=? AND direction='inflow'"
                "   AND abs(amount-?)<=0.01 AND merchant LIKE '%零钱%'"
                "   AND abs(strftime('%s', tx_time)-strftime('%s', ?))<=1800"
                " LIMIT 1",
                (self.book, f.tx_date, f.amount, f.tx_time),
            )
            if hit:
                f.tx_type = "transfer"
                f.category_id = self._get_category_id("内部-转账")
                f.merchant = "工作号微信零钱" if self.book == "personal" \
                    else "微信零钱"
                f.counterparty = hit["merchant"]

    def _apply_flow_overrides(self, flows: list):
        """人工确认的归类规则优先于所有自动推断。"""
        rules = load_flow_overrides()
        if not rules:
            return
        for rule in rules:
            if rule.get("book") and rule["book"] != self.book:
                continue
            amount = rule.get("amount")
            for f in flows:
                if rule.get("date") and rule["date"] != f.tx_date:
                    continue
                if (rule.get("direction")
                        and rule["direction"] != f.direction):
                    continue
                if amount is not None and abs(float(amount) - f.amount) > 0.01:
                    continue
                if rule.get("tx_type"):
                    f.tx_type = rule["tx_type"]
                if rule.get("category"):
                    f.category_id = self._get_category_id(rule["category"])
                if rule.get("merchant"):
                    f.merchant = rule["merchant"]
                if rule.get("counterparty"):
                    f.counterparty = rule["counterparty"]
                if rule.get("notes"):
                    f.notes = rule["notes"]
                break

    # ── 归并 ──

    def _merge_event(self, wx_evt: dict, sms: dict) -> AccountFlow:
        amt = round(wx_evt.get("amount", 0), 2)
        direction = "outflow" if wx_evt.get("direction", "支出") == "支出" else "inflow"
        payment_method = wx_evt.get("payment_method", "") or sms.get("payment_method", "")
        card_suffix = sms.get("card_suffix") or wx_evt.get("card_suffix", "")
        org = sms.get("payment_method", "")
        merchant = wx_evt.get("merchant", "") or sms.get("merchant", "")
        cat_name = wx_evt.get("category", "未分类-其他")

        # 归并时以短信发卡行+尾号为准，微信侧“Acquirer 某某银行”
        # 只是收单行，会把交易记到错误的卡上
        account = None
        if org and card_suffix:
            account = self.acct_mgr.match_by_bank_sms(
                org, card_suffix, self.book,
                "credit_card" if "信用卡" in (sms.get("source_text") or "") else "bank")
        if account is None:
            account = self._resolve_wechat_account(payment_method, card_suffix, org)
        wx_sid = self._event_sid(wx_evt, self._source_prefix(wx_evt))
        sms_sid = self._event_sid(sms, "sms")
        sid = "|".join(sorted([wx_sid, sms_sid]))

        # 银行短信能识别还款/贷款等特殊类型时，优先采用短信侧口径
        sms_type = self._sms_tx_type_from_record(sms)
        wx_type = self._wechat_tx_type(cat_name, direction)
        if sms_type in ("credit_card_repayment", "loan_disbursement",
                        "loan_repayment", "fee"):
            tx_type = sms_type
        elif "transfer" in (sms_type, wx_type):
            tx_type = "transfer"
        else:
            tx_type = wx_type
        if tx_type == "credit_card_repayment":
            category_name, merchant = "内部-转账", "信用卡还款"
        elif tx_type == "transfer":
            category_name = cat_name if wx_type == "transfer" else "内部-转账"
        elif tx_type == "loan_repayment":
            category_name = "借贷-还款"
        elif tx_type == "loan_disbursement":
            category_name = "借贷-放款"
        else:
            category_name = cat_name

        return AccountFlow(
            book=self.book,
            tx_date=(wx_evt.get("time") or "")[:10],
            tx_time=wx_evt.get("time", ""),
            amount=amt,
            account_id=account.id if account else None,
            direction=direction,
            tx_type=tx_type,
            category_id=self._get_category_id(category_name),
            merchant=merchant,
            counterparty=merchant,
            source=self._event_source(wx_evt),
            source_event_id=sid,
            source_group_id=self._group_id(sid, amt),
            raw_data=json.dumps({
                "wechat": wx_evt.get("source_text", ""),
                "sms": sms.get("source_text", ""),
            }, ensure_ascii=False),
        )

    # ── 微信单腿 ──

    def _route_single_wechat(self, evt: dict) -> Optional[AccountFlow]:
        direction = "outflow" if evt.get("direction", "支出") == "支出" else "inflow"
        payment_method = evt.get("payment_method", "")
        merchant = evt.get("merchant", "")
        cat_name = evt.get("category", "未分类-其他")

        if self.book == "work":
            account = self.acct_mgr.get_wechat_wallet("work")
        elif evt.get("source") in ("bank_expense", "bank_income"):
            account = self.acct_mgr.match_by_bank_sms(
                evt.get("org", ""), evt.get("card_suffix", ""), self.book,
                "credit_card" if "信用卡" in (evt.get("source_text") or "") else "bank")
            if account is None:
                account = self._resolve_wechat_account(
                    payment_method, evt.get("card_suffix", ""), evt.get("org", ""))
        elif evt.get("source") == "refund":
            # 退款退回原卡/零钱
            account = self._resolve_wechat_account(
                payment_method, evt.get("card_suffix", ""), "")
        elif direction == "inflow":
            account = self.acct_mgr.get_wechat_wallet(self.book)
        else:
            account = self._resolve_wechat_account(
                payment_method, evt.get("card_suffix", ""), "")

        sid = self._event_sid(evt, self._source_prefix(evt))
        return AccountFlow(
            book=self.book,
            tx_date=(evt.get("time") or "")[:10],
            tx_time=evt.get("time", ""),
            amount=round(evt.get("amount", 0), 2),
            account_id=account.id if account else None,
            direction=direction,
            tx_type=self._wechat_tx_type(cat_name, direction),
            category_id=self._get_category_id(cat_name),
            merchant=merchant,
            counterparty=merchant,
            source=self._event_source(evt),
            source_event_id=sid,
            source_group_id=self._group_id(sid, evt.get("amount", 0)),
            raw_data=json.dumps({"wechat": evt.get("source_text", "")}, ensure_ascii=False),
        )

    # ── SMS 路由 ──

    def _route_single_sms(self, sms: dict, source: str = "sms",
                          allow_create: bool = True) -> Optional[list]:
        amt = round(sms.get("amount", 0), 2)
        if amt <= 0 or sms.get("_skip"):
            return None

        is_loan = sms.get("is_loan", False)
        category = sms.get("category", "未分类-其他")
        direction = sms.get("direction", "支出")
        org = sms.get("payment_method", "")
        card_suffix = sms.get("card_suffix", "")
        merchant = sms.get("merchant", "")
        source_text = sms.get("source_text", "")
        detail = sms.get("tx_type_detail", "")
        sid = self._event_sid(sms, source)
        f_dir = "outflow" if direction == "支出" else "inflow"

        if is_loan:
            # 识别不到平台时不要用银行名冒充贷款账户
            loan_org = (self._detect_loan_platform(merchant)
                        or self._detect_loan_platform(source_text)
                        or "其他贷款")
            return self._route_loan(
                sms, amt, category, org, card_suffix, merchant, loan_org,
                source=source, allow_create=allow_create)

        # 信用卡还款：每侧短信只记自己这一腿，跨行/跨账户不强行配对
        cc_flow = self._route_cc_repayment(
            sms, amt, org, card_suffix, source_text, detail,
            source=source, allow_create=allow_create)
        if cc_flow is not None:
            return cc_flow

        prefer_type = "credit_card" if "信用卡" in source_text else "bank"
        account = self.acct_mgr.match_by_bank_sms(
            org, card_suffix, self.book, prefer_type, allow_create=allow_create)
        if account is None and "支付宝" in source_text:
            account = self.acct_mgr.get_alipay(self.book)
        if account is None and ("微信" in source_text or "零钱" in source_text):
            account = self.acct_mgr.get_wechat_wallet(self.book)

        # 零钱充值/提现：银行腿 + 钱包腿
        if "零钱充值" in source_text and f_dir == "outflow":
            return self._wallet_transfer(sms, amt, f_dir, account, "零钱充值", bank_leg=True)
        if "零钱提现" in source_text and f_dir == "inflow":
            return self._wallet_transfer(sms, amt, f_dir, account, "零钱提现", bank_leg=True)

        tx_type = self._sms_tx_type(detail, category, f_dir)

        flows = [AccountFlow(
            book=self.book,
            tx_date=(sms.get("time") or "")[:10],
            tx_time=sms.get("time", ""),
            amount=amt,
            account_id=account.id if account else None,
            direction=f_dir,
            tx_type=tx_type,
            category_id=self._get_category_id("内部-转账" if tx_type == "transfer" else category),
            merchant=merchant,
            counterparty=sms.get("counterparty") or merchant,
            source=source,
            source_event_id=sid,
            source_group_id=self._group_id(sid, amt),
            raw_data=json.dumps({source: source_text}, ensure_ascii=False),
        )]
        return flows

    def _sms_tx_type_from_record(self, sms: dict) -> str:
        text = sms.get("source_text") or ""
        detail = sms.get("tx_type_detail") or ""
        category = sms.get("category") or "未分类-其他"
        if (any(k in text for k in self._CC_OUTFLOW_KW)
                or detail == "repayment_in"
                or any(k in text for k in self._CC_INFLOW_KW)):
            return "credit_card_repayment"
        if sms.get("is_loan"):
            return ("loan_disbursement" if category == "借贷-放款"
                    else "loan_repayment")
        return self._sms_tx_type(detail, category, self._flow_dir(sms))

    def _sms_tx_type(self, detail: str, category: str, f_dir: str) -> str:
        if detail == "loan_repayment":
            return "loan_repayment"
        if detail == "repayment_in":
            return "credit_card_repayment"
        if detail == "fee":
            return "fee"
        if detail in ("transfer", "transfer_in", "transfer_out",
                      "withdrawal", "deposit", "cash_deposit"):
            return "transfer"
        if category.startswith(("内部-", "社交-转账")) or category == "收入-转账":
            return "transfer"
        if category.startswith("收入-") or f_dir == "inflow":
            return "income"
        return "consumption"

    # ── 贷款双侧 ──

    def _route_loan(self, sms: dict, amt: float, category: str,
                    org: str, card_suffix: str, merchant: str,
                    loan_org: str = None, source: str = "sms",
                    allow_create: bool = True) -> Optional[list]:
        loan_acct = self.acct_mgr.get_or_create_loan(loan_org or org, self.book)
        source_text = sms.get("source_text", "")
        sid = self._event_sid(sms, source)
        group = self._group_id(sid, amt)
        pair = "loan|" + hashlib.sha1(
            f"{self.book}|{sid}|{amt:.2f}".encode()).hexdigest()[:16]
        raw = json.dumps({source: source_text}, ensure_ascii=False)
        tx_date = (sms.get("time") or "")[:10]
        bank_acct = self.acct_mgr.match_by_bank_sms(
            org, card_suffix, self.book,
            "credit_card" if "信用卡" in source_text else "bank",
            allow_create=allow_create)

        if category == "借贷-放款":
            tx_type = "loan_disbursement"
            bank_dir, loan_dir = "inflow", "outflow"
        else:
            tx_type = "loan_repayment"
            bank_dir, loan_dir = "outflow", "inflow"

        legs = []
        if bank_acct:
            legs.append(AccountFlow(
                book=self.book, tx_date=tx_date, tx_time=sms.get("time", ""),
                amount=amt, account_id=bank_acct.id, direction=bank_dir,
                tx_type=tx_type, category_id=self._get_category_id(category),
                merchant=merchant, counterparty=org, source=source,
                source_event_id=sid, source_group_id=group, pair_key=pair,
                raw_data=raw,
            ))
        legs.append(AccountFlow(
            book=self.book, tx_date=tx_date, tx_time=sms.get("time", ""),
            amount=amt, account_id=loan_acct.id, direction=loan_dir,
            tx_type=tx_type, category_id=self._get_category_id(category),
            merchant=merchant, counterparty=bank_acct.name if bank_acct else org,
            source=source, source_event_id=sid, source_group_id=group,
            pair_key=pair, raw_data=raw,
        ))
        return legs

    # ── 信用卡还款（单侧）──

    _CC_INFLOW_KW = ("还款入账", "自动还款已扣", "成功还款", "信用卡还款人民币")
    _CC_OUTFLOW_KW = ("信用卡还款交易", "信用卡卡号还款", "还款支出",
                      "的信用卡还款")

    def _route_cc_repayment(self, sms: dict, amt: float, org: str,
                            card_suffix: str, source_text: str,
                            detail: str, source: str = "sms",
                            allow_create: bool = True) -> Optional[AccountFlow]:
        """还款短信只记发卡行这一腿；另一腿由对侧短信独立入账。"""
        is_inflow = (detail == "repayment_in"
                     or any(k in source_text for k in self._CC_INFLOW_KW))
        is_outflow = any(k in source_text for k in self._CC_OUTFLOW_KW)
        if not is_inflow and not is_outflow:
            return None

        if is_outflow:
            account = self.acct_mgr.match_by_bank_sms(
                org, card_suffix, self.book, "bank", allow_create=allow_create)
            f_dir = "outflow"
        else:
            account = (self.acct_mgr.match_by_bank_sms(
                org, card_suffix, self.book, "credit_card",
                allow_create=allow_create)
                or self._find_credit_card(source_text, org, card_suffix))
            f_dir = "inflow"
        if account is None:
            return None

        sid = self._event_sid(sms, source)
        return AccountFlow(
            book=self.book,
            tx_date=(sms.get("time") or "")[:10],
            tx_time=sms.get("time", ""),
            amount=amt,
            account_id=account.id,
            direction=f_dir,
            tx_type="credit_card_repayment",
            category_id=self._get_category_id("内部-转账"),
            merchant="信用卡还款入账" if is_inflow else "信用卡还款",
            counterparty=account.name,
            source=source,
            source_event_id=sid,
            source_group_id=self._group_id(sid, amt),
            raw_data=json.dumps({source: source_text}, ensure_ascii=False),
        )

    # ── 短信内部去重 ──

    def _dedup_sms(self, records: list) -> list:
        """贷款还款：银行扣款通知与平台确认通知同额同日时保留银行侧。"""
        if not records:
            return records
        used = set()
        kept = []
        n = len(records)
        for i, a in enumerate(records):
            if i in used:
                continue
            if a.get("is_loan"):
                for j in range(i + 1, n):
                    if j in used:
                        continue
                    b = records[j]
                    if not b.get("is_loan"):
                        continue
                    if abs(a.get("amount", 0) - b.get("amount", 0)) >= 0.01:
                        continue
                    if (a.get("time") or "")[:10] != (b.get("time") or "")[:10]:
                        continue
                    if self._is_bank_side(a) != self._is_bank_side(b):
                        kept.append(a if self._is_bank_side(a) else b)
                        used.add(i)
                        used.add(j)
                        break
                else:
                    kept.append(a)
                    used.add(i)
                continue
            kept.append(a)
            used.add(i)
        return self._dedup_similar(kept)

    def _dedup_similar(self, records: list) -> list:
        """同一笔交易的双通知（银行扣款 + 平台/缴费确认）合并为一条。"""
        result = []
        for rec in records:
            for idx, prev in enumerate(result):
                if self._same_sms_txn(prev, rec):
                    if self._sms_score(rec) > self._sms_score(prev):
                        result[idx] = rec
                    break
            else:
                result.append(rec)
        return result

    @staticmethod
    def _sms_score(rec: dict) -> float:
        score = min(len(rec.get("source_text") or "") / 200.0, 1.0)
        pm = rec.get("payment_method") or ""
        if any(k in pm for k in ("银行", "支付宝", "微信", "电信", "移动", "联通")):
            score += 2
        if rec.get("card_suffix"):
            score += 1
        if rec.get("merchant") and rec.get("merchant") != rec.get("payment_method"):
            score += 1
        if rec.get("category") not in (None, "", "未分类-其他"):
            score += 1
        return score

    def _same_sms_txn(self, a: dict, b: dict) -> bool:
        if a.get("is_loan") or b.get("is_loan"):
            return False
        if abs(round(a.get("amount", 0), 2) - round(b.get("amount", 0), 2)) > 0.01:
            return False
        if a.get("direction") != b.get("direction"):
            return False
        if (a.get("time") or "")[:10] != (b.get("time") or "")[:10]:
            return False
        if self._time_diff_seconds(a.get("time", ""), b.get("time", "")) > 600:
            return False
        if a.get("card_suffix") != b.get("card_suffix"):
            return False
        if a.get("payment_method") == b.get("payment_method"):
            return True
        kw = ("充值", "缴费", "话费")
        return all(any(k in (r.get("source_text") or "") for k in kw)
                   for r in (a, b))

    @staticmethod
    def _is_bank_side(rec: dict) -> bool:
        text = " ".join([
            rec.get("source_text", ""), rec.get("merchant", ""),
            rec.get("payment_method", ""),
        ])
        return any(bank in text for bank in get_bank_names())

    def _find_credit_card(self, source_text: str, org: str,
                          card_suffix: str) -> Optional[Account]:
        """从短信正文定位被还款的信用卡（可能与他行储蓄卡跨行还款）。"""
        from ..cloudcfg import get_bank_names
        banks = [b for b in get_bank_names() if b in (source_text or "")]
        if card_suffix:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE book=? AND type='credit_card'"
                " AND card_suffix=? AND is_active=1",
                (self.book, card_suffix),
            )
            if row:
                return self.acct_mgr._row_to_account(row)
        for bank in banks:
            if bank != AccountManager._normalize_bank_name(org):
                row = self.db.fetchone(
                    "SELECT * FROM accounts WHERE book=? AND type='credit_card'"
                    " AND bank=? AND is_active=1 ORDER BY id LIMIT 1",
                    (self.book, bank),
                )
                if row:
                    return self.acct_mgr._row_to_account(row)
        return self.acct_mgr.match_by_bank_sms(org, None, self.book, "credit_card")

    # ── 零钱充值/提现双侧 ──

    def _wallet_transfer(self, sms: dict, amt: float, bank_dir: str,
                         bank_acct: Optional[Account], label: str,
                         bank_leg: bool = True) -> list:
        sid = self._event_sid(sms, "sms")
        group = self._group_id(sid, amt)
        pair = "wallet|" + hashlib.sha1(
            f"{self.book}|{sid}|{amt:.2f}".encode()).hexdigest()[:16]
        raw = json.dumps({"sms": sms.get("source_text", "")}, ensure_ascii=False)
        tx_date = (sms.get("time") or "")[:10]
        cat = self._get_category_id("内部-转账")
        wallet = self.acct_mgr.get_wechat_wallet(self.book)
        legs = []
        if bank_acct:
            legs.append(AccountFlow(
                book=self.book, tx_date=tx_date, tx_time=sms.get("time", ""),
                amount=amt, account_id=bank_acct.id, direction=bank_dir,
                tx_type="transfer", category_id=cat, merchant=label,
                counterparty="微信零钱", source="sms", source_event_id=sid,
                source_group_id=group, pair_key=pair, raw_data=raw,
            ))
        if wallet:
            legs.append(AccountFlow(
                book=self.book, tx_date=tx_date, tx_time=sms.get("time", ""),
                amount=amt, account_id=wallet.id,
                direction="inflow" if bank_dir == "outflow" else "outflow",
                tx_type="transfer", category_id=cat, merchant=label,
                counterparty=bank_acct.name if bank_acct else "",
                source="sms", source_event_id=sid, source_group_id=group,
                pair_key=pair, raw_data=raw,
            ))
        return legs

    # ── 辅助 ──

    def _resolve_wechat_account(self, payment_method: str, card_suffix: str,
                                org: str) -> Optional[Account]:
        if self.book == "work":
            return self.acct_mgr.get_wechat_wallet("work")
        # 尾号最可靠；同一尾号优先信用卡，避免把卡交易记到储蓄卡
        if card_suffix:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE book=? AND card_suffix=?"
                " AND is_active=1 ORDER BY"
                " CASE WHEN type='credit_card' THEN 0 WHEN type='bank' THEN 1"
                " ELSE 2 END, id LIMIT 1",
                (self.book, card_suffix),
            )
            if row:
                return self.acct_mgr._row_to_account(row)
        if payment_method:
            acct = self.acct_mgr.match_by_payment_method(
                payment_method, card_suffix, self.book)
            if acct:
                return acct
        if org:
            prefer = "credit_card" if "信用卡" in org else None
            acct = self.acct_mgr.match_by_bank_sms(org, card_suffix, self.book, prefer)
            if acct:
                return acct
        return self.acct_mgr.get_wechat_wallet(self.book)

    def _wechat_source(self) -> str:
        return "wechat_work" if self.book == "work" else "wechat_main"

    def _event_source(self, evt: dict) -> str:
        """事件实际来源：个人账套也会收到工作号的卡付事件（_source 标记）。"""
        return evt.get("_source") or self._wechat_source()

    @staticmethod
    def _is_wechat_source(src: str) -> bool:
        return src in ("wechat_main", "wechat_work")

    def _wechat_prefix(self) -> str:
        return "wx2" if self.book == "work" else "wxmain"

    def _source_prefix(self, evt: dict) -> str:
        return "wx2" if self._event_source(evt) == "wechat_work" else "wxmain"

    def _wechat_tx_type(self, category: str, direction: str) -> str:
        if category == "借贷-放款":
            return "loan_disbursement"
        if category == "借贷-还款":
            return "loan_repayment"
        if category.startswith("内部-") or category in ("社交-转账", "收入-转账"):
            return "transfer"
        if category.startswith("收入-") or direction == "inflow":
            return "income"
        return "consumption"

    def _event_sid(self, evt: dict, prefix: str) -> str:
        sid = evt.get("source_event_id")
        if sid:
            return str(sid)
        ids = evt.get("source_ids")
        if ids:
            return f"{prefix}:" + ",".join(str(i) for i in sorted(ids))
        raw = "|".join([
            str(evt.get("time", "")), f"{float(evt.get('amount', 0)):.2f}",
            str(evt.get("merchant", "")), str(evt.get("direction", "")),
            str(evt.get("payment_method", "")),
        ])
        return f"{prefix}:h{hashlib.sha1(raw.encode()).hexdigest()[:16]}"

    def _get_category_id(self, name: str) -> Optional[int]:
        if not name:
            return None
        if name in self._cat_cache:
            return self._cat_cache[name]
        row = self.db.fetchone("SELECT id FROM categories WHERE name=?", (name,))
        if row:
            self._cat_cache[name] = row["id"]
            return row["id"]

        direction = "expense"
        if name.startswith("收入-"):
            direction = "income"
        elif name.startswith("内部-"):
            direction = "transfer"
        is_loan = 1 if name.startswith("借贷-") else 0
        cat_id = self.db.insert("categories", {
            "name": name, "direction": direction, "is_loan": is_loan,
        })
        self._cat_cache[name] = cat_id
        return cat_id

    @staticmethod
    def _time_diff_seconds(t1: str, t2: str) -> float:
        try:
            dt1 = datetime.strptime(t1[:19], "%Y-%m-%d %H:%M:%S")
            dt2 = datetime.strptime(t2[:19], "%Y-%m-%d %H:%M:%S")
            return abs((dt1 - dt2).total_seconds())
        except (ValueError, OSError):
            return 9999

    @staticmethod
    def _group_id(sid: str, amt: float) -> str:
        return hashlib.sha1(f"{sid}|{float(amt):.2f}".encode()).hexdigest()[:16]

    @staticmethod
    def _extend(flows: list, result):
        if not result:
            return
        if isinstance(result, list):
            flows.extend(result)
        else:
            flows.append(result)

    # ── 跨来源去重（同卡同额同向）──

    _DUP_SKIP_TYPES = ("transfer", "loan_disbursement", "loan_repayment",
                       "credit_card_repayment", "fee", "adjustment")

    _RE_TXT_TIME_FULL = re.compile(
        r"(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2})[时:：](\d{1,2})"
        r"(?:[分:：](\d{1,2}))?")
    _RE_TXT_TIME_MD = re.compile(
        r"(?<!\d)(\d{1,2})月(\d{1,2})日\s*(\d{1,2})[时:：](\d{1,2})"
        r"(?:[分:：](\d{1,2}))?")

    def _merge_duplicate_flows(self, flows: list) -> list:
        """同一笔刷卡交易被两个微信号/银行短信各自播报时只保留一条。

        同一张卡绑在两个微信号上：付款后主号收到银行「信用卡交易提醒」，
        工作号收到微信支付回执，两者相差几秒；银行短信/App 通知可能
        晚到数小时（短信 10 小时后才送达也见过）。因此按同卡同额同向
        分组，用消息正文里的“交易时间”配对，而不是送达时间。
        """
        groups = defaultdict(list)
        for f in flows:
            if (f.account_id is None or f.tx_type in self._DUP_SKIP_TYPES
                    or f.pair_key or f.linked_flow_id):
                continue
            groups[(f.tx_date, f.direction, round(f.amount, 2),
                    f.account_id)].append(f)

        dead = set()
        for group in groups.values():
            if len(group) < 2:
                continue
            group.sort(key=lambda f: (f.tx_time or "", f.source))
            mt = {id(f): self._match_time(f) for f in group}
            for a in group:
                if id(a) in dead or not self._is_wechat_source(a.source):
                    continue
                for b in group:
                    if (b is a or id(b) in dead
                            or not self._is_wechat_source(b.source)
                            or b.source == a.source):
                        continue
                    if self._time_diff_seconds(mt[id(a)], mt[id(b)]) <= 300:
                        self._absorb(a, b)
                        dead.add(id(b))
            for a in group:
                if id(a) in dead or a.source not in ("sms", "notification"):
                    continue
                best = None
                for b in group:
                    if (id(b) in dead or b is a
                            or not self._is_wechat_source(b.source)):
                        continue
                    diff = self._time_diff_seconds(mt[id(a)], mt[id(b)])
                    if diff <= NOTIF_SMS_WINDOW and (best is None or diff < best[0]):
                        best = (diff, b)
                if best:
                    self._absorb(best[1], a)
                    dead.add(id(a))
        return [f for f in flows if id(f) not in dead]

    def _match_time(self, f) -> str:
        """流水对应的交易时间：优先取正文里的“X月X日 X时X分”，退回记录时间。"""
        text = f.raw_data or ""
        year = (f.tx_date or "")[:4]
        m = self._RE_TXT_TIME_FULL.search(text)
        if m:
            y, mo, d, h, mi, s = m.groups()
        else:
            m = self._RE_TXT_TIME_MD.search(text)
            if not m:
                return f.tx_time or ""
            (mo, d, h, mi, s), y = m.groups(), year
        try:
            return "%04d-%02d-%02d %02d:%02d:%02d" % (
                int(y), int(mo), int(d), int(h), int(mi), int(s or 0))
        except (TypeError, ValueError):
            return f.tx_time or ""

    def _absorb(self, base, other):
        """把 other 的信息并入 base（保留 base 行）。"""
        if (self._is_generic_merchant(base.merchant)
                and not self._is_generic_merchant(other.merchant)):
            base.merchant = other.merchant
            base.counterparty = other.merchant
        if (self._category_name(base.category_id) in ("金融-其他", "未分类-其他")
                and self._category_name(other.category_id)
                not in ("金融-其他", "未分类-其他", None)):
            base.category_id = other.category_id
        sids = sorted({s for s in (base.source_event_id,
                                   other.source_event_id) if s})
        if sids:
            base.source_event_id = "|".join(sids)
            base.source_group_id = self._group_id(base.source_event_id, base.amount)
        base.raw_data = self._merge_raw(base.raw_data, other.raw_data,
                                        other.source)

    @staticmethod
    def _is_generic_merchant(name: str) -> bool:
        if not name:
            return True
        if name.startswith(("退货", "退款", "信用卡消费", "消费")):
            return True
        return any(b in name for b in get_bank_names())

    @staticmethod
    def _merge_raw(base_raw: str, other_raw: str, other_source: str) -> str:
        def load(raw):
            try:
                v = json.loads(raw or "{}")
                return v if isinstance(v, dict) else {}
            except Exception:
                return {}
        base = load(base_raw)
        for k, v in load(other_raw).items():
            base.setdefault(k if k not in base else other_source, v)
        return json.dumps(base, ensure_ascii=False)

    def _category_name(self, category_id) -> Optional[str]:
        if not category_id:
            return None
        row = self.db.fetchone("SELECT name FROM categories WHERE id=?",
                               (category_id,))
        return row["name"] if row else None

    @staticmethod
    def _dedup(flows: list) -> list:
        """同一事件 id 或完全相同的交易内容只保留一条。

        合并微信库会同时导出双侧消息，事件 id 不同但时间、金额、
        商户、账户完全一致，必须按内容再兜底一次。
        """
        seen = set()
        result = []
        for f in flows:
            key = (f.source, f.tx_date, (f.tx_time or "")[:19],
                   round(f.amount, 2), f.account_id, f.direction)
            if key in seen:
                continue
            seen.add(key)
            result.append(f)
        return result
