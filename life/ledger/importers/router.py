# 自动归类引擎 — 微信+SMS 归并、账户匹配、路由决策

import json
import re
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from ..db import Database
from ..accounts import AccountManager
from ..cloudcfg import get_merge_window, get_loan_platforms
from ..models import Account, AccountFlow

__all__ = ["FlowRouter"]

MERGE_WINDOW = get_merge_window()


class FlowRouter:
    """将原始财务事件归并为 account_flows。

    核心逻辑：
    1. 归并：同笔交易的多个通知 (WeChat paid + SMS bank) 合并
    2. 账户匹配：payment_method/bank+card_suffix → account_id
    3. 路由：单条分录（常规收支）vs 双条分录（贷款放款/还款）
    """

    def __init__(self, db: Database, acct_mgr: AccountManager):
        self.db = db
        self.acct_mgr = acct_mgr
        self._cat_cache = {}  # category_name → category_id

    @staticmethod
    def _detect_loan_platform(text: str):
        """从交易描述中检测实际贷款平台名。"""
        if not text:
            return None
        for p in get_loan_platforms():
            if p in text:
                return p
        return None

    def route_events(self, wechat_events: list, sms_records: list) -> list:
        """主入口：归并 + 匹配 + 路由。返回 AccountFlow 列表。"""
        flows = []
        used_sms = set()

        # 建立 WeChat 事件索引: (date, amount) → [(event, idx)]
        wx_index = defaultdict(list)
        for i, evt in enumerate(wechat_events):
            date = (evt.get("time") or "")[:10]
            amt = round(evt.get("amount", 0), 2)
            if amt > 0:
                wx_index[(date, amt)].append((evt, i))

        used_wx = set()

        # Phase 1: SMS 找 WeChat 归并
        for si, sms in enumerate(sms_records):
            date = (sms.get("time") or "")[:10]
            amt = round(sms.get("amount", 0), 2)
            if amt <= 0:
                continue

            candidates = wx_index.get((date, amt), [])
            matched = None
            for wx_evt, wi in candidates:
                if wi in used_wx:
                    continue
                # 内部转账（零钱提现/充值）不归并，它们是跨账户的不同交易腿
                if wx_evt.get("category") == "内部-转账":
                    continue
                # 检查时间窗口
                if self._time_diff_seconds(wx_evt.get("time", ""), sms.get("time", "")) <= MERGE_WINDOW:
                    matched = (wx_evt, wi)
                    break

            if matched:
                wx_evt, wi = matched
                used_wx.add(wi)
                used_sms.add(si)
                # 归并：payment_method 从微信取，card_suffix 从短信取
                merged = self._merge_event(wx_evt, sms)
                flows.append(merged)
            else:
                # SMS 独立路由
                f = self._route_single_sms(sms)
                if f:
                    flows.extend(f if isinstance(f, list) else [f])
                used_sms.add(si)

        # Phase 2: 未被归并的 WeChat 事件
        for i, evt in enumerate(wechat_events):
            if i in used_wx:
                continue
            amt = round(evt.get("amount", 0), 2)
            if amt <= 0:
                continue
            f = self._route_single_wechat(evt)
            if f:
                flows.append(f)

        # Phase 3: 未被处理的 SMS（兜底）
        for si, sms in enumerate(sms_records):
            if si in used_sms:
                continue
            f = self._route_single_sms(sms)
            if f:
                flows.extend(f if isinstance(f, list) else [f])

        # 去重：同一 account_id + amount + 日期 ±1天 + direction 的合并
        flows = self._dedup_flows(flows)

        # 按时间排序
        flows.sort(key=lambda f: f.tx_date or "")
        return flows

    # ── 归并 ──

    def _merge_event(self, wx_evt: dict, sms: dict) -> AccountFlow:
        """归并微信和短信通知为一条流水。"""
        amt = round(wx_evt.get("amount", 0), 2)
        direction = "outflow" if wx_evt.get("direction", "支出") == "支出" else "inflow"
        payment_method = wx_evt.get("payment_method", "") or sms.get("payment_method", "")
        card_suffix = sms.get("card_suffix", "")
        org = sms.get("payment_method", "")
        merchant = wx_evt.get("merchant", "") or sms.get("merchant", "")

        account = self._resolve_account(payment_method, card_suffix, org)
        cat_name = wx_evt.get("category", "未分类-其他")
        cat_id = self._get_category_id(cat_name)
        group_id = self._make_group_id(wx_evt, sms)

        return AccountFlow(
            tx_date=(wx_evt.get("time") or "")[:10],
            tx_time=wx_evt.get("time", ""),
            amount=amt,
            account_id=account.id if account else 0,
            direction=direction,
            tx_type="expense" if direction == "outflow" else "income",
            category_id=cat_id,
            merchant=merchant,
            counterparty=merchant,
            source="wechat",
            source_group_id=group_id,
            raw_data=json.dumps({"wechat": wx_evt.get("source_text", ""), "sms": sms.get("source_text", "")},
                                ensure_ascii=False),
        )

    # ── 单条 WeChat 路由 ──

    def _route_single_wechat(self, evt: dict) -> Optional[AccountFlow]:
        direction = "outflow" if evt.get("direction", "支出") == "支出" else "inflow"
        payment_method = evt.get("payment_method", "")
        merchant = evt.get("merchant", "")

        cat_name = evt.get("category", "未分类-其他")
        # 收入类（转账入账）→ inflow
        if direction == "inflow":
            account = self.acct_mgr.get_wechat_wallet()
        else:
            account = self._resolve_account(payment_method, "", "")

        if account is None:
            return None

        return AccountFlow(
            tx_date=(evt.get("time") or "")[:10],
            tx_time=evt.get("time", ""),
            amount=round(evt.get("amount", 0), 2),
            account_id=account.id,
            direction=direction,
            tx_type="expense" if direction == "outflow" else "income",
            category_id=self._get_category_id(cat_name),
            merchant=merchant,
            counterparty=merchant,
            source="wechat",
            source_group_id=self._make_group_id(evt),
            raw_data=json.dumps({"wechat": evt.get("source_text", "")}, ensure_ascii=False),
        )

    # ── 单条 SMS 路由 ──

    def _route_single_sms(self, sms: dict) -> Optional[list]:
        """SMS 可能产生 1 条（常规）或 2 条（贷款放款/还款）流水。"""
        amt = round(sms.get("amount", 0), 2)
        if amt <= 0:
            return None

        is_loan = sms.get("is_loan", False)
        category = sms.get("category", "未分类-其他")
        direction = sms.get("direction", "支出")
        org = sms.get("payment_method", "")
        card_suffix = sms.get("card_suffix", "")
        merchant = sms.get("merchant", "")

        if is_loan:
            # 从交易描述检测实际贷款平台（银行短信中可能含贷款平台名如中邮消费金融还款）
            loan_org = self._detect_loan_platform(merchant) or org
            return self._route_loan(sms, amt, category, org, card_suffix, merchant, loan_org)

        # 常规银行交易
        account = self.acct_mgr.match_by_bank_sms(org, card_suffix)
        if account is None:
            return None

        f_dir = "outflow" if direction == "支出" else "inflow"
        tx_type = "expense" if direction == "支出" else "income"

        flows = [AccountFlow(
            tx_date=(sms.get("time") or "")[:10],
            tx_time=sms.get("time", ""),
            amount=amt,
            account_id=account.id,
            direction=f_dir,
            tx_type=tx_type,
            category_id=self._get_category_id(category),
            merchant=merchant,
            counterparty=merchant,
            source="sms",
            source_group_id=self._make_group_id(sms),
            raw_data=json.dumps({"sms": sms.get("source_text", "")}, ensure_ascii=False),
        )]

        # 零钱充值：银行卡→微信零钱，同步生成微信零钱入账
        source_text = sms.get("source_text", "")
        if "零钱充值" in source_text and f_dir == "outflow":
            wx_acct = self.acct_mgr.get_wechat_wallet()
            if wx_acct:
                flows.append(AccountFlow(
                    tx_date=(sms.get("time") or "")[:10],
                    tx_time=sms.get("time", ""),
                    amount=amt,
                    account_id=wx_acct.id,
                    direction="inflow",
                    tx_type="transfer",
                    category_id=self._get_category_id("内部-转账"),
                    merchant="零钱充值",
                    counterparty="零钱充值",
                    source="sms",
                    source_group_id=self._make_group_id(sms),
                    raw_data=json.dumps({"sms": source_text}, ensure_ascii=False),
                ))

        # 信用卡还款：储蓄卡→信用卡，同步生成信用卡入账（负债减少）
        tx_type_detail = sms.get("tx_type_detail", "")
        is_cc_repayment = (
            tx_type_detail == "transfer" and account.type == "bank_debit" and f_dir == "outflow"
        ) or (
            "信用卡还款" in source_text and "还款" in source_text
        )
        if is_cc_repayment:
            bank = AccountManager._normalize_bank_name(org)
            if bank:
                credit_acct = self._find_credit_card_for_bank(bank)
                if credit_acct and not self._has_credit_inflow(credit_acct.id, amt, sms.get("time", "")):
                    flows.append(AccountFlow(
                        tx_date=(sms.get("time") or "")[:10],
                        tx_time=sms.get("time", ""),
                        amount=amt,
                        account_id=credit_acct.id,
                        direction="inflow",
                        tx_type="transfer",
                        category_id=self._get_category_id("内部-转账"),
                        merchant="信用卡还款入账",
                        counterparty=account.name,
                        source="sms",
                        source_group_id=self._make_group_id(sms),
                        raw_data=json.dumps({"sms": source_text}, ensure_ascii=False),
                    ))

        return flows

    def _route_loan(self, sms: dict, amt: float, category: str,
                    org: str, card_suffix: str, merchant: str,
                    loan_org: str = None) -> Optional[list]:
        """贷款放款/还款 → 双条分录。"""
        loan_acct = self.acct_mgr.get_or_create_loan(loan_org or org)
        source_text = sms.get("source_text", "")
        raw_data_loan = json.dumps({"sms": source_text}, ensure_ascii=False) if source_text else None

        if category == "借贷-放款":
            # 放款：银行 inflow + 负债 outflow（负债增加）
            bank_acct = self.acct_mgr.match_by_bank_sms(org, card_suffix)
            if bank_acct is None:
                # 无法匹配银行账户，只记贷款端
                return [AccountFlow(
                    tx_date=(sms.get("time") or "")[:10],
                    tx_time=sms.get("time", ""),
                    amount=amt,
                    account_id=loan_acct.id,
                    direction="outflow",  # 负债增加
                    tx_type="loan_disbursement",
                    category_id=self._get_category_id(category),
                    merchant=merchant,
                    counterparty=org,
                    source="sms", raw_data=raw_data_loan,
                    source_group_id=self._make_group_id(sms),
                )]

            group_id = self._make_group_id(sms)
            f1 = AccountFlow(
                tx_date=(sms.get("time") or "")[:10],
                tx_time=sms.get("time", ""),
                amount=amt,
                account_id=bank_acct.id,
                direction="inflow",  # 钱进银行账户
                tx_type="loan_disbursement",
                category_id=self._get_category_id(category),
                merchant=merchant,
                counterparty=org,
                source="sms", raw_data=raw_data_loan,
                source_group_id=group_id,
            )
            f2 = AccountFlow(
                tx_date=(sms.get("time") or "")[:10],
                tx_time=sms.get("time", ""),
                amount=amt,
                account_id=loan_acct.id,
                direction="outflow",  # 负债增加
                tx_type="loan_disbursement",
                category_id=self._get_category_id(category),
                merchant=merchant,
                counterparty=bank_acct.name if bank_acct else "",
                source="sms", raw_data=raw_data_loan,
                source_group_id=group_id,
            )
            return [f1, f2]

        elif category in ("借贷-还款", "借贷-其他"):
            # 还款：银行 outflow + 负债 inflow（负债减少）
            bank_acct = self.acct_mgr.match_by_bank_sms(org, card_suffix)
            if bank_acct is None:
                return [AccountFlow(
                    tx_date=(sms.get("time") or "")[:10],
                    tx_time=sms.get("time", ""),
                    amount=amt,
                    account_id=loan_acct.id,
                    direction="inflow",  # 负债减少
                    tx_type="loan_repayment",
                    category_id=self._get_category_id(category),
                    merchant=merchant,
                    counterparty=org,
                    source="sms", raw_data=raw_data_loan,
                    source_group_id=self._make_group_id(sms),
                )]

            group_id = self._make_group_id(sms)
            f1 = AccountFlow(
                tx_date=(sms.get("time") or "")[:10],
                tx_time=sms.get("time", ""),
                amount=amt,
                account_id=bank_acct.id,
                direction="outflow",  # 钱从银行出去
                tx_type="loan_repayment",
                category_id=self._get_category_id(category),
                merchant=merchant,
                counterparty=org,
                source="sms", raw_data=raw_data_loan,
                source_group_id=group_id,
            )
            f2 = AccountFlow(
                tx_date=(sms.get("time") or "")[:10],
                tx_time=sms.get("time", ""),
                amount=amt,
                account_id=loan_acct.id,
                direction="inflow",  # 负债减少
                tx_type="loan_repayment",
                category_id=self._get_category_id(category),
                merchant=merchant,
                counterparty=bank_acct.name if bank_acct else "",
                source="sms", raw_data=raw_data_loan,
                source_group_id=group_id,
            )
            return [f1, f2]

        return None

    # ── 信用卡双条分录 ──

    def _find_credit_card_for_bank(self, bank_name: str) -> Optional[Account]:
        """找到指定银行的信用卡账户（还款双条分录用）。"""
        row = self.db.fetchone(
            "SELECT * FROM accounts WHERE type='bank_credit' AND bank=? AND is_active=1 ORDER BY id LIMIT 1",
            (bank_name,),
        )
        if row:
            return AccountManager._row_to_account(row)
        return None

    def _has_credit_inflow(self, credit_acct_id: int, amount: float, tx_date: str, days: int = 2) -> bool:
        """检查信用卡在时间窗口内是否已有同等金额的还款入账（防重复）。"""
        try:
            dt = datetime.strptime(tx_date[:10], "%Y-%m-%d")
            start = (dt - timedelta(days=days)).strftime("%Y-%m-%d")
            end = (dt + timedelta(days=days)).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return False
        row = self.db.fetchone(
            """SELECT COUNT(*) as cnt FROM account_flows
               WHERE account_id=? AND direction='inflow' AND amount=?
               AND tx_date BETWEEN ? AND ?""",
            (credit_acct_id, amount, start, end),
        )
        return row and row["cnt"] > 0

    # ── 辅助 ──

    def _resolve_account(self, payment_method: str, card_suffix: str, org: str) -> Optional[Account]:
        """统一账户解析入口。"""
        if not payment_method:
            return None

        # 先尝试按 payment_method 匹配
        acct = self.acct_mgr.match_by_payment_method(payment_method, card_suffix)
        if acct:
            return acct

        # 降级：按银行短信匹配
        if org:
            acct = self.acct_mgr.match_by_bank_sms(org, card_suffix)
            if acct:
                return acct

        # 兜底：微信零钱
        return self.acct_mgr.get_wechat_wallet()

    def _get_category_id(self, name: str) -> Optional[int]:
        if not name:
            return None
        if name in self._cat_cache:
            return self._cat_cache[name]

        row = self.db.fetchone("SELECT id FROM categories WHERE name=?", (name,))
        if row:
            self._cat_cache[name] = row["id"]
            return row["id"]

        # 自动创建
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
    def _make_group_id(*items) -> str:
        raw = "|".join(str(i.get("time", "") or "") + str(round(i.get("amount", 0), 2)) for i in items if i)
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    @staticmethod
    def _dedup_flows(flows: list) -> list:
        """去除重复流水。

        精确匹配：相同 account + amount + date + direction。
        贷款近匹配：相同 account + amount + direction，日期在 ±2 天内。
        """
        seen_exact = set()
        seen_loan = []  # [(account_id, amount, direction, tx_date)]
        result = []
        for f in flows:
            amt = round(f.amount, 2)
            key = (f.account_id, amt, f.tx_date, f.direction)

            # 精确匹配
            if key in seen_exact:
                continue
            seen_exact.add(key)

            # 贷款近匹配：同一 account + amount + direction 且在 ±3 天内
            is_loan_type = f.tx_type in ("loan_repayment", "loan_disbursement")
            if is_loan_type:
                try:
                    f_date = datetime.strptime(f.tx_date[:10], "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    result.append(f)
                    continue

                is_dup = False
                for sid, s_amt, s_dir, s_date in seen_loan:
                    if sid == f.account_id and s_amt == amt and s_dir == f.direction:
                        d_diff = abs((f_date - s_date).days)
                        if d_diff <= 3:
                            is_dup = True
                            break

                if is_dup:
                    continue
                seen_loan.append((f.account_id, amt, f.direction, f_date))

            result.append(f)
        return result
