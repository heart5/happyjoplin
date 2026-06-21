# 自动归类引擎 — 微信+SMS 归并、账户匹配、路由决策

import json
import re
import hashlib
from collections import defaultdict
from datetime import datetime
from typing import Optional

from ..db import Database
from ..accounts import AccountManager
from ..cloudcfg import get_merge_window
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
        payment_method = wx_evt.get("payment_method", "")
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
            return self._route_loan(sms, amt, category, org, card_suffix, merchant)

        # 常规银行交易
        account = self.acct_mgr.match_by_bank_sms(org, card_suffix)
        if account is None:
            return None

        f_dir = "outflow" if direction == "支出" else "inflow"
        tx_type = "expense" if direction == "支出" else "income"

        return [AccountFlow(
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

    def _route_loan(self, sms: dict, amt: float, category: str,
                    org: str, card_suffix: str, merchant: str) -> Optional[list]:
        """贷款放款/还款 → 双条分录。"""
        loan_acct = self.acct_mgr.get_or_create_loan(org)

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
                    source="sms",
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
                source="sms",
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
                source="sms",
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
                    source="sms",
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
                source="sms",
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
                source="sms",
                source_group_id=group_id,
            )
            return [f1, f2]

        return None

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
        """去除完全重复的流水（相同 account + amount + date + direction）。"""
        seen = set()
        result = []
        for f in flows:
            key = (f.account_id, round(f.amount, 2), f.tx_date, f.direction)
            if key not in seen:
                seen.add(key)
                result.append(f)
        return result
