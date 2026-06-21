# 个人财务系统 — 账户管理

import re
from typing import Optional

from .db import Database
from .models import Account
from .cloudcfg import (
    get_bank_short_codes as _get_bank_short_codes,
    get_bank_names as _get_bank_names,
    get_payment_method_map as _get_payment_method_map,
)

__all__ = ["AccountManager"]


class AccountManager:
    """账户 CRUD + 自动匹配。"""

    ACCOUNT_TYPE_LABELS = {
        "bank_debit": "储蓄卡",
        "bank_credit": "信用卡",
        "wechat_wallet": "微信零钱",
        "alipay": "支付宝",
        "loan": "贷款",
        "cash": "现金",
    }

    def __init__(self, db: Database):
        self.db = db

    # ── 查询 ──

    def list_accounts(self, type_filter: str = None, active_only: bool = True) -> list:
        """列出账户。"""
        sql = "SELECT * FROM accounts WHERE 1=1"
        params = []
        if type_filter:
            sql += " AND type=?"
            params.append(type_filter)
        if active_only:
            sql += " AND is_active=1"
        sql += " ORDER BY type, bank, id"
        rows = self.db.fetchall(sql, params)
        return [self._row_to_account(r) for r in rows]

    def get_account(self, account_id: int) -> Optional[Account]:
        row = self.db.fetchone("SELECT * FROM accounts WHERE id=?", (account_id,))
        return self._row_to_account(row) if row else None

    def get_account_by_type(self, acct_type: str) -> Optional[Account]:
        """按类型获取唯一账户（微信零钱/支付宝等）。"""
        row = self.db.fetchone("SELECT * FROM accounts WHERE type=?", (acct_type,))
        return self._row_to_account(row) if row else None

    def get_wechat_wallet(self) -> Optional[Account]:
        return self.get_account_by_type("wechat_wallet")

    def get_alipay(self) -> Optional[Account]:
        return self.get_account_by_type("alipay")

    # ── 自动匹配 ──

    def match_by_payment_method(self, method: str, card_suffix: str = None) -> Optional[Account]:
        """根据微信支付方式匹配账户。

        例如 "广发信用卡" → bank_credit + 广发银行。
        如果 card_suffix 有值，优先匹配尾号。
        """
        pmap = _get_payment_method_map()
        rule = pmap.get(method)
        if not rule:
            # 尝试模糊匹配银行名
            for key, val in pmap.items():
                if key in method or method in key:
                    rule = val
                    break
        if not rule:
            return None

        acct_type, bank = rule
        if bank and card_suffix:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE type=? AND bank=? AND card_suffix=? AND is_active=1",
                (acct_type, bank, card_suffix),
            )
            if row:
                return self._row_to_account(row)

        if bank:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE type=? AND bank=? AND is_active=1 ORDER BY id LIMIT 1",
                (acct_type, bank),
            )
            if row:
                return self._row_to_account(row)

        return self.get_account_by_type(acct_type)

    def match_by_bank_sms(self, org: str, card_suffix: str) -> Optional[Account]:
        """根据银行短信匹配账户。

        org 可能为 "招商银行" / "广发银行" 等银行名或短号码。
        card_suffix 为卡号尾号4位。
        """
        bank = self._normalize_bank_name(org)
        if not bank:
            return None

        if card_suffix:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE bank=? AND card_suffix=? AND is_active=1",
                (bank, card_suffix),
            )
            if row:
                return self._row_to_account(row)

        row = self.db.fetchone(
            "SELECT * FROM accounts WHERE bank=? AND is_active=1 ORDER BY id LIMIT 1",
            (bank,),
        )
        return self._row_to_account(row) if row else None

    def get_or_create_loan(self, institution: str) -> Account:
        """获取或自动创建贷款账户。"""
        row = self.db.fetchone(
            "SELECT * FROM accounts WHERE type='loan' AND institution=? AND is_active=1",
            (institution,),
        )
        if row:
            return self._row_to_account(row)

        acct_id = self.db.insert("accounts", {
            "name": institution,
            "type": "loan",
            "institution": institution,
            "notes": "自动创建",
        })
        return self.get_account(acct_id)

    def get_or_create_bank_card(self, bank: str, card_suffix: str, card_type: str = "bank_credit") -> Account:
        """获取或自动创建银行卡账户。"""
        row = self.db.fetchone(
            "SELECT * FROM accounts WHERE bank=? AND card_suffix=?",
            (bank, card_suffix),
        )
        if row:
            return self._row_to_account(row)

        name = f"{bank}-尾号{card_suffix}"
        acct_id = self.db.insert("accounts", {
            "name": name,
            "type": card_type,
            "bank": bank,
            "card_suffix": card_suffix,
            "notes": "自动创建",
        })
        return self.get_account(acct_id)

    # ── 手动管理 ──

    def add_account(self, name: str, acct_type: str, bank: str = None,
                    card_suffix: str = None, institution: str = None,
                    notes: str = None) -> Account:
        acct_id = self.db.insert("accounts", {
            "name": name,
            "type": acct_type,
            "bank": bank,
            "card_suffix": card_suffix,
            "institution": institution,
            "notes": notes or "",
        })
        return self.get_account(acct_id)

    def deactivate(self, account_id: int):
        self.db.update("accounts", {"is_active": 0, "updated_at": "datetime('now','localtime')"},
                       {"id": account_id})

    # ── 辅助 ──

    @staticmethod
    def _row_to_account(row: dict) -> Account:
        return Account(
            id=row["id"], name=row["name"], type=row["type"],
            bank=row.get("bank"), card_suffix=row.get("card_suffix"),
            institution=row.get("institution"),
            currency=row.get("currency", "CNY"),
            is_active=bool(row.get("is_active", 1)),
            notes=row.get("notes"),
        )

    @staticmethod
    def _normalize_bank_name(org: str) -> Optional[str]:
        """将短信来源统一为银行标准名。"""
        short_codes = _get_bank_short_codes()
        if org in short_codes:
            return short_codes[org]

        banks = _get_bank_names()
        for bank in banks:
            if bank in org:
                return bank
        return None
