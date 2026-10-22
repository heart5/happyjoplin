# 个人财务系统 — 账户管理（book 感知）

import re
from typing import Optional

from .db import Database
from .models import Account
from .schema import LOAN_ALIASES, make_identity_key
from .cloudcfg import (
    get_bank_short_codes as _get_bank_short_codes,
    get_bank_names as _get_bank_names,
    get_payment_method_map as _get_payment_method_map,
)

__all__ = ["AccountManager"]

_OLD_TYPE_MAP = {
    "bank_credit": "credit_card",
    "bank_debit": "bank",
    "wechat_wallet": "payment",
    "alipay": "payment",
    "loan": "loan",
    "cash": "cash",
}


class AccountManager:
    """账户 CRUD + 自动匹配。"""

    ACCOUNT_TYPE_LABELS = {
        "bank": "储蓄卡",
        "credit_card": "信用卡",
        "payment": "支付平台",
        "loan": "贷款",
        "investment": "投资",
        "cash": "现金",
    }

    def __init__(self, db: Database):
        self.db = db

    # ── 查询 ──

    def list_accounts(self, book: str = None, type_filter: str = None,
                      active_only: bool = True) -> list:
        sql = "SELECT * FROM accounts WHERE 1=1"
        params = []
        if book:
            sql += " AND book=?"
            params.append(book)
        if type_filter:
            sql += " AND type=?"
            params.append(type_filter)
        if active_only:
            sql += " AND is_active=1"
        sql += " ORDER BY book, type, bank, id"
        rows = self.db.fetchall(sql, params)
        return [self._row_to_account(r) for r in rows]

    def get_account(self, account_id: int) -> Optional[Account]:
        row = self.db.fetchone("SELECT * FROM accounts WHERE id=?", (account_id,))
        return self._row_to_account(row) if row else None

    def get_account_by_type(self, acct_type: str, book: str = "personal",
                            platform: str = None) -> Optional[Account]:
        sql = "SELECT * FROM accounts WHERE type=? AND book=? AND is_active=1"
        params = [acct_type, book]
        if platform:
            sql += " AND platform=?"
            params.append(platform)
        sql += " ORDER BY id LIMIT 1"
        row = self.db.fetchone(sql, params)
        return self._row_to_account(row) if row else None

    def get_wechat_wallet(self, book: str = "personal") -> Optional[Account]:
        return self.get_account_by_type("payment", book, "wechat")

    def get_alipay(self, book: str = "personal") -> Optional[Account]:
        return self.get_account_by_type("payment", book, "alipay")

    # ── 自动匹配 ──

    def match_by_payment_method(self, method: str, card_suffix: str = None,
                                book: str = "personal") -> Optional[Account]:
        """根据微信支付方式匹配账户。"""
        if not method:
            return None
        if "支付宝" in method:
            return self.get_account_by_type("payment", book, "alipay")
        if "零钱" in method or "微信" in method:
            return self.get_account_by_type("payment", book, "wechat")

        pmap = _get_payment_method_map()
        rule = pmap.get(method)
        if not rule:
            for key, val in pmap.items():
                if key in method or method in key:
                    rule = val
                    break
        if not rule:
            return None

        acct_type, bank = rule
        acct_type = _OLD_TYPE_MAP.get(acct_type, acct_type)
        if acct_type == "payment":
            return self.get_account_by_type("payment", book, "wechat")

        if acct_type == "loan":
            inst = LOAN_ALIASES.get(bank or method, bank or method)
            return self.get_or_create_loan(inst, book)

        if bank and card_suffix:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE book=? AND type=? AND bank=? AND card_suffix=? AND is_active=1",
                (book, acct_type, bank, card_suffix),
            )
            if row:
                return self._row_to_account(row)

        if bank:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE book=? AND type=? AND bank=? AND is_active=1 ORDER BY id LIMIT 1",
                (book, acct_type, bank),
            )
            if row:
                return self._row_to_account(row)
            if card_suffix:
                return self.get_or_create_bank_card(bank, card_suffix, acct_type, book)
        return None

    def match_by_bank_sms(self, org: str, card_suffix: str,
                          book: str = "personal",
                          prefer_type: str = None,
                          allow_create: bool = True) -> Optional[Account]:
        """根据银行短信匹配账户。prefer_type 可为 credit_card / bank。

        allow_create=False 用于通知等弱来源：绝不新建银行账户，
        且带尾号时不做同银行其他卡的降级匹配。
        """
        bank = self._normalize_bank_name(org)
        if not bank:
            return None

        if card_suffix:
            sql = ("SELECT * FROM accounts WHERE book=? AND bank=? AND card_suffix=?"
                   " AND is_active=1")
            params = [book, bank, card_suffix]
            if prefer_type:
                sql += " AND type=?"
                params.append(prefer_type)
            sql += " ORDER BY id LIMIT 1"
            row = self.db.fetchone(sql, params)
            if row:
                return self._row_to_account(row)
            if not allow_create:
                return None
            if prefer_type:
                return self.get_or_create_bank_card(bank, card_suffix, prefer_type, book)

        if prefer_type:
            row = self.db.fetchone(
                "SELECT * FROM accounts WHERE book=? AND bank=? AND type=? AND is_active=1 ORDER BY id LIMIT 1",
                (book, bank, prefer_type),
            )
        else:
            row = self.db.fetchone(
                """SELECT * FROM accounts WHERE book=? AND bank=? AND is_active=1
                   ORDER BY CASE WHEN type='bank' THEN 0 ELSE 1 END, id LIMIT 1""",
                (book, bank),
            )
        return self._row_to_account(row) if row else None

    def get_or_create_loan(self, institution: str, book: str = "personal") -> Account:
        institution = LOAN_ALIASES.get(institution, institution)
        row = self.db.fetchone(
            "SELECT * FROM accounts WHERE book=? AND type='loan' AND institution=? AND is_active=1",
            (book, institution),
        )
        if row:
            return self._row_to_account(row)
        return self.add_account(
            name=institution, acct_type="loan", institution=institution,
            book=book, notes="自动创建",
        )

    def get_or_create_bank_card(self, bank: str, card_suffix: str,
                                card_type: str = "credit_card",
                                book: str = "personal") -> Account:
        row = self.db.fetchone(
            "SELECT * FROM accounts WHERE book=? AND bank=? AND card_suffix=?",
            (book, bank, card_suffix),
        )
        if row:
            return self._row_to_account(row)
        label = "信用卡" if card_type == "credit_card" else "储蓄卡"
        return self.add_account(
            name=f"{bank}{label}-{card_suffix}", acct_type=card_type, bank=bank,
            card_suffix=card_suffix, book=book, notes="自动创建",
        )

    # ── 手动管理 ──

    def add_account(self, name: str, acct_type: str, book: str = "personal",
                    bank: str = None, card_suffix: str = None,
                    institution: str = None, platform: str = None,
                    opening_balance: float = 0.0, opening_date: str = None,
                    aliases: str = None, notes: str = None) -> Account:
        key = make_identity_key(book, acct_type, bank, card_suffix, institution, platform)
        row = self.db.fetchone("SELECT * FROM accounts WHERE identity_key=?", (key,))
        if row:
            return self._row_to_account(row)
        acct_id = self.db.insert("accounts", {
            "book": book,
            "name": name,
            "type": acct_type,
            "bank": bank,
            "card_suffix": card_suffix,
            "institution": institution,
            "platform": platform,
            "opening_balance": opening_balance,
            "opening_date": opening_date,
            "aliases": aliases,
            "notes": notes or "",
            "identity_key": key,
        })
        return self.get_account(acct_id)

    def deactivate(self, account_id: int):
        self.db.update("accounts", {"is_active": 0}, {"id": account_id})

    def set_anchor(self, account_id: int, balance: float, date_str: str):
        """设置余额锚点：date_str 起从该余额滚算（含之后的流水）。"""
        self.db.update("accounts", {
            "opening_balance": round(float(balance), 2),
            "opening_date": date_str,
        }, {"id": account_id})

    # ── 辅助 ──

    @staticmethod
    def _row_to_account(row: dict) -> Account:
        return Account(
            id=row["id"], book=row.get("book", "personal"), name=row["name"],
            type=row["type"], bank=row.get("bank"),
            card_suffix=row.get("card_suffix"),
            institution=row.get("institution"), platform=row.get("platform"),
            currency=row.get("currency", "CNY"),
            opening_balance=row.get("opening_balance") or 0.0,
            opening_date=row.get("opening_date"),
            aliases=row.get("aliases"),
            is_active=bool(row.get("is_active", 1)),
            notes=row.get("notes"),
        )

    @staticmethod
    def _normalize_bank_name(org: str) -> Optional[str]:
        if not org:
            return None
        short_codes = _get_bank_short_codes()
        if org in short_codes:
            return short_codes[org]
        banks = _get_bank_names()
        for bank in banks:
            if bank in org:
                return bank
        return None
