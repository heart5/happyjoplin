# 个人财务系统 — 数据类

from dataclasses import dataclass
from typing import Optional


@dataclass
class Account:
    id: int
    book: str = "personal"    # personal / work
    name: str = ""
    type: str = "bank"        # bank / payment / credit_card / loan / investment / cash
    bank: Optional[str] = None
    card_suffix: Optional[str] = None
    institution: Optional[str] = None
    platform: Optional[str] = None
    currency: str = "CNY"
    opening_balance: float = 0.0
    opening_date: Optional[str] = None
    aliases: Optional[str] = None
    is_active: bool = True
    notes: Optional[str] = None

    @property
    def is_liability(self) -> bool:
        return self.type in ("credit_card", "loan")


@dataclass
class AccountFlow:
    id: int = 0
    book: str = "personal"
    tx_date: str = ""
    tx_time: Optional[str] = None
    amount: float = 0.0
    account_id: Optional[int] = None
    direction: str = "outflow"   # inflow / outflow
    tx_type: str = "consumption"
    category_id: Optional[int] = None
    merchant: Optional[str] = None
    description: Optional[str] = None
    counterparty: Optional[str] = None
    source: str = "manual"       # sms / wechat_main / wechat_work / manual
    source_event_id: Optional[str] = None
    dedup_key: Optional[str] = None
    source_group_id: Optional[str] = None
    pair_key: Optional[str] = None
    linked_flow_id: Optional[int] = None
    raw_data: Optional[str] = None
    is_reconciled: bool = False
    notes: Optional[str] = None


@dataclass
class AccountBalance:
    account_id: int
    year: int
    month: int
    opening_balance: float = 0.0
    closing_balance: float = 0.0
    total_inflow: float = 0.0
    total_outflow: float = 0.0
    is_estimated: bool = True


@dataclass
class EnrichedFlow:
    """含账户名/分类名的展示用流水。"""
    flow: AccountFlow
    account_name: str = ""
    account_type: str = ""
    category_name: Optional[str] = None
    linked_account_name: Optional[str] = None
