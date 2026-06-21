# 个人财务系统 — 数据类

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Account:
    id: int
    name: str
    type: str          # bank_debit / bank_credit / wechat_wallet / alipay / loan / cash
    bank: Optional[str] = None
    card_suffix: Optional[str] = None
    institution: Optional[str] = None
    currency: str = "CNY"
    is_active: bool = True
    notes: Optional[str] = None


@dataclass
class AccountFlow:
    id: int = 0
    tx_date: str = ""
    tx_time: Optional[str] = None
    amount: float = 0.0
    account_id: int = 0
    direction: str = "outflow"   # inflow / outflow
    linked_flow_id: Optional[int] = None
    tx_type: str = "expense"     # expense / income / transfer / loan_disbursement / loan_repayment
    category_id: Optional[int] = None
    merchant: Optional[str] = None
    description: Optional[str] = None
    counterparty: Optional[str] = None
    source: str = "manual"       # wechat / sms / manual / alipay
    source_group_id: Optional[str] = None
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
