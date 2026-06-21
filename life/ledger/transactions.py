# 个人财务系统 — 交易管理与余额计算

import json
from collections import defaultdict
from datetime import datetime
from typing import Optional

import pathmagic
with pathmagic.context():
    from func.logme import log

from .db import Database
from .accounts import AccountManager
from .importers.router import FlowRouter
from .models import Account, AccountFlow, AccountBalance

__all__ = ["TransactionManager"]


class TransactionManager:
    """交易导入、余额计算、净资产快照。"""

    def __init__(self, db: Database, acct_mgr: AccountManager = None):
        self.db = db
        self.acct_mgr = acct_mgr or AccountManager(db)
        self.router = FlowRouter(db, self.acct_mgr)

    # ── 导入 ──

    def import_wechat(self, wechat_events: list, month_key: str = "") -> int:
        """导入微信解析结果。"""
        flows = self.router.route_events(wechat_events, [])
        return self._save_flows(flows, month_key)

    def import_sms(self, sms_records: list, month_key: str = "") -> int:
        """导入短信解析结果。"""
        flows = self.router.route_events([], sms_records)
        return self._save_flows(flows, month_key)

    def import_merged(self, wechat_events: list, sms_records: list, month_key: str = "") -> dict:
        """合并导入（推荐）：微信+短信归并后写入。"""
        flows = self.router.route_events(wechat_events, sms_records)
        count = self._save_flows(flows, month_key)

        # 按来源统计
        by_source = defaultdict(int)
        for f in flows:
            by_source[f.source] += 1

        return {
            "total_flows": count,
            "by_source": dict(by_source),
            "accounts_involved": len(set(f.account_id for f in flows)),
        }

    def _save_flows(self, flows: list, month_key: str) -> int:
        """批量写入 account_flows。"""
        if not flows:
            return 0

        # 先写所有 flow，记录 id
        inserted = []
        for f in flows:
            data = self._flow_to_dict(f)
            fid = self.db.insert("account_flows", data)
            inserted.append(fid)

        # 更新 linked_flow_id：一对一对的 flow，相邻插入的互为 linked
        i = 0
        while i < len(flows):
            if flows[i].tx_type in ("loan_disbursement", "loan_repayment") and i + 1 < len(flows):
                t1 = flows[i].tx_type
                t2 = flows[i + 1].tx_type
                if t1 == t2:
                    # 两个 flow 是同一笔 loan 交易的双条分录
                    self.db.update("account_flows",
                                   {"linked_flow_id": inserted[i + 1]},
                                   {"id": inserted[i]})
                    self.db.update("account_flows",
                                   {"linked_flow_id": inserted[i]},
                                   {"id": inserted[i + 1]})
                    i += 2
                    continue
            i += 1

        log.info(f"已写入 {len(flows)} 条流水")
        return len(flows)

    # ── 余额计算 ──

    def calculate_monthly_balance(self, account_id: int, year: int, month: int) -> AccountBalance:
        """计算指定账户的月度余额。"""
        prev = self._get_balance_record(account_id, year, month - 1)
        opening = prev.closing_balance if prev else 0.0

        flows = self.db.fetchall(
            """SELECT direction, amount, tx_type FROM account_flows
               WHERE account_id = ? AND strftime('%Y', tx_date) = ? AND strftime('%m', tx_date) = ?""",
            (account_id, str(year), f"{month:02d}"),
        )

        acct = self.acct_mgr.get_account(account_id)
        if acct is None:
            return AccountBalance(account_id, year, month, 0, 0, 0, 0, True)

        is_liability = acct.type in ("bank_credit", "loan")

        total_inflow = sum(f["amount"] for f in flows if f["direction"] == "inflow")
        total_outflow = sum(f["amount"] for f in flows if f["direction"] == "outflow")

        if is_liability:
            # 负债类：outflow 增加负债，inflow 减少负债
            closing = opening + total_outflow - total_inflow
        else:
            # 资产类：inflow 增加余额
            closing = opening + total_inflow - total_outflow

        self.db.upsert("account_balances", {
            "account_id": account_id,
            "year": year,
            "month": month,
            "opening_balance": opening,
            "closing_balance": closing,
            "total_inflow": total_inflow,
            "total_outflow": total_outflow,
            "is_estimated": 1 if prev is None or prev.is_estimated else 0,
        }, conflict_cols=["account_id", "year", "month"])

        return AccountBalance(account_id, year, month, opening, closing,
                              total_inflow, total_outflow,
                              is_estimated=(prev is None or prev.is_estimated))

    def calculate_all_balances(self, year: int, month: int) -> list:
        """为所有活跃账户计算月度余额。"""
        accounts = self.acct_mgr.list_accounts()
        results = []
        for acct in accounts:
            bal = self.calculate_monthly_balance(acct.id, year, month)
            results.append(bal)
        return results

    # ── 净资产快照 ──

    def snapshot_net_worth(self, date: str) -> dict:
        """计算指定日期的净资产快照。"""
        year, month = int(date[:4]), int(date[5:7])
        accounts = self.acct_mgr.list_accounts()

        details = {}
        total_assets = 0.0
        total_liabilities = 0.0

        for acct in accounts:
            bal = self.calculate_monthly_balance(acct.id, year, month)
            balance = bal.closing_balance
            is_liability = acct.type in ("bank_credit", "loan")

            if is_liability:
                total_liabilities += balance
            else:
                total_assets += balance

            details[acct.name] = {
                "type": acct.type,
                "balance": balance,
                "is_liability": is_liability,
            }

        net_worth = total_assets - total_liabilities

        self.db.upsert("net_worth_snapshots", {
            "snapshot_date": date,
            "total_assets": round(total_assets, 2),
            "total_liabilities": round(total_liabilities, 2),
            "net_worth": round(net_worth, 2),
            "details": json.dumps(details, ensure_ascii=False),
        }, conflict_cols=["snapshot_date"])

        return {
            "date": date,
            "total_assets": round(total_assets, 2),
            "total_liabilities": round(total_liabilities, 2),
            "net_worth": round(net_worth, 2),
            "details": details,
        }

    def set_initial_balance(self, account_id: int, year: int, month: int,
                            balance: float, notes: str = ""):
        """设定初始余额（覆盖自动计算的估计值）。"""
        self.db.upsert("account_balances", {
            "account_id": account_id,
            "year": year,
            "month": month,
            "opening_balance": balance,
            "closing_balance": balance,
            "total_inflow": 0,
            "total_outflow": 0,
            "is_estimated": 0,
            "notes": notes or "手动设定",
        }, conflict_cols=["account_id", "year", "month"])

    # ── 查询 ──

    def get_flows(self, account_id: int = None, year: int = None, month: int = None,
                  source: str = None, limit: int = 200) -> list:
        """查询流水。"""
        sql = "SELECT af.*, a.name as account_name, a.type as account_type, c.name as category_name FROM account_flows af"
        sql += " LEFT JOIN accounts a ON af.account_id = a.id"
        sql += " LEFT JOIN categories c ON af.category_id = c.id WHERE 1=1"
        params = []

        if account_id:
            sql += " AND af.account_id=?"
            params.append(account_id)
        if year:
            sql += " AND strftime('%Y', af.tx_date)=?"
            params.append(str(year))
        if month:
            sql += " AND strftime('%m', af.tx_date)=?"
            params.append(f"{month:02d}")
        if source:
            sql += " AND af.source=?"
            params.append(source)

        sql += " ORDER BY af.tx_date DESC, af.id DESC LIMIT ?"
        params.append(limit)
        return self.db.fetchall(sql, params)

    def get_account_balance(self, account_id: int, year: int, month: int) -> Optional[AccountBalance]:
        """获取指定月份的余额记录。"""
        return self._get_balance_record(account_id, year, month)

    def list_balance_history(self, account_id: int, months: int = 12) -> list:
        """查询账户的余额历史。"""
        rows = self.db.fetchall(
            """SELECT account_id, year, month, opening_balance, closing_balance,
                      total_inflow, total_outflow, is_estimated
               FROM account_balances
               WHERE account_id=? ORDER BY year DESC, month DESC LIMIT ?""",
            (account_id, months),
        )
        return [AccountBalance(**r) for r in rows]

    # ── 内部 ──

    def _get_balance_record(self, account_id: int, year: int, month: int) -> Optional[AccountBalance]:
        """读取余额记录，越界月份返回 None。"""
        if month < 1:
            return self._get_balance_record(account_id, year - 1, 12)
        if month > 12:
            return self._get_balance_record(account_id, year + 1, 1)
        row = self.db.fetchone(
            "SELECT account_id, year, month, opening_balance, closing_balance,"
            " total_inflow, total_outflow, is_estimated"
            " FROM account_balances WHERE account_id=? AND year=? AND month=?",
            (account_id, year, month),
        )
        return AccountBalance(**row) if row else None

    def _close_month(self, year: int, month: int, prev_row: dict = None) -> dict:
        """计算期末余额。"""
        acct = self.acct_mgr.get_account(prev_row["account_id"] if prev_row else 0)

    @staticmethod
    def _flow_to_dict(f: AccountFlow) -> dict:
        d = {
            "tx_date": f.tx_date,
            "amount": round(f.amount, 2),
            "account_id": f.account_id,
            "direction": f.direction,
            "tx_type": f.tx_type,
            "source": f.source,
        }
        if f.tx_time:
            d["tx_time"] = f.tx_time
        if f.category_id:
            d["category_id"] = f.category_id
        if f.merchant:
            d["merchant"] = f.merchant
        if f.counterparty:
            d["counterparty"] = f.counterparty
        if f.source_group_id:
            d["source_group_id"] = f.source_group_id
        if f.raw_data:
            d["raw_data"] = f.raw_data
        if f.description:
            d["description"] = f.description
        if f.notes:
            d["notes"] = f.notes
        return d
