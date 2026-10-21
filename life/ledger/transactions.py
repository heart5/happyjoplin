# 个人财务系统 — 交易写入、余额计算、净资产快照

import hashlib
import json
import uuid
from datetime import date
from typing import Optional

import pathmagic
with pathmagic.context():
    from func.logme import log

from .db import Database
from .accounts import AccountManager
from .importers.router import FlowRouter
from .models import Account, AccountFlow, AccountBalance

__all__ = ["TransactionManager", "make_dedup_key", "month_bounds"]


def month_bounds(month_key: str):
    year, month = int(month_key[:4]), int(month_key[5:7])
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1:04d}-01-01"
    else:
        end = f"{year:04d}-{month + 1:02d}-01"
    return start, end


def make_dedup_key(book: str, tx_date: str, tx_time: Optional[str], amount: float,
                   account_id: Optional[int], direction: str,
                   source_event_id: Optional[str]) -> str:
    raw = "|".join([
        book or "", tx_date or "", (tx_time or "")[:19],
        f"{float(amount):.2f}", str(account_id or 0), direction or "",
        source_event_id or "",
    ])
    return hashlib.sha1(raw.encode()).hexdigest()


class TransactionManager:
    """交易导入、余额计算、净资产快照。"""

    def __init__(self, db: Database, acct_mgr: AccountManager = None,
                 book: str = "personal"):
        self.db = db
        self.book = book
        self.acct_mgr = acct_mgr or AccountManager(db)
        self.router = FlowRouter(db, self.acct_mgr, book=book)

    # ── 导入 ──

    def import_wechat(self, wechat_events: list, month_key: str = "") -> int:
        return self.import_merged(wechat_events, [], month_key)

    def import_sms(self, sms_records: list, month_key: str = "") -> int:
        return self.import_merged([], sms_records, month_key)

    def import_merged(self, wechat_events: list, sms_records: list,
                      month_key: str = "", rebuild: bool = True) -> dict:
        """归并导入：先清当月非手工流水，再写入。"""
        flows = self.router.route_events(wechat_events, sms_records)
        inserted = self._save_flows(flows, clear_month=month_key if rebuild else None)
        by_source = {}
        for f in flows:
            by_source[f.source] = by_source.get(f.source, 0) + 1
        return {
            "total_flows": inserted,
            "parsed_flows": len(flows),
            "by_source": by_source,
            "accounts_involved": len({f.account_id for f in flows if f.account_id}),
            "unmatched": sum(1 for f in flows if not f.account_id),
        }

    def add_manual(self, **kwargs) -> int:
        """手工补录一条流水。"""
        f = AccountFlow(**{k: v for k, v in kwargs.items()
                           if k in AccountFlow.__dataclass_fields__})
        f.book = f.book or self.book
        f.source = "manual"
        if not f.source_event_id:
            f.source_event_id = f"manual:{uuid.uuid4().hex}"
        return self._save_flows([f])

    def _clear_month(self, conn, month_key: str) -> int:
        start, end = month_bounds(month_key)
        cur = conn.execute(
            """DELETE FROM account_flows
               WHERE book=? AND tx_date>=? AND tx_date<?
                 AND source IN ('sms','wechat_main','wechat_work')""",
            (self.book, start, end),
        )
        return cur.rowcount

    def _save_flows(self, flows: list, clear_month: str = None) -> int:
        rows = []
        for f in flows:
            d = self._flow_to_dict(f)
            d["dedup_key"] = make_dedup_key(
                f.book, f.tx_date, f.tx_time, f.amount,
                f.account_id, f.direction, f.source_event_id)
            d["book"] = f.book or self.book
            rows.append(d)

        written = 0
        with self.db.transaction() as conn:
            if clear_month:
                cleared = self._clear_month(conn, clear_month)
                log.info(f"[{self.book}] 重建 {clear_month}：清除 {cleared} 条流水")
            for d in rows:
                cur = conn.execute(
                    f"INSERT INTO account_flows ({','.join(d.keys())}) "
                    f"VALUES ({','.join('?' * len(d))}) "
                    f"ON CONFLICT(dedup_key) DO NOTHING",
                    list(d.values()),
                )
                written += cur.rowcount
            self._link_pairs(conn)
        log.info(f"[{self.book}] 写入 {written}/{len(rows)} 条流水")
        return written

    def _link_pairs(self, conn):
        rows = conn.execute(
            """SELECT pair_key, id FROM account_flows
               WHERE book=? AND pair_key IS NOT NULL AND linked_flow_id IS NULL
               ORDER BY pair_key, id""",
            (self.book,),
        ).fetchall()
        by_pair = {}
        for r in rows:
            by_pair.setdefault(r["pair_key"], []).append(r["id"])
        for ids in by_pair.values():
            if len(ids) == 2:
                conn.execute("UPDATE account_flows SET linked_flow_id=? WHERE id=?",
                             (ids[1], ids[0]))
                conn.execute("UPDATE account_flows SET linked_flow_id=? WHERE id=?",
                             (ids[0], ids[1]))

    # ── 余额 ──

    def calculate_monthly_balance(self, account_id: int, year: int,
                                  month: int) -> AccountBalance:
        acct = self.acct_mgr.get_account(account_id)
        if acct is None:
            return AccountBalance(account_id, year, month, 0, 0, 0, 0, True)

        start = f"{year:04d}-{month:02d}-01"
        end = f"{year + 1:04d}-01-01" if month == 12 else f"{year:04d}-{month + 1:02d}-01"

        opening = acct.opening_balance or 0.0
        if acct.is_liability:
            opening += self._liability_net(account_id, start, acct)
        else:
            params = [account_id, start]
            sql = ("SELECT COALESCE(SUM(CASE WHEN direction='inflow' THEN amount ELSE -amount END),0) AS net"
                   " FROM account_flows WHERE account_id=? AND tx_date<?")
            if acct.opening_date:
                sql += " AND tx_date>=?"
                params.append(acct.opening_date)
            opening += self.db.fetchone(sql, params)["net"]

        # 锚点可能落在月中：锚点之前的本月流水已含在期初余额里
        month_begin = max(start, acct.opening_date) if acct.opening_date else start
        month_rows = self.db.fetchall(
            """SELECT direction, amount FROM account_flows
               WHERE account_id=? AND tx_date>=? AND tx_date<?""",
            (account_id, month_begin, end),
        )
        total_in = sum(r["amount"] for r in month_rows if r["direction"] == "inflow")
        total_out = sum(r["amount"] for r in month_rows if r["direction"] == "outflow")
        if acct.is_liability:
            closing = opening + total_out - total_in
        else:
            closing = opening + total_in - total_out

        estimated = 1 if not acct.opening_date else 0
        self.db.upsert("account_balances", {
            "account_id": account_id, "year": year, "month": month,
            "opening_balance": round(opening, 2),
            "closing_balance": round(closing, 2),
            "total_inflow": round(total_in, 2),
            "total_outflow": round(total_out, 2),
            "is_estimated": estimated,
        }, conflict_cols=["account_id", "year", "month"])
        return AccountBalance(account_id, year, month, opening, closing,
                              total_in, total_out, bool(estimated))

    def _liability_net(self, account_id: int, start: str, acct: Account) -> float:
        """负债期初：历史 outflow 增加负债，inflow 减少。"""
        params = [account_id, start]
        sql = ("SELECT COALESCE(SUM(CASE WHEN direction='outflow' THEN amount ELSE -amount END),0) AS net"
               " FROM account_flows WHERE account_id=? AND tx_date<?")
        if acct.opening_date:
            sql += " AND tx_date>=?"
            params.append(acct.opening_date)
        return self.db.fetchone(sql, params)["net"]

    def calculate_all_balances(self, year: int, month: int, book: str = None) -> list:
        results = []
        for acct in self.acct_mgr.list_accounts(book=book):
            results.append(self.calculate_monthly_balance(acct.id, year, month))
        return results

    # ── 净资产 ──

    def snapshot_net_worth(self, date_str: str, book: str = "combined",
                           anchored_only: bool = False) -> dict:
        year, month = int(date_str[:4]), int(date_str[5:7])
        books = ["personal", "work"] if book == "combined" else [book]
        details = {}
        total_assets = total_liabilities = 0.0
        skipped = 0
        for b in books:
            for acct in self.acct_mgr.list_accounts(book=b, active_only=False):
                if anchored_only and (not acct.opening_date
                                      or acct.opening_date > date_str):
                    skipped += 1
                    continue
                bal = self.calculate_monthly_balance(acct.id, year, month)
                value = bal.closing_balance
                if acct.is_liability:
                    total_liabilities += value
                    liabilities = True
                else:
                    total_assets += value
                    liabilities = False
                details[acct.name] = {
                    "book": b, "type": acct.type,
                    "balance": round(value, 2),
                    "is_liability": liabilities,
                }
        net_worth = total_assets - total_liabilities
        self.db.upsert("net_worth_snapshots", {
            "book": book,
            "snapshot_date": date_str,
            "total_assets": round(total_assets, 2),
            "total_liabilities": round(total_liabilities, 2),
            "net_worth": round(net_worth, 2),
            "details": json.dumps(details, ensure_ascii=False),
        }, conflict_cols=["book", "snapshot_date"])
        return {
            "book": book, "date": date_str,
            "total_assets": round(total_assets, 2),
            "total_liabilities": round(total_liabilities, 2),
            "net_worth": round(net_worth, 2),
            "details": details,
            "included_accounts": len(details),
            "skipped_accounts": skipped,
        }

    # ── 查询 ──

    def get_flows(self, account_id: int = None, year: int = None, month: int = None,
                  source: str = None, book: str = None, limit: int = 200) -> list:
        sql = ("SELECT af.*, a.name as account_name, a.type as account_type,"
               " c.name as category_name FROM account_flows af"
               " LEFT JOIN accounts a ON af.account_id = a.id"
               " LEFT JOIN categories c ON af.category_id = c.id WHERE 1=1")
        params = []
        book = book or self.book
        if book:
            sql += " AND af.book=?"
            params.append(book)
        if account_id:
            sql += " AND af.account_id=?"
            params.append(account_id)
        if year:
            sql += " AND substr(af.tx_date,1,4)=?"
            params.append(str(year))
        if month:
            sql += " AND substr(af.tx_date,6,2)=?"
            params.append(f"{month:02d}")
        if source:
            sql += " AND af.source=?"
            params.append(source)
        sql += " ORDER BY af.tx_date DESC, af.id DESC LIMIT ?"
        params.append(limit)
        return self.db.fetchall(sql, params)

    # ── 内部 ──

    @staticmethod
    def _flow_to_dict(f: AccountFlow) -> dict:
        d = {
            "book": f.book,
            "tx_date": f.tx_date,
            "amount": round(f.amount, 2),
            "direction": f.direction,
            "tx_type": f.tx_type,
            "source": f.source,
        }
        for key in ("tx_time", "category_id", "merchant", "description",
                    "counterparty", "source_event_id", "source_group_id",
                    "pair_key", "raw_data", "notes"):
            val = getattr(f, key)
            if val:
                d[key] = val
        if f.account_id:
            d["account_id"] = f.account_id
        return d
