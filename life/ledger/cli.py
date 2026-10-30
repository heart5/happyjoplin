#! /usr/bin/env python
"""个人财务系统 CLI 入口。

用法：
    python -m life.ledger.cli sync --month 2026-09
    python -m life.ledger.cli sync --backfill --books personal
    python -m life.ledger.cli accounts list --book personal
    python -m life.ledger.cli tx list --month 2026-09 --book personal
    python -m life.ledger.cli tx add --date 2026-09-15 --amount 35 --account 1 --merchant 瑞幸
    python -m life.ledger.cli report monthly --month 2026-09 --book personal
    python -m life.ledger.cli status
"""

import argparse
import sys
from datetime import datetime, timedelta

from .db import Database
from .accounts import AccountManager
from .transactions import TransactionManager
from .models import AccountFlow

TYPES = ("bank", "payment", "credit_card", "loan", "investment", "cash")
TX_TYPES = ("consumption", "income", "transfer", "loan_disbursement",
            "loan_repayment", "credit_card_repayment", "refund", "fee",
            "adjustment")
BOOKS = ("personal", "work")


def _parse_month(month_str: str):
    now = datetime.now()
    if not month_str or month_str == "prev":
        first = now.replace(day=1) - timedelta(days=1)
        return first.year, first.month
    year, month = month_str.split("-")
    return int(year), int(month)


def _db():
    return Database()


# ── accounts ──

def cmd_accounts_list(args):
    acct_mgr = AccountManager(_db())
    accounts = acct_mgr.list_accounts(
        book=args.book, type_filter=args.type,
        active_only=not args.inactive)
    if not accounts:
        print("暂无账户。")
        return
    print(f"\n{'ID':<4} {'账套':<9} {'名称':<24} {'类型':<10} "
          f"{'银行':<10} {'尾号':<6} {'机构':<10} {'锚点':<22}")
    print("-" * 100)
    for a in accounts:
        label = AccountManager.ACCOUNT_TYPE_LABELS.get(a.type, a.type)
        anchor = (f"{a.opening_date} {a.opening_balance:,.2f}"
                  if a.opening_date else "估算")
        print(f"{a.id:<4} {a.book:<9} {a.name:<24} {label:<10} "
              f"{(a.bank or ''):<10} {(a.card_suffix or ''):<6} "
              f"{(a.institution or ''):<10} {anchor:<22}")


def cmd_accounts_add(args):
    acct_mgr = AccountManager(_db())
    acct = acct_mgr.add_account(
        name=args.name, acct_type=args.type, book=args.book,
        bank=args.bank, card_suffix=args.card_suffix,
        institution=args.institution, platform=args.platform,
        opening_balance=args.opening_balance,
        opening_date=args.opening_date, notes=args.notes,
    )
    print(f"账户已就绪: [{acct.id}] {acct.book}/{acct.name}")


def cmd_accounts_anchor(args):
    acct_mgr = AccountManager(_db())
    acct_mgr.set_anchor(args.account, args.balance, args.date)
    acct = acct_mgr.get_account(args.account)
    print(f"锚点已设置: [{acct.id}] {acct.name} "
          f"{acct.opening_date} 起余额 {acct.opening_balance:,.2f}")


# ── sync ──

def cmd_sync(args):
    from .sync import main as sync_main
    argv = ["--books", args.books]
    if args.month:
        argv += ["--month", args.month]
    if args.from_month:
        argv += ["--from-month", args.from_month]
    if args.to_month:
        argv += ["--to-month", args.to_month]
    if args.backfill:
        argv.append("--backfill")
    if args.force:
        argv.append("--force")
    if args.dry_run:
        argv.append("--dry-run")
    if not args.reclassify:
        argv.append("--no-reclassify")
    return sync_main(argv)


def cmd_import_all(args):
    from .sync import main as sync_main
    return sync_main(["--month", args.month, "--books", args.books])


def cmd_doctor(args):
    from .doctor import main as doctor_main
    argv = ["--books", args.books, "--months", str(args.months)]
    if args.month:
        argv += ["--month", args.month]
    if args.from_month:
        argv += ["--from-month", args.from_month]
    if args.to_month:
        argv += ["--to-month", args.to_month]
    if args.publish:
        argv.append("--publish")
    if args.force:
        argv.append("--force")
    if args.dry_run:
        argv.append("--dry-run")
    if args.json:
        argv.append("--json")
    return doctor_main(argv)


def cmd_status(args):
    db = _db()
    rows = db.fetchall(
        """SELECT book, source, period, status, row_count, md5, error, finished_at
           FROM import_batches ORDER BY period DESC, book, source LIMIT ?""",
        (args.limit,),
    )
    if not rows:
        print("暂无同步记录。")
        return
    print(f"\n{'月份':<9} {'账套':<9} {'来源':<13} {'状态':<8} "
          f"{'原始条数':<8} {'完成时间':<20}")
    print("-" * 76)
    for r in rows:
        print(f"{r['period']:<9} {r['book']:<9} {r['source']:<13} "
              f"{r['status']:<8} {r['row_count']:<8} "
              f"{(r['finished_at'] or ''):<20}")
        if r["error"]:
            print(f"    错误: {r['error']}")


# ── tx ──

def cmd_tx_list(args):
    db = _db()
    tx_mgr = TransactionManager(db, book=args.book)
    year, month = _parse_month(args.month)
    flows = tx_mgr.get_flows(account_id=args.account, year=year, month=month,
                             source=args.source, book=args.book,
                             limit=args.limit)
    print(f"\n{'ID':<6} {'日期':<12} {'账户':<18} {'金额':<11} "
          f"{'类型':<10} {'分类':<13} {'商户':<18}")
    print("-" * 94)
    for f in flows:
        direction = "↑" if f["direction"] == "inflow" else "↓"
        amt = f"{direction}¥{f['amount']:,.2f}"
        acct = (f.get("account_name") or "?")[:16]
        cat = (f.get("category_name") or "")[:11]
        mch = (f.get("merchant") or "")[:16]
        print(f"{f['id']:<6} {f['tx_date']:<12} {acct:<18} {amt:<11} "
              f"{f['tx_type']:<10} {cat:<13} {mch:<18}")
    print(f"\n共 {len(flows)} 条")


def cmd_tx_add(args):
    db = _db()
    cat_id = None
    if args.category:
        row = db.fetchone("SELECT id FROM categories WHERE name=?",
                          (args.category,))
        if row:
            cat_id = row["id"]
    flow = AccountFlow(
        book=args.book, tx_date=args.date, amount=args.amount,
        account_id=args.account, direction=args.direction,
        tx_type=args.tx_type, category_id=cat_id, merchant=args.merchant,
        counterparty=args.counterparty, description=args.description,
    )
    written = TransactionManager(db, book=args.book).add_manual(
        **flow.__dict__)
    print(f"已添加手动流水 {written} 条")


# ── balance ──

def cmd_balance_calc(args):
    db = _db()
    year, month = _parse_month(args.month)
    book = None if args.book == "all" else args.book
    results = TransactionManager(db, book=book or "personal") \
        .calculate_all_balances(year, month, book=book)
    print(f"已计算 {len(results)} 个账户的 {year}-{month:02d} 余额")


# ── report ──

def cmd_report_monthly(args):
    from .reports.monthly import generate_monthly_report
    year, month = _parse_month(args.month)
    print(generate_monthly_report(_db(), year, month, book=args.book))


def cmd_report_annual(args):
    from .reports.annual_report import generate_annual_report
    print(generate_annual_report(_db(), args.year, book=args.book))


def cmd_report_net_worth(args):
    from .reports.net_worth import generate_net_worth_report
    print(generate_net_worth_report(_db(), args.date, book=args.book))


def cmd_report_balances(args):
    from .reports.balances import generate_balance_report
    year, month = _parse_month(args.month)
    print(generate_balance_report(_db(), year, month, book=args.book,
                                  account_id=args.account))


def cmd_report_credit_cards(args):
    from .reports.credit_cards import generate_credit_cards_report
    print(generate_credit_cards_report(_db(), as_of=args.date))


def cmd_report_accounts(args):
    from .reports.accounts_report import generate_accounts_report
    year, month = _parse_month(args.month)
    print(generate_accounts_report(_db(), year, month, book=args.book))


def cmd_publish(args):
    from .publish import main as publish_main
    argv = [args.target]
    if args.month:
        argv += ["--month", args.month]
    if args.year:
        argv += ["--year", str(args.year)]
    argv += ["--books", args.books]
    if args.force:
        argv.append("--force")
    if args.dry_run:
        argv.append("--dry-run")
    return publish_main(argv)


# ── 主入口 ──

def main(argv=None):
    parser = argparse.ArgumentParser(description="个人财务系统")
    sub = parser.add_subparsers(dest="cmd")
    sub.required = True

    p_accts = sub.add_parser("accounts")
    p_accts_sub = p_accts.add_subparsers(dest="action")
    p_accts_sub.required = True

    p_list = p_accts_sub.add_parser("list")
    p_list.add_argument("--book", choices=BOOKS)
    p_list.add_argument("--type", choices=TYPES)
    p_list.add_argument("--inactive", action="store_true")

    p_add = p_accts_sub.add_parser("add")
    p_add.add_argument("--name", required=True)
    p_add.add_argument("--type", required=True, choices=TYPES)
    p_add.add_argument("--book", choices=BOOKS, default="personal")
    p_add.add_argument("--bank")
    p_add.add_argument("--card-suffix", dest="card_suffix")
    p_add.add_argument("--institution")
    p_add.add_argument("--platform")
    p_add.add_argument("--opening-balance", dest="opening_balance",
                       type=float, default=0.0)
    p_add.add_argument("--opening-date", dest="opening_date")
    p_add.add_argument("--notes")

    p_anchor = p_accts_sub.add_parser("anchor")
    p_anchor.add_argument("--account", type=int, required=True)
    p_anchor.add_argument("--balance", type=float, required=True)
    p_anchor.add_argument("--date", required=True,
                          help="从该日期起按此余额滚算（通常为余额短信次日）")

    p_sync = sub.add_parser("sync")
    p_sync.add_argument("--month")
    p_sync.add_argument("--from-month", dest="from_month")
    p_sync.add_argument("--to-month", dest="to_month")
    p_sync.add_argument("--books", default="personal,work")
    p_sync.add_argument("--backfill", action="store_true")
    p_sync.add_argument("--force", action="store_true")
    p_sync.add_argument("--dry-run", dest="dry_run", action="store_true")
    p_sync.add_argument("--no-reclassify", dest="reclassify",
                        action="store_false",
                        help="同步后不按分类表重映射未分类流水")

    p_import = sub.add_parser("import")
    p_import_sub = p_import.add_subparsers(dest="source")
    p_import_sub.required = True
    p_all = p_import_sub.add_parser("all")
    p_all.add_argument("--month", required=True)
    p_all.add_argument("--books", default="personal,work")

    p_status = sub.add_parser("status")
    p_status.add_argument("--limit", type=int, default=30)

    p_tx = sub.add_parser("tx")
    p_tx_sub = p_tx.add_subparsers(dest="action")
    p_tx_sub.required = True

    p_tx_list = p_tx_sub.add_parser("list")
    p_tx_list.add_argument("--month")
    p_tx_list.add_argument("--book", choices=BOOKS, default="personal")
    p_tx_list.add_argument("--account", type=int)
    p_tx_list.add_argument("--source")
    p_tx_list.add_argument("--limit", type=int, default=100)

    p_tx_add = p_tx_sub.add_parser("add")
    p_tx_add.add_argument("--date", required=True)
    p_tx_add.add_argument("--amount", type=float, required=True)
    p_tx_add.add_argument("--account", type=int, required=True)
    p_tx_add.add_argument("--book", choices=BOOKS, default="personal")
    p_tx_add.add_argument("--direction", choices=["inflow", "outflow"],
                          default="outflow")
    p_tx_add.add_argument("--tx-type", dest="tx_type", choices=TX_TYPES,
                          default="consumption")
    p_tx_add.add_argument("--category")
    p_tx_add.add_argument("--merchant")
    p_tx_add.add_argument("--counterparty")
    p_tx_add.add_argument("--description")

    p_bal = sub.add_parser("balance")
    p_bal_sub = p_bal.add_subparsers(dest="action")
    p_bal_sub.required = True
    p_bal_calc = p_bal_sub.add_parser("calc")
    p_bal_calc.add_argument("--month")
    p_bal_calc.add_argument("--book", choices=BOOKS + ("all",), default="all")

    p_report = sub.add_parser("report")
    p_report_sub = p_report.add_subparsers(dest="report_type")
    p_report_sub.required = True

    p_monthly = p_report_sub.add_parser("monthly")
    p_monthly.add_argument("--month")
    p_monthly.add_argument("--book", choices=BOOKS, default="personal")

    p_annual = p_report_sub.add_parser("annual")
    p_annual.add_argument("--year", type=int, default=datetime.now().year)
    p_annual.add_argument("--book", choices=BOOKS, default="personal")

    p_nw = p_report_sub.add_parser("net-worth")
    p_nw.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    p_nw.add_argument("--book", default="combined")

    p_br = p_report_sub.add_parser("balances")
    p_br.add_argument("--month")
    p_br.add_argument("--book", choices=BOOKS + ("all",), default="personal")
    p_br.add_argument("--account", type=int)

    p_cc = p_report_sub.add_parser("credit-cards")
    p_cc.add_argument("--date")

    p_ac = p_report_sub.add_parser("accounts")
    p_ac.add_argument("--month")
    p_ac.add_argument("--book", choices=BOOKS + ("all",), default="all")

    p_pub = sub.add_parser("publish")
    p_pub.add_argument("target", choices=["monthly", "annual", "net-worth",
                                          "credit-cards", "status", "all"])
    p_pub.add_argument("--month")
    p_pub.add_argument("--year", type=int)
    p_pub.add_argument("--books", default="personal,work")
    p_pub.add_argument("--force", action="store_true")
    p_pub.add_argument("--dry-run", dest="dry_run", action="store_true")

    p_doc = sub.add_parser("doctor", help="数据体检（只读，可发布）")
    p_doc.add_argument("--month")
    p_doc.add_argument("--months", type=int, default=3)
    p_doc.add_argument("--from-month", dest="from_month")
    p_doc.add_argument("--to-month", dest="to_month")
    p_doc.add_argument("--books", default="personal,work")
    p_doc.add_argument("--publish", action="store_true")
    p_doc.add_argument("--force", action="store_true")
    p_doc.add_argument("--dry-run", dest="dry_run", action="store_true")
    p_doc.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)

    cmd_map = {
        ("accounts", "list"): cmd_accounts_list,
        ("accounts", "add"): cmd_accounts_add,
        ("accounts", "anchor"): cmd_accounts_anchor,
        ("sync", None): cmd_sync,
        ("import", "all"): cmd_import_all,
        ("status", None): cmd_status,
        ("tx", "list"): cmd_tx_list,
        ("tx", "add"): cmd_tx_add,
        ("balance", "calc"): cmd_balance_calc,
        ("report", "monthly"): cmd_report_monthly,
        ("report", "annual"): cmd_report_annual,
        ("report", "net-worth"): cmd_report_net_worth,
        ("report", "balances"): cmd_report_balances,
        ("report", "credit-cards"): cmd_report_credit_cards,
        ("report", "accounts"): cmd_report_accounts,
        ("publish", None): cmd_publish,
        ("doctor", None): cmd_doctor,
    }
    key = (args.cmd, getattr(args, "action", None)
           or getattr(args, "source", None)
           or getattr(args, "report_type", None))
    handler = cmd_map.get(key)
    if not handler:
        parser.print_help()
        return 1
    return handler(args) or 0


if __name__ == "__main__":
    sys.exit(main())
