#! /usr/bin/env python
"""个人财务系统 CLI 入口。

用法：
    python -m life.ledger.cli accounts list
    python -m life.ledger.cli accounts add --name xxx --type bank_credit --bank 广发银行 --card-suffix 4321

    python -m life.ledger.cli import wechat --month 2026-06
    python -m life.ledger.cli import sms --month 2026-06
    python -m life.ledger.cli import all --month 2026-06 --account 白晔峰

    python -m life.ledger.cli report net-worth --date 2026-06-30
    python -m life.ledger.cli report cash-flow --month 2026-06
    python -m life.ledger.cli report monthly --month 2026-06
    python -m life.ledger.cli report balances --month 2026-06

    python -m life.ledger.cli tx list --month 2026-06
    python -m life.ledger.cli tx add --date 2026-06-15 --amount 5000 --account 1 --direction outflow

    python -m life.ledger.cli balance set --account 1 --year 2026 --month 6 --balance 12345.67
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.logme import log
    from func.configpr import getcfpoptionvalue

# 确保 JOPLINAI 路径可导入
JOPLINAI = getdirmain().parent / "joplinai"
if str(JOPLINAI) not in sys.path:
    sys.path.insert(0, str(JOPLINAI))

from .db import Database
from .accounts import AccountManager
from .transactions import TransactionManager
from .cloudcfg import get_sms_api_url

# log is from func.logme — configured by mylog()


def _get_db(args=None) -> Database:
    return Database()


def _get_acct_mgr(db) -> AccountManager:
    return AccountManager(db)


def _parse_month(month_str: str):
    """解析月份参数，返回 (year, month)。"""
    now = datetime.now()
    if not month_str or month_str == "prev":
        first = now.replace(day=1) - timedelta(days=1)
        return first.year, first.month
    parts = month_str.split("-")
    return int(parts[0]), int(parts[1])


def _get_sms_api_key() -> str:
    """从本地 ini 获取 SMS API 密钥，回落环境变量。"""
    key = getcfpoptionvalue("happyjphard", "sms_collector", "api_key")
    if key:
        return str(key)
    return os.environ.get("SMS_API_KEY", "")


# ── accounts ──

def cmd_accounts_list(args):
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    accounts = acct_mgr.list_accounts(type_filter=args.type)
    if not accounts:
        print("暂无账户。")
        return

    type_labels = {
        "bank_debit": "储蓄卡", "bank_credit": "信用卡",
        "wechat_wallet": "微信零钱", "alipay": "支付宝",
        "loan": "贷款", "cash": "现金",
    }

    print(f"\n{'ID':<4} {'名称':<20} {'类型':<10} {'银行':<10} {'尾号':<6} {'机构':<12}")
    print("-" * 70)
    for a in accounts:
        label = type_labels.get(a.type, a.type)
        print(f"{a.id:<4} {a.name:<20} {label:<10} {(a.bank or ''):<10} {(a.card_suffix or ''):<6} {(a.institution or ''):<12}")


def cmd_accounts_add(args):
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    acct = acct_mgr.add_account(
        name=args.name, acct_type=args.type,
        bank=args.bank, card_suffix=args.card_suffix,
        institution=args.institution, notes=args.notes,
    )
    print(f"已添加账户: [{acct.id}] {acct.name}")


# ── import ──

def _import_wechat(args, month_key: str):
    from life.wechat_finance import process_month as wx_process
    year, month = _parse_month(args.month)
    report = wx_process(args.account, year, month)
    return []  # process_month 已经打印了，我们直接返回空列表


def _import_wechat_events(args, year: int, month: int):
    """获取微信解析事件列表。"""
    from life.wechat_finance import process_month
    report = process_month(args.account, year, month)
    return []


def cmd_import_wechat(args):
    year, month = _parse_month(args.month)
    month_key = f"{year}-{month:02d}"
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    # 复用 wechat_finance 的 parse_finance_messages
    from life.wechat_finance import parse_finance_messages, classify_merchant, load_category_map
    from aimod.wechat_client import WeChatClient

    client = WeChatClient(mode="local", db_path=str(JOPLINAI / "data" / "wcitemsall_merged.db"))
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"

    msgs = client.query(args.account, date_from=date_from, date_to=date_to, limit=50000)
    log.info(f"拉取 {len(msgs)} 条消息")
    records = parse_finance_messages(msgs)
    cat_map = load_category_map()
    for r in records:
        if r["category"] == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)

    log.info(f"识别 {len(records)} 条财务记录")
    result = tx_mgr.import_wechat(records, month_key)
    print(f"微信导入完成: {result} 条流水")


def cmd_import_sms(args):
    year, month = _parse_month(args.month)
    month_key = f"{year}-{month:02d}"
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    from life.sms_finance import parse_sms_records

    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"

    # 通过 HTTP API 获取短信
    import urllib.request, json, ssl
    sms_api = get_sms_api_url()
    url = f"{sms_api}?date_from={date_from}&date_to={date_to}&limit=50000"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    sms_api_key = _get_sms_api_key()
    if not sms_api_key:
        log.error("SMS API 密钥未配置：请在 happyjphard.ini [sms_collector] 设置 api_key，或设环境变量 SMS_API_KEY")
        return

    req = urllib.request.Request(url)
    req.add_header("X-API-Key", sms_api_key)

    try:
        resp = urllib.request.urlopen(req, timeout=30, context=ctx)
        data = json.loads(resp.read().decode())
        sms_list = data.get("records", [])
    except Exception as e:
        log.error(f"SMS API 调用失败: {e}")
        return

    records = parse_sms_records(sms_list)
    log.info(f"SMS: {len(sms_list)}条 → {len(records)}条财务记录")
    result = tx_mgr.import_sms(records, month_key)
    print(f"短信导入完成: {result} 条流水")


def cmd_import_all(args):
    year, month = _parse_month(args.month)
    month_key = f"{year}-{month:02d}"
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    # 微信
    from life.wechat_finance import parse_finance_messages, classify_merchant, load_category_map
    from aimod.wechat_client import WeChatClient

    client = WeChatClient(mode="local", db_path=str(JOPLINAI / "data" / "wcitemsall_merged.db"))
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"

    msgs = client.query(args.account, date_from=date_from, date_to=date_to, limit=50000)
    log.info(f"微信: 拉取 {len(msgs)} 条消息")
    wx_records = parse_finance_messages(msgs)
    cat_map = load_category_map()
    for r in wx_records:
        if r["category"] == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)
    log.info(f"微信: {len(wx_records)} 条财务记录")

    # 短信
    from life.sms_finance import parse_sms_records
    import urllib.request, json, ssl
    sms_api = get_sms_api_url()
    url = f"{sms_api}?date_from={date_from}&date_to={date_to}&limit=50000"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    sms_api_key = _get_sms_api_key()
    if sms_api_key:
        req = urllib.request.Request(url)
        req.add_header("X-API-Key", sms_api_key)
    else:
        req = urllib.request.Request(url)

    try:
        resp = urllib.request.urlopen(req, timeout=30, context=ctx)
        data = json.loads(resp.read().decode())
        sms_list = data.get("records", [])
    except Exception as e:
        log.warning(f"SMS API 失败: {e}")
        sms_list = []

    sms_records = parse_sms_records(sms_list) if sms_list else []
    log.info(f"SMS: {len(sms_list)}条 → {len(sms_records)}条记录")

    # 归并导入
    result = tx_mgr.import_merged(wx_records, sms_records, month_key)
    print(f"合并导入完成: 共 {result['total_flows']} 条流水")
    print(f"  来源分布: {result['by_source']}")
    print(f"  涉及账户: {result['accounts_involved']} 个")


# ── report ──

def cmd_report_net_worth(args):
    db = _get_db()
    from .reports.net_worth import generate_net_worth_report
    report = generate_net_worth_report(db, args.date)
    print(report)


def cmd_report_cash_flow(args):
    db = _get_db()
    year, month = _parse_month(args.month)
    from .reports.cash_flow import generate_cash_flow_report
    report = generate_cash_flow_report(db, year, month, by_account=args.by_account, by_category=args.by_category)
    print(report)


def cmd_report_monthly(args):
    db = _get_db()
    year, month = _parse_month(args.month)
    from .reports.monthly import generate_monthly_report
    report = generate_monthly_report(db, year, month)
    print(report)


def cmd_report_balances(args):
    db = _get_db()
    year, month = _parse_month(args.month)
    from .reports.balances import generate_balance_report
    report = generate_balance_report(db, year, month, account_id=args.account)
    print(report)


# ── tx ──

def cmd_tx_list(args):
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    tx_mgr = TransactionManager(db, acct_mgr)
    year, month = _parse_month(args.month)
    flows = tx_mgr.get_flows(account_id=args.account, year=year, month=month, source=args.source, limit=args.limit)

    print(f"\n{'ID':<6} {'日期':<12} {'账户':<16} {'金额':<12} {'方向':<6} {'分类':<14} {'商户':<20}")
    print("-" * 90)
    for f in flows:
        direction = "↓支出" if f["direction"] == "outflow" else "↑收入"
        amt = f"¥{f['amount']:,.2f}"
        acct_name = (f.get("account_name") or "?")[:14]
        cat = (f.get("category_name") or "")[:12]
        mch = (f.get("merchant") or "")[:18]
        print(f"{f['id']:<6} {f['tx_date']:<12} {acct_name:<16} {amt:<12} {direction:<6} {cat:<14} {mch:<20}")
    print(f"\n共 {len(flows)} 条")


def cmd_tx_add(args):
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    # 查找分类
    cat_id = None
    if args.category:
        row = db.fetchone("SELECT id FROM categories WHERE name=?", (args.category,))
        if row:
            cat_id = row["id"]

    from .models import AccountFlow
    flow = AccountFlow(
        tx_date=args.date,
        amount=args.amount,
        account_id=args.account,
        direction=args.direction,
        tx_type=args.tx_type,
        category_id=cat_id,
        merchant=args.merchant,
        description=args.description,
        source="manual",
    )

    count = tx_mgr.import_wechat([], "")
    from .transactions import TransactionManager as TM
    flows_list = [flow]
    # 直接写入
    tx_mgr._save_flows(flows_list, "")
    print(f"已添加手动流水")


# ── balance ──

def cmd_balance_set(args):
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    tx_mgr = TransactionManager(db, acct_mgr)
    tx_mgr.set_initial_balance(args.account, args.year, args.month, args.balance, args.notes or "")
    print(f"已设定账户 [{args.account}] {args.year}-{args.month:02d} 余额: ¥{args.balance:,.2f}")


def cmd_balance_calc(args):
    db = _get_db()
    acct_mgr = _get_acct_mgr(db)
    tx_mgr = TransactionManager(db, acct_mgr)
    year, month = _parse_month(args.month)
    results = tx_mgr.calculate_all_balances(year, month)
    print(f"已计算 {len(results)} 个账户的 {year}-{month:02d} 余额")


# ── 主入口 ──

def main():
    parser = argparse.ArgumentParser(description="个人财务系统")
    sub = parser.add_subparsers(dest="cmd")
    sub.required = True

    # accounts
    p_accts = sub.add_parser("accounts")
    p_accts_sub = p_accts.add_subparsers(dest="action")
    p_accts_sub.required = True

    p_accts_list = p_accts_sub.add_parser("list")
    p_accts_list.add_argument("--type", help="账户类型过滤")

    p_accts_add = p_accts_sub.add_parser("add")
    p_accts_add.add_argument("--name", required=True)
    p_accts_add.add_argument("--type", required=True, dest="type")
    p_accts_add.add_argument("--bank")
    p_accts_add.add_argument("--card-suffix", dest="card_suffix")
    p_accts_add.add_argument("--institution")
    p_accts_add.add_argument("--notes")

    # import
    p_import = sub.add_parser("import")
    p_import_sub = p_import.add_subparsers(dest="source")
    p_import_sub.required = True

    for src in ("wechat", "sms"):
        p = p_import_sub.add_parser(src)
        p.add_argument("--month", required=True)
        p.add_argument("--account", default="白晔峰")

    p_all = p_import_sub.add_parser("all")
    p_all.add_argument("--month", required=True)
    p_all.add_argument("--account", default="白晔峰")

    # report
    p_report = sub.add_parser("report")
    p_report_sub = p_report.add_subparsers(dest="report_type")
    p_report_sub.required = True

    p_nw = p_report_sub.add_parser("net-worth")
    p_nw.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))

    p_cf = p_report_sub.add_parser("cash-flow")
    p_cf.add_argument("--month")
    p_cf.add_argument("--by-account", action="store_true")
    p_cf.add_argument("--by-category", action="store_true")

    p_mr = p_report_sub.add_parser("monthly")
    p_mr.add_argument("--month")

    p_br = p_report_sub.add_parser("balances")
    p_br.add_argument("--month")
    p_br.add_argument("--account", type=int, help="指定账户 ID")

    # tx
    p_tx = sub.add_parser("tx")
    p_tx_sub = p_tx.add_subparsers(dest="action")
    p_tx_sub.required = True

    p_tx_list = p_tx_sub.add_parser("list")
    p_tx_list.add_argument("--month")
    p_tx_list.add_argument("--account", type=int)
    p_tx_list.add_argument("--source")
    p_tx_list.add_argument("--limit", type=int, default=100)

    p_tx_add = p_tx_sub.add_parser("add")
    p_tx_add.add_argument("--date", required=True)
    p_tx_add.add_argument("--amount", type=float, required=True)
    p_tx_add.add_argument("--account", type=int, required=True)
    p_tx_add.add_argument("--direction", choices=["inflow", "outflow"], default="outflow")
    p_tx_add.add_argument("--tx-type", default="expense")
    p_tx_add.add_argument("--category")
    p_tx_add.add_argument("--merchant")
    p_tx_add.add_argument("--description")

    # balance
    p_bal = sub.add_parser("balance")
    p_bal_sub = p_bal.add_subparsers(dest="action")
    p_bal_sub.required = True

    p_bal_set = p_bal_sub.add_parser("set")
    p_bal_set.add_argument("--account", type=int, required=True)
    p_bal_set.add_argument("--year", type=int, required=True)
    p_bal_set.add_argument("--month", type=int, required=True)
    p_bal_set.add_argument("--balance", type=float, required=True)
    p_bal_set.add_argument("--notes")

    p_bal_calc = p_bal_sub.add_parser("calc")
    p_bal_calc.add_argument("--month")

    args = parser.parse_args()

    # 分发
    cmd_map = {
        ("accounts", "list"): cmd_accounts_list,
        ("accounts", "add"): cmd_accounts_add,
        ("import", "wechat"): cmd_import_wechat,
        ("import", "sms"): cmd_import_sms,
        ("import", "all"): cmd_import_all,
        ("report", "net-worth"): cmd_report_net_worth,
        ("report", "cash-flow"): cmd_report_cash_flow,
        ("report", "monthly"): cmd_report_monthly,
        ("report", "balances"): cmd_report_balances,
        ("tx", "list"): cmd_tx_list,
        ("tx", "add"): cmd_tx_add,
        ("balance", "set"): cmd_balance_set,
        ("balance", "calc"): cmd_balance_calc,
    }

    key = (args.cmd, getattr(args, "action", None) or getattr(args, "source", None) or getattr(args, "report_type", None))
    handler = cmd_map.get(key)
    if handler:
        handler(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
