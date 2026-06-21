"""
全量历史数据导入脚本。

用法：
    python life/ledger/batch_import.py          # 全量导入 2025-07 ~ 2026-06
    python life/ledger/batch_import.py --clear  # 清空后全量
"""

import argparse
import os
import sys

# 确保项目根目录在 path 上
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.logme import log

JOPLINAI = getdirmain().parent / "joplinai"
if str(JOPLINAI) not in sys.path:
    sys.path.insert(0, str(JOPLINAI))

from life.ledger.db import Database
from life.ledger.accounts import AccountManager
from life.ledger.transactions import TransactionManager
from life.ledger.cloudcfg import get_sms_api_url
from life.wechat_finance import parse_finance_messages, classify_merchant, load_category_map
from life.sms_finance import parse_sms_records
from aimod.wechat_client import WeChatClient
from func.configpr import getcfpoptionvalue
from func.logme import log

import urllib.request, json, ssl


def get_sms_api_key() -> str:
    key = getcfpoptionvalue("happyjphard", "sms_collector", "api_key")
    if key:
        return str(key)
    return ""


def fetch_wechat_financial(account_name: str, year: int, month: int, client, cat_map) -> list:
    date_from = f"{year}-{month:02d}-01"
    if month == 12:
        date_to = f"{year + 1}-01-01"
    else:
        date_to = f"{year}-{month + 1:02d}-01"
    msgs = client.query(account_name, date_from=date_from, date_to=date_to, limit=50000)
    records = parse_finance_messages(msgs)
    for r in records:
        if r["category"] == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)
    return records


def fetch_sms_financial(year: int, month: int) -> list:
    sms_api = get_sms_api_url()
    key = get_sms_api_key()
    if not key:
        return []

    date_from = f"{year}-{month:02d}-01"
    if month == 12:
        date_to = f"{year + 1}-01-01"
    else:
        date_to = f"{year}-{month + 1:02d}-01"

    url = f"{sms_api}?date_from={date_from}&date_to={date_to}&limit=50000"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url)
    req.add_header("X-API-Key", key)

    try:
        resp = urllib.request.urlopen(req, timeout=30, context=ctx)
        data = json.loads(resp.read().decode())
        sms_list = data.get("records", [])
    except Exception as e:
        log.warning(f"SMS API 失败 ({year}-{month:02d}): {e}")
        return []

    records = parse_sms_records(sms_list)
    cat_map = load_category_map()
    for r in records:
        if r.get("category", "") == "未分类-其他":
            r["category"] = classify_merchant(r["merchant"], cat_map)
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="清空现有流水后重新导入")
    parser.add_argument("--from-month", default="2025-07", help="起始月份 格式 YYYY-MM")
    parser.add_argument("--to-month", default="2026-06", help="截止月份 格式 YYYY-MM")
    parser.add_argument("--account", default="白晔峰", help="微信账户名")
    args = parser.parse_args()

    db = Database()
    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    if args.clear:
        confirm = input("将清空所有 account_flows 数据，确认？(yes/no): ")
        if confirm == "yes":
            db.execute("DELETE FROM account_flows")
            print("已清空所有流水数据")
        else:
            print("取消")
            return

    # 初始化 WeChatClient
    client = WeChatClient(mode="local", db_path=str(JOPLINAI / "data" / "wcitemsall_merged.db"))
    cat_map = load_category_map()

    # 解析月份范围
    from_year, from_month = map(int, args.from_month.split("-"))
    to_year, to_month = map(int, args.to_month.split("-"))

    total_flows = 0
    months_processed = 0

    year, month = from_year, from_month
    while (year < to_year) or (year == to_year and month <= to_month):
        month_key = f"{year}-{month:02d}"
        print(f"\n{'='*50}")
        print(f"  处理 {month_key}")

        # 微信
        try:
            wx_records = fetch_wechat_financial(args.account, year, month, client, cat_map)
        except Exception as e:
            log.error(f"微信 {month_key} 失败: {e}")
            wx_records = []

        # 短信
        sms_records = fetch_sms_financial(year, month)

        print(f"  微信: {len(wx_records)} 条, 短信: {len(sms_records)} 条")

        if not wx_records and not sms_records:
            print(f"  跳过（无数据）")
        else:
            result = tx_mgr.import_merged(wx_records, sms_records, month_key)
            print(f"  导入完成: {result['total_flows']} 条流水, {result['accounts_involved']} 个账户")
            total_flows += result["total_flows"]

        months_processed += 1

        # 下个月
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    print(f"\n{'='*50}")
    print(f"全量导入完成: {months_processed} 个月, 共 {total_flows} 条流水")

    # 验证
    row = db.fetchone("SELECT COUNT(*) as cnt FROM account_flows")
    print(f"数据库总流水数: {row['cnt']}")
    row = db.fetchone("SELECT MIN(tx_date) as dmin, MAX(tx_date) as dmax FROM account_flows")
    print(f"日期范围: {row['dmin']} ~ {row['dmax']}")


if __name__ == "__main__":
    main()
