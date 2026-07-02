"""全量重新导入：清空后走 import_merged（微信+短信归并）。
覆盖 2023-01 ~ 2026-06，含余额重算。

用法：
    python tools/reimport_full.py                    # 全量重导
    python tools/reimport_full.py --skip-clear       # 不清空，增量追加 WeChat
"""

import argparse
import json
import sys
import urllib.request
import ssl
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pathmagic
with pathmagic.context():
    from func.first import getdirmain
    from func.logme import log
    from func.configpr import getcfpoptionvalue

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


def get_sms_api_key() -> str:
    key = getcfpoptionvalue("happyjphard", "sms_collector", "api_key")
    if key:
        return str(key)
    return ""


def fetch_sms_month(year: int, month: int) -> list:
    sms_api = get_sms_api_url()
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"
    url = f"{sms_api}?date_from={date_from}&date_to={date_to}&limit=50000"

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    sms_api_key = get_sms_api_key()
    req = urllib.request.Request(url)
    if sms_api_key:
        req.add_header("X-API-Key", sms_api_key)

    try:
        resp = urllib.request.urlopen(req, timeout=30, context=ctx)
        data = json.loads(resp.read().decode())
        return data.get("records", [])
    except Exception as e:
        log.warning(f"  SMS API 失败 ({year}-{month:02d}): {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="全量重导财务数据（微信+短信归并）")
    parser.add_argument("--skip-clear", action="store_true", help="不清空现有数据，仅增量导入 WeChat")
    parser.add_argument("--start-month", default="2023-01")
    parser.add_argument("--end-month", default=datetime.now().strftime("%Y-%m"))
    args = parser.parse_args()

    sy, sm = map(int, args.start_month.split("-"))
    ey, em = map(int, args.end_month.split("-"))
    total_months = (ey - sy) * 12 + (em - sm) + 1
    log.info(f"月份范围: {args.start_month} ~ {args.end_month}（共 {total_months} 个月）")

    db = Database()
    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)
    client = WeChatClient(mode="local", db_path=str(JOPLINAI / "data" / "wcitemsall_merged.db"))
    cat_map = load_category_map()

    if not args.skip_clear:
        log.info("清空 account_flows / account_balances / net_worth_snapshots...")
        db.execute("DELETE FROM account_flows")
        db.execute("DELETE FROM account_balances")
        db.execute("DELETE FROM net_worth_snapshots")
        db.commit()
        log.info("已清空")

    total_flows = 0
    monthly = []

    for y in range(sy, ey + 1):
        m_start = sm if y == sy else 1
        m_end = em if y == ey else 12
        for m in range(m_start, m_end + 1):
            month_key = f"{y}-{m:02d}"
            log.info(f"\n=== {month_key} ===")

            # WeChat
            date_from = f"{y}-{m:02d}-01"
            date_to = f"{y}-{m+1:02d}-01" if m < 12 else f"{y+1}-01-01"
            msgs = client.query("白晔峰", date_from=date_from, date_to=date_to, limit=100000)
            wx_records = parse_finance_messages(msgs)
            for r in wx_records:
                if r["category"] == "未分类-其他":
                    r["category"] = classify_merchant(r["merchant"], cat_map)
            log.info(f"  微信: {len(msgs)} 条消息 → {len(wx_records)} 条财务记录")

            # SMS
            sms_list = fetch_sms_month(y, m)
            sms_records = parse_sms_records(sms_list) if sms_list else []
            for r in sms_records:
                if r.get("category", "") == "未分类-其他":
                    r["category"] = classify_merchant(r["merchant"], cat_map)
            log.info(f"  SMS:  {len(sms_list)} 条 → {len(sms_records)} 条记录")

            if not wx_records and not sms_records:
                log.info(f"  {month_key}: 无数据")
                monthly.append((month_key, 0, 0, 0))
                continue

            # 合并导入
            result = tx_mgr.import_merged(wx_records, sms_records, month_key)
            total_flows += result["total_flows"]
            monthly.append((month_key, result["total_flows"], len(wx_records), len(sms_records)))
            log.info(f"  → {result['total_flows']} 条流水（微信: {result['by_source'].get('wechat',0)}, SMS: {result['by_source'].get('sms',0)}）")

    # 汇总
    print(f"\n{'='*60}")
    print(f"导入完成! 共 {total_flows} 条流水, {total_months} 个月")
    print(f"\n各月明细:")
    print(f"{'月份':<10} {'流水':<8} {'微信':<8} {'短信':<8}")
    print("-" * 34)
    for mk, cnt, wx, sm in monthly:
        print(f"{mk:<10} {cnt:<8} {wx:<8} {sm:<8}")
    print(f"\n{'总计':<10} {total_flows:<8}")

    # 余额重算
    log.info("重算所有月份余额...")
    bal_count = 0
    for y in range(sy, ey + 1):
        m_start = sm if y == sy else 1
        m_end = em if y == ey else 12
        for m in range(m_start, m_end + 1):
            tx_mgr.calculate_all_balances(y, m)
            bal_count += 1
    log.info(f"余额重算完成: {bal_count} 个月 × 账户数")

    print(f"\n数据已写入 ledger.db")
    print(f"运行报告: python -m life.ledger.cli report monthly --month {args.end_month}")


if __name__ == "__main__":
    main()
