"""重新导入所有月份财务数据（清空后全量导入）。

用法：
    python tools/reimport_ledger.py              # 全量重新导入
    python tools/reimport_ledger.py --dry-run     # 试跑，只显示统计不写入
"""

import argparse
import json
import sys
import time
import urllib.request
import ssl
from datetime import datetime, timedelta
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
from life.sms_finance import parse_sms_records


def get_sms_api_key() -> str:
    key = getcfpoptionvalue("happyjphard", "sms_collector", "api_key")
    if key:
        return str(key)
    return ""


def fetch_sms_month(year: int, month: int) -> list:
    """拉取指定月份的 SMS 数据。"""
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
        records = data.get("records", [])
        log.info(f"  SMS API: {len(records)} 条")
        return records
    except Exception as e:
        log.warning(f"  SMS API 失败: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="重新导入财务数据")
    parser.add_argument("--dry-run", action="store_true", help="试跑，不写入")
    parser.add_argument("--start-month", help="起始月份 (YYYY-MM)，默认 2023-11")
    parser.add_argument("--end-month", help="结束月份 (YYYY-MM)，默认当前月")
    args = parser.parse_args()

    start = args.start_month or "2023-11"
    end = args.end_month or datetime.now().strftime("%Y-%m")
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))

    total_months = (ey - sy) * 12 + (em - sm) + 1
    log.info(f"共 {total_months} 个月: {start} ~ {end}")

    db = Database()
    acct_mgr = AccountManager(db)
    tx_mgr = TransactionManager(db, acct_mgr)

    if not args.dry_run:
        # 清空现有数据
        log.info("清空 account_flows 和 account_balances...")
        db.execute("DELETE FROM account_flows")
        db.execute("DELETE FROM account_balances")
        db.execute("DELETE FROM net_worth_snapshots")
        db.commit()
        log.info("已清空")

    total_flows = 0
    month_flow_counts = []

    for y in range(sy, ey + 1):
        m_start = sm if y == sy else 1
        m_end = em if y == ey else 12
        for m in range(m_start, m_end + 1):
            month_key = f"{y}-{m:02d}"
            log.info(f"\n--- {month_key} ---")

            sms_list = fetch_sms_month(y, m)
            if not sms_list:
                log.info(f"  {month_key}: 无数据")
                month_flow_counts.append((month_key, 0))
                continue

            records = parse_sms_records(sms_list)
            log.info(f"  解析后: {len(records)} 条财务记录")

            if args.dry_run:
                month_flow_counts.append((month_key, len(records)))
                total_flows += len(records)
                continue

            # 导入
            count = tx_mgr.import_sms(records, month_key)
            total_flows += count
            month_flow_counts.append((month_key, count))
            log.info(f"  → {count} 条流水入库")

    # 统计
    print(f"\n{'='*50}")
    print(f"重新导入完成! 共 {total_flows} 条流水, {total_months} 个月")
    print(f"\n各月明细:")
    for mk, cnt in month_flow_counts:
        print(f"  {mk}: {cnt} 条")

    if args.dry_run:
        print(f"\n[试跑] 共 {total_flows} 条可导入")
    else:
        print(f"\n数据已写入 ledger.db")


if __name__ == "__main__":
    main()
