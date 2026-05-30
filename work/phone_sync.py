#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pixel 6 Pro → hcx 聊天记录推送同步。

轻量级，只需 sqlite3 + json + requests（均为标准库或常用库）。
用本地 JSON 文件存同步游标，HTTP POST 至 hcx 的 /chat/sync 端点。

使用：
    python work/phone_sync.py                          # 增量推送（自动跳过重复）
    python work/phone_sync.py --full                   # 全量重推（重置游标）
    python work/phone_sync.py --stats                  # 查看数据库概况
    python work/phone_sync.py --account 白晔峰 --limit 10000
"""

import json
import os
import sqlite3
import sys
import time

try:
    import requests
except ImportError:
    print("需要安装 requests: pip install requests")
    sys.exit(1)


def get_device_id():
    """获取设备标识，用于游标文件和上报。"""
    for env in ["HOSTNAME", "USER"]:
        val = os.environ.get(env, "")
        if val:
            return val
    return "phone"


def find_db_path():
    """自动查找微信聊天记录数据库。"""
    home = os.path.expanduser("~")
    candidates = [
        f"{home}/storage/shared/happyjoplin/data/webchat/",
        f"{home}/codebase/happyjoplin/data/webchat/",
        f"{home}/happyjoplin/data/webchat/",
    ]
    for d in candidates:
        if os.path.isdir(d):
            db_files = [f for f in os.listdir(d) if f.startswith("wcitemsall_") and f.endswith(".db")]
            if db_files:
                return os.path.join(d, db_files[0])
    return None


def show_stats(db_path, account):
    """展示数据库概况：总记录 / Recording 数 / mp3 存在率估算。"""
    table = f"wc_{account}"
    conn = sqlite3.connect(db_path)
    total = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    rec_total = conn.execute(f"SELECT COUNT(*) FROM [{table}] WHERE type='Recording'").fetchone()[0]

    # 抽样检查 mp3 文件实际存在率
    samples = conn.execute(
        f"SELECT content FROM [{table}] WHERE type='Recording' LIMIT 500"
    ).fetchall()
    exist = sum(1 for (c,) in samples if c and os.path.exists(c))
    sample_n = len(samples)
    rate = exist / sample_n * 100 if sample_n else 0
    estimated = rec_total * exist // sample_n if sample_n else 0

    # 游标
    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".phone_sync_cursor_{account}.json")
    cursor = load_cursor(cursor_file)
    max_id = conn.execute(f"SELECT MAX(id) FROM [{table}]").fetchone()[0] or 0
    conn.close()

    print(f"=== {account} 数据库概况 ===")
    print(f"总记录:       {total:,}")
    print(f"Recording:    {rec_total:,}")
    print(f"mp3 存在率:   {exist}/{sample_n} ({rate:.0f}%)")
    print(f"估算 mp3 数: ~{estimated:,}")
    print(f"同步游标:     {cursor:,} / 最大 id {max_id:,}")
    print(f"待同步:       {max_id - cursor:,} 条")
    return {"total": total, "recording": rec_total, "mp3_estimate": estimated, "cursor": cursor, "pending": max_id - cursor}


def load_cursor(cursor_file):
    """读取游标文件。"""
    if os.path.exists(cursor_file):
        with open(cursor_file) as f:
            data = json.load(f)
            return data.get("last_id", 0)
    return 0


def save_cursor(cursor_file, last_id):
    """写入游标文件。"""
    with open(cursor_file, "w") as f:
        json.dump({"last_id": last_id, "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}, f)


def push_records(db_path, account, api_url, limit=200, full=False):
    """推送增量记录到 hcx。自动跳过重复区间，累计到 limit 条实际写入后停。

    Args:
        limit: 实际写入 hcx 的目标条数（也是每次 HTTP 取多少行）
        full: True=重置游标全量推送
    """
    table = f"wc_{account}"
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".phone_sync_cursor_{account}.json")
    since_id = 0 if full else load_cursor(cursor_file)

    # 获取实际待处理行数用于真实进度
    conn = sqlite3.connect(db_path)
    total_pending = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE id > ?", (since_id,)
    ).fetchone()[0]
    conn.close()

    total_inserted = 0
    total_processed = 0

    while total_inserted < limit:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            f"SELECT id, time, send, sender, type, content FROM [{table}] WHERE id > ? ORDER BY id LIMIT ?",
            (since_id, limit),
        ).fetchall()
        conn.close()

        if not rows:
            break

        records = []
        for r in rows:
            records.append({
                "time": str(r[1]) if r[1] else "",
                "send": bool(r[2]),
                "sender": str(r[3]) if r[3] else "",
                "type": str(r[4]) if r[4] else "",
                "content": str(r[5]) if r[5] else "",
            })

        last_id = rows[-1][0]
        total_processed += len(records)
        pct = total_processed / total_pending * 100 if total_pending > 0 else 100

        try:
            resp = requests.post(
                api_url,
                json={"account": account, "records": records, "source": get_device_id()},
                timeout=120,
            )
            if resp.ok:
                data = resp.json()
                inserted = data.get("inserted", 0)
                save_cursor(cursor_file, last_id)
                since_id = last_id

                if inserted > 0:
                    total_inserted += inserted
                    print(f"  [{pct:.1f}%] +{inserted} 条写入 (累计 {total_inserted}/{limit})")
                elif total_processed % (limit * 25) == 0:
                    print(f"  [{pct:.1f}%] 已处理 {total_processed}/{total_pending}")
            else:
                print(f"  → HTTP {resp.status_code}: {resp.text[:200]}")
                break
        except Exception as e:
            print(f"  → 推送失败: {e}")
            break

    if total_inserted == 0:
        if total_processed > 0:
            print(f"无新数据，已处理 {total_processed} 条均为重复")
        else:
            print("无新数据")
    else:
        print(f"完成: 写入 {total_inserted} 条，已处理 {total_processed}/{total_pending}")
    return {"inserted": total_inserted, "processed": total_processed}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="手机端聊天记录推送同步")
    parser.add_argument("--account", default="白晔峰", help="微信账号")
    parser.add_argument("--limit", type=int, default=200, help="实际写入目标条数")
    parser.add_argument("--full", action="store_true", help="全量重推")
    parser.add_argument("--db", default="", help="数据库路径")
    parser.add_argument("--stats", action="store_true", help="展示数据库概况")
    parser.add_argument(
        "--api",
        default="https://ollama.strcoder.com/voice/chat/sync",
        help="hcx 推送接口",
    )
    args = parser.parse_args()

    # 数据库路径
    if args.db:
        db_path = args.db
    else:
        db_path = find_db_path()
        if not db_path:
            print("未找到 webchat 数据目录，请用 --db 指定")
            sys.exit(1)
        print(f"自动检测数据库: {db_path}")

    if args.stats:
        show_stats(db_path, args.account)
    else:
        push_records(db_path, args.account, args.api, limit=args.limit, full=args.full)
