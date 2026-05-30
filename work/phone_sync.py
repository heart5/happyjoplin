#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pixel 6 Pro → hcx 聊天记录推送同步。

轻量级，只需 sqlite3 + json + requests（均为标准库或常用库）。
用本地 JSON 文件存同步游标，HTTP POST 至 hcx 的 /chat/sync 端点。

使用：
    python work/phone_sync.py                          # 增量推送
    python work/phone_sync.py --full                   # 全量重推（重置游标）
    python work/phone_sync.py --account 白晔峰 --limit 200  # 每次200条
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
    # Termux 下的简单标识
    for env in ["HOSTNAME", "USER"]:
        val = os.environ.get(env, "")
        if val:
            return val
    return "phone"


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
    """推送增量记录到 hcx。

    Args:
        db_path: 本地 SQLite 数据库路径
        account: 微信账号名
        api_url: hcx chat/sync 接口地址
        limit: 每次最多推送条数
        full: True=重置游标全量推送
    """
    table = f"wc_{account}"
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    # 游标
    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".phone_sync_cursor_{account}.json")
    since_id = 0 if full else load_cursor(cursor_file)

    # 读取新记录
    conn = sqlite3.connect(db_path)
    max_id = conn.execute(f"SELECT MAX(id) FROM [{table}]").fetchone()[0] or 0
    if since_id >= max_id:
        print(f"无新数据 (cursor={since_id}, max={max_id})")
        conn.close()
        return

    rows = conn.execute(
        f"SELECT id, time, send, sender, type, content FROM [{table}] WHERE id > ? ORDER BY id LIMIT ?",
        (since_id, limit),
    ).fetchall()
    conn.close()

    if not rows:
        print("无新数据")
        return

    # 转换格式
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
    print(f"推送 {len(records)} 条记录 (id {since_id+1}→{last_id})")

    # 发送
    try:
        resp = requests.post(
            api_url,
            json={"account": account, "records": records, "source": get_device_id()},
            timeout=60,
        )
        if resp.ok:
            data = resp.json()
            inserted = data.get("inserted", 0)
            print(f"  → 成功: 接收 {data.get('received', 0)}, 写入 {inserted}")
            # 更新游标（用本次推送的最后一条 id）
            save_cursor(cursor_file, last_id)
            return {"pushed": len(records), "inserted": inserted}
        else:
            print(f"  → HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"  → 推送失败: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="手机端聊天记录推送同步")
    parser.add_argument("--account", default="白晔峰", help="微信账号")
    parser.add_argument("--limit", type=int, default=200, help="每次推送条数")
    parser.add_argument("--full", action="store_true", help="全量重推")
    parser.add_argument("--db", default="", help="数据库路径")
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
        # Termux 默认路径
        home = os.path.expanduser("~")
        candidates = [
            f"{home}/storage/shared/happyjoplin/data/webchat/",
            f"{home}/codebase/happyjoplin/data/webchat/",
            f"{home}/happyjoplin/data/webchat/",
        ]
        wc_dir = None
        for d in candidates:
            if os.path.isdir(d):
                wc_dir = d
                break
        if not wc_dir:
            print("未找到 webchat 数据目录，请用 --db 指定")
            sys.exit(1)
        # 查找数据库文件
        db_files = [f for f in os.listdir(wc_dir) if f.startswith("wcitemsall_") and f.endswith(".db")]
        if not db_files:
            print(f"在 {wc_dir} 未找到数据库文件")
            sys.exit(1)
        db_path = os.path.join(wc_dir, db_files[0])
        print(f"自动检测数据库: {db_path}")

    push_records(db_path, args.account, args.api, limit=args.limit, full=args.full)
