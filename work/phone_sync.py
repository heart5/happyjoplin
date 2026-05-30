#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pixel 6 Pro → hcx 聊天记录推送同步。

轻量级，只需 sqlite3 + json + requests（均为标准库或常用库）。
用本地 JSON 文件存同步游标，HTTP POST 至 hcx 的 /chat/sync 端点。

使用：
    python work/phone_sync.py                          # 增量推送（自动跳过重复）
    python work/phone_sync.py --full                   # 全量重推（重置游标）
    python work/phone_sync.py --stats                  # 查看数据库概况
    python work/phone_sync.py --limit 10000            # 每轮写满10000条后停
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

# ---------------------------------------------------------------------------
# 内部常量
# ---------------------------------------------------------------------------
_FETCH_CAP = 2000  # 单次 HTTP 最多取多少行（手机内存/网络平衡点）


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# 核心功能
# ---------------------------------------------------------------------------
def show_stats(db_path, account, debug_mp3=False):
    """展示数据库概况：总记录 / Recording 数 / mp3 存在率估算 / 游标 / 待处理行数。"""
    table = f"wc_{account}"
    conn = sqlite3.connect(db_path)

    total = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    rec_total = conn.execute(f"SELECT COUNT(*) FROM [{table}] WHERE type='Recording'").fetchone()[0]

    # --- mp3 存在率 ---
    samples = conn.execute(
        f"SELECT content FROM [{table}] WHERE type='Recording' ORDER BY id DESC LIMIT 500"
    ).fetchall()
    sample_n = len(samples)

    # 尝试多个根目录（Android 存储路径多样，不过滤 isdir）
    roots = [os.path.dirname(os.path.dirname(os.path.dirname(db_path)))]
    home = os.path.expanduser("~")
    roots += [
        f"{home}/storage/shared/happyjoplin",
        f"{home}/storage/shared/0code/happyjoplin",
        "/storage/emulated/0/happyjoplin",
        "/storage/emulated/0/0code/happyjoplin",
        "/sdcard/happyjoplin",
        "/sdcard/0code/happyjoplin",
    ]

    exist = 0
    found_root = None
    for (c,) in samples:
        if not c:
            continue
        if c.startswith("/"):
            if os.path.exists(c):
                exist += 1
        else:
            for r in roots:
                if os.path.exists(os.path.join(r, c)):
                    exist += 1
                    found_root = r
                    break

    rate = exist / sample_n * 100 if sample_n else 0
    estimated = rec_total * exist // sample_n if sample_n else 0

    # 调试：打印前几条路径解析
    if debug_mp3:
        print("\n--- mp3 路径调试 (前5条) ---")
        for (c,) in samples[:5]:
            if not c:
                continue
            print(f"\nDB: {c}")
            if c.startswith("/"):
                print(f"  abs → {os.path.exists(c)}")
            else:
                for r in roots:
                    print(f"  {r}/... → {os.path.exists(os.path.join(r, c))}")
        print(f"匹配根目录: {found_root}")
        print("---\n")

    # 游标 + 待处理实际行数（COUNT(*)，非稀疏 id 范围）
    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".phone_sync_cursor_{account}.json")
    cursor = load_cursor(cursor_file)
    pending = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE id > ?", (cursor,)
    ).fetchone()[0]

    conn.close()

    print(f"=== {account} 数据库概况 ===")
    print(f"总记录:       {total:,}")
    print(f"Recording:    {rec_total:,}")
    print(f"mp3 存在率:   {exist}/{sample_n} ({rate:.0f}%)")
    print(f"估算 mp3 数: ~{estimated:,}")
    print(f"游标:         id={cursor:,}")
    print(f"待处理:       {pending:,} 行 (实际)")
    return {"total": total, "recording": rec_total, "mp3_estimate": estimated,
            "cursor": cursor, "pending": pending}


def push_records(db_path, account, api_url, target=2000, full=False):
    """推送增量记录到 hcx。自动跳过重复，累计实际写入 target 条后停。

    Args:
        target: 实际写入目标条数（--limit 传入）
        full: True=重置游标全量推送
    """
    table = f"wc_{account}"
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    # --- 游标 ---
    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".phone_sync_cursor_{account}.json")
    cursor = 0 if full else load_cursor(cursor_file)

    # --- 待处理行数（真实分母） ---
    conn = sqlite3.connect(db_path)
    total_pending = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE id > ?", (cursor,)
    ).fetchone()[0]
    conn.close()

    written = 0     # 累计实际写入 hcx 的条数
    sent = 0        # 累计已发送的行数
    batch = min(_FETCH_CAP, target)  # 每轮取行上限

    while written < target:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            f"SELECT id, time, send, sender, type, content FROM [{table}] "
            f"WHERE id > ? ORDER BY id LIMIT ?",
            (cursor, batch),
        ).fetchall()
        conn.close()

        if not rows:
            break

        # 组装 JSON
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
        sent += len(records)
        pct = sent / total_pending * 100 if total_pending > 0 else 100

        # HTTP 推送
        try:
            resp = requests.post(
                api_url,
                json={"account": account, "records": records, "source": get_device_id()},
                timeout=120,
            )
        except Exception as e:
            print(f"  → 推送失败: {e}")
            break

        if not resp.ok:
            print(f"  → HTTP {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json()
        inserted = data.get("inserted", 0)
        save_cursor(cursor_file, last_id)
        cursor = last_id

        if inserted > 0:
            written += inserted
            print(f"  [{pct:.1f}%] +{inserted} 条写入 (累计 {written}/{target})")
        elif sent % (batch * 10) == 0:
            print(f"  [{pct:.1f}%] 重复跳过，已扫描 {sent}/{total_pending}")

    # --- 最终报告 ---
    if written == 0:
        if sent > 0:
            print(f"本轮无新数据，扫描 {sent} 行均为重复")
        else:
            print("无新数据")
    else:
        print(f"完成: 写入 {written} 条，扫描 {sent}/{total_pending} 行")
    return {"written": written, "scanned": sent}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="手机端聊天记录推送同步")
    parser.add_argument("--account", default="白晔峰", help="微信账号")
    parser.add_argument("--limit", type=int, default=2000, help="每轮实际写入目标条数")
    parser.add_argument("--full", action="store_true", help="全量重推（重置游标）")
    parser.add_argument("--db", default="", help="数据库路径")
    parser.add_argument("--stats", action="store_true", help="查看数据库概况")
    parser.add_argument("--debug-mp3", action="store_true", help="(with --stats) 调试 mp3 路径解析")
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
        show_stats(db_path, args.account, debug_mp3=args.debug_mp3)
    else:
        push_records(db_path, args.account, args.api, target=args.limit, full=args.full)
