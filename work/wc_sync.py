#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""全平台聊天记录推送同步（手机/tc/Linux 通用） → hcx 合并库。

轻量级，只需 sqlite3 + json + requests（均为标准库或常用库）。
用本地 JSON 文件存同步游标，HTTP POST 至 hcx 的 /chat/sync 端点。

使用：
    python work/wc_sync.py                          # 增量推送（自动跳过重复）
    python work/wc_sync.py --full                   # 全量重推（重置游标）
    python work/wc_sync.py --stats                  # 查看数据库概况
    python work/wc_sync.py --stats --debug-mp3      # 查看概况 + mp3 路径调试
    python work/wc_sync.py --limit 10000            # 每轮写满10000条后停
    python work/wc_sync.py --transcribe --limit 50  # 上传 mp3 至 hcx 语音转文字
    python work/wc_sync.py --clean --dry-run        # 查看可清理的已转录 mp3
    python work/wc_sync.py --clean                  # 删除本地已转录 mp3
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
    for env in ["HOSTNAME"]:
        val = os.environ.get(env, "")
        if val:
            return val
    try:
        import subprocess
        model = subprocess.run(
            ["getprop", "ro.product.model"], capture_output=True, text=True, timeout=5
        ).stdout.strip()
        if model:
            return model.replace(" ", "")
    except Exception:
        pass
    # Linux 回退
    try:
        return os.uname().nodename.split(".")[0]
    except Exception:
        pass
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

    # 调试：前几条路径解析 + 4-6月专项检查
    if debug_mp3:
        print("\n--- mp3 路径调试 (前5条最新) ---")
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

        # 4-6月旧语音专项检查（手机独有数据的日期范围）
        old_samples = conn.execute(
            f"SELECT content FROM [{table}] WHERE type='Recording' "
            f"AND (content LIKE 'img/webchat/202504%' OR content LIKE 'img/webchat/202505%' OR content LIKE 'img/webchat/202506%') "
            f"ORDER BY id DESC LIMIT 100"
        ).fetchall()
        old_exist = sum(1 for (c,) in old_samples if c and any(
            os.path.exists(os.path.join(r, c)) for r in roots
        ))
        print(f"\n4-6月旧语音抽查100条: {old_exist}/{len(old_samples)} 存在")
        print("---\n")

    # 游标 + 进度（COUNT(*)，非稀疏 id 范围）
    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".wc_sync_cursor_{account}.json")
    cursor = load_cursor(cursor_file)
    scanned = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE id <= ?", (cursor,)
    ).fetchone()[0]
    pending = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE id > ?", (cursor,)
    ).fetchone()[0]
    rec_scanned = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE type='Recording' AND id <= ?", (cursor,)
    ).fetchone()[0]
    conn.close()

    pct_done = scanned / total * 100 if total > 0 else 0
    rec_pct = rec_scanned / rec_total * 100 if rec_total > 0 else 0

    print(f"=== {account} 数据库概况 ===")
    print(f"总记录:       {total:,}")
    print(f"已同步:       {scanned:,} 行 ({pct_done:.1f}%)")
    print(f"Recording:    {rec_total:,}  (已同步 {rec_scanned:,}, {rec_pct:.1f}%)")
    print(f"mp3 存在率:   {exist}/{sample_n} ({rate:.0f}%)  估算 ~{estimated:,} 个")
    print(f"游标:         id={cursor:,}")
    print(f"待处理:       {pending:,} 行 (实际)")
    return {"total": total, "recording": rec_total, "mp3_estimate": estimated,
            "cursor": cursor, "scanned": scanned, "pending": pending}


def _normalize_time(val):
    """将各种时间值统一转为 unix 时间戳字符串，匹配 v4txt_v2.msg_time。"""
    if val is None:
        return ""
    if isinstance(val, (int, float)):
        return str(int(val))
    try:
        return str(int(val.timestamp()))
    except Exception:
        pass
    if isinstance(val, str):
        try:
            return str(int(float(val)))
        except (ValueError, OverflowError):
            pass
    return str(val)


def _get_mp3_roots(db_path):
    """获取手机端 mp3 文件的可能根目录列表。"""
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
    return roots


def _resolve_mp3(content, roots):
    """解析 mp3 相对路径为绝对路径，文件不存在返回 None。"""
    if not content or not content.endswith(".mp3"):
        return None
    if content.startswith("/"):
        return content if os.path.exists(content) else None
    for r in roots:
        fpath = os.path.join(r, content)
        if os.path.exists(fpath):
            return fpath
    return None


def transcribe_records(db_path, account, voice_url="https://ollama.strcoder.com/voice", write_target=50):
    """上传手机端 mp3 至 hcx 语音转文字。

    1. 扫描 Recording 记录，解析 mp3 文件路径
    2. 批量查 hcx 已转录记录，跳过
    3. 逐个上传未转录 mp3 至 /transcribe
    4. 本地 JSON 游标记录进度

    Args:
        write_target: 实际转录成功多少条后停（非扫描数）
    """
    table = f"wc_{account}"
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".wc_transcribe_cursor_{account}.json")
    cursor_id = load_cursor(cursor_file)

    roots = _get_mp3_roots(db_path)

    conn = sqlite3.connect(db_path)
    total_pending = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE type='Recording' AND id > ?", (cursor_id,)
    ).fetchone()[0]
    print(f"待转录 Recording: {total_pending} 条 (游标 id={cursor_id})")

    already_done = 0
    already_scanned = 0

    while already_done < write_target:
        fetch_size = min(_FETCH_CAP, max(write_target - already_done, 20))
        rows = conn.execute(
            f"SELECT id, time, sender, content FROM [{table}] "
            f"WHERE type='Recording' AND id > ? ORDER BY id LIMIT ?",
            (cursor_id, fetch_size),
        ).fetchall()

        if not rows:
            print("无更多 Recording 记录")
            break

        last_id = rows[-1][0]

        # 解析 mp3 路径
        mp3_records = []
        for r in rows:
            rid, msg_time, sender, content = r
            fpath = _resolve_mp3(content, roots)
            if fpath:
                mp3_records.append((rid, msg_time, sender, fpath))

        already_scanned += len(rows)

        if not mp3_records:
            save_cursor(cursor_file, last_id)
            cursor_id = last_id
            continue

        # 批量查已转录（最多重试2次）
        already_set = set()
        check_records = [(_normalize_time(t), str(s)) for _, t, s, _ in mp3_records]
        for attempt in range(2):
            try:
                resp = requests.post(
                    f"{voice_url}/transcriptions/batch",
                    json={"account": account, "records": [[t, s] for t, s in check_records]},
                    timeout=30,
                )
                if resp.ok:
                    for key in resp.json().get("results", {}):
                        t, s = key.split("#", 1)
                        already_set.add((t, s))
                    break
            except Exception as e:
                if attempt == 0:
                    time.sleep(3)
                else:
                    print(f"  ⚠ 查询已转录失败: {e}")

        # 逐个上传
        for rid, msg_time, sender, fpath in mp3_records:
            if already_done >= write_target:
                break

            nt = _normalize_time(msg_time)
            if (nt, str(sender)) in already_set:
                continue

            fname = os.path.basename(fpath)
            for attempt in range(3):
                try:
                    with open(fpath, "rb") as fh:
                        resp = requests.post(
                            f"{voice_url}/transcribe",
                            files={"file": (fname, fh)},
                            data={"account": account, "msg_time": nt, "sender": str(sender), "source": get_device_id()},
                            timeout=120,
                        )
                    if resp.ok:
                        data = resp.json()
                        text = data.get("text", "")
                        if text:
                            already_done += 1
                            print(f"  ✓ [{already_done}/{write_target}] {fname} → {text[:60]}...")
                        else:
                            print(f"  ⚠ [{already_done}/{write_target}] {fname} 转录为空")
                            already_done += 1
                        break
                    else:
                        print(f"  ✗ {fname}: HTTP {resp.status_code} (尝试{attempt+1}/3)")
                        if attempt < 2:
                            time.sleep(5)
                except Exception as e:
                    print(f"  ✗ {fname}: {e} (尝试{attempt+1}/3)")
                    if attempt < 2:
                        time.sleep(5)

        save_cursor(cursor_file, last_id)
        cursor_id = last_id

    conn.close()
    pct = already_scanned / total_pending * 100 if total_pending > 0 else 100
    print(f"转录完成: 成功 {already_done} 条, 扫描 {already_scanned}/{total_pending} ({pct:.1f}%)")
    return {"transcribed": already_done, "scanned": already_scanned}


def clean_transcribed_mp3(db_path, account, voice_url="https://ollama.strcoder.com/voice", dry_run=True):
    """删除本地已转录的 mp3 文件。

    1. 扫描 Recording 记录，找到 mp3 存在的
    2. 批量查 hcx /transcriptions/batch 确认已转录
    3. dry_run=True 时只报告，不删除

    Returns: {"scanned": N, "hit": N, "deleted": N, "missing": N, "freed_mb": N}
    """
    table = f"wc_{account}"
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    roots = _get_mp3_roots(db_path)
    conn = sqlite3.connect(db_path)

    # 查所有 Recording 记录数
    total = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE type='Recording'"
    ).fetchone()[0]
    print(f"Recording 总数: {total:,}")

    scanned, hit, deleted, missing, freed = 0, 0, 0, 0, 0
    batch_size = 200

    offset_id = 0
    while True:
        rows = conn.execute(
            f"SELECT id, time, sender, content FROM [{table}] "
            f"WHERE type='Recording' AND id > ? ORDER BY id LIMIT ?",
            (offset_id, batch_size),
        ).fetchall()

        if not rows:
            break

        # 解析 mp3 路径
        mp3_map = {}
        for rid, msg_time, sender, content in rows:
            fpath = _resolve_mp3(content, roots)
            if fpath:
                mp3_map[(rid, msg_time, sender)] = fpath

        scanned += len(rows)
        offset_id = rows[-1][0]

        if not mp3_map:
            continue

        # 批量查已转录
        transcribed = set()
        try:
            check_records = [(_normalize_time(t), str(s)) for _, t, s in mp3_map]
            resp = requests.post(
                f"{voice_url}/transcriptions/batch",
                json={"account": account, "records": [[t, s] for t, s in check_records]},
                timeout=60,
            )
            if resp.ok:
                for key in resp.json().get("results", {}):
                    t, s = key.split("#", 1)
                    transcribed.add((t, s))
        except Exception as e:
            print(f"  ⚠ 查询已转录失败: {e}")

        # 删除命中文件
        for (rid, msg_time, sender), fpath in mp3_map.items():
            nt = _normalize_time(msg_time)
            if (nt, str(sender)) not in transcribed:
                continue
            hit += 1
            if os.path.exists(fpath):
                fsize_mb = os.path.getsize(fpath) / (1024 * 1024)
                if not dry_run:
                    os.remove(fpath)
                deleted += 1
                freed += fsize_mb
            else:
                missing += 1

        pct = scanned / total * 100 if total > 0 else 100
        action = "可删" if dry_run else "已删"
        print(f"  [{pct:.1f}%] {action} {deleted} 个, 已命中 {hit}, 缺失 {missing}, 释放 {freed:.1f}MB")

    conn.close()

    if dry_run:
        print(f"\n[DRY-RUN] 扫描 {scanned:,} 条, 命中转录 {hit} 条, 可删除 {deleted} 个文件 ({freed:.1f}MB), 已缺失 {missing}")
    else:
        print(f"\n清理完成: 删除 {deleted} 个文件, 释放 {freed:.1f}MB, 已缺失 {missing}")
    return {"scanned": scanned, "hit": hit, "deleted": deleted, "missing": missing, "freed_mb": round(freed, 1)}


def push_records(db_path, account, api_url, write_target=2000, record_type=None, full=False):
    """推送增量记录到 hcx。自动跳过重复，累计「实际写入」write_target 条后停。

    Args:
        write_target: 计划实际写入多少条（--limit 传入）
        record_type: 只推送此类型（如 'Recording'），None=全部。游标始终覆盖全类型。
        full: True=重置游标全量推送
    """
    table = f"wc_{account}"
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return

    # --- 游标 ---
    cursor_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
    cursor_file = os.path.join(cursor_dir, f".wc_sync_cursor_{account}.json")
    cursor_id = 0 if full else load_cursor(cursor_file)

    # --- 待处理行数（进度条分母，按过滤类型） ---
    conn = sqlite3.connect(db_path)
    type_filter_sql = f"AND type='{record_type}'" if record_type else ""
    total_pending = conn.execute(
        f"SELECT COUNT(*) FROM [{table}] WHERE id > ? {type_filter_sql}", (cursor_id,)
    ).fetchone()[0]
    conn.close()

    already_written = 0     # 累计实际写入 hcx 的数量
    already_scanned = 0     # 累计已扫描（发送）的行数

    while already_written < write_target:
        fetch_size = min(_FETCH_CAP, write_target - already_written)
        conn = sqlite3.connect(db_path)
        # 始终取全类型保证游标不跳号
        all_rows = conn.execute(
            f"SELECT id, time, send, sender, type, content FROM [{table}] "
            f"WHERE id > ? ORDER BY id LIMIT ?",
            (cursor_id, fetch_size),
        ).fetchall()
        conn.close()

        if not all_rows:
            break

        # 游标以全类型最后一条 id 为准
        last_id = all_rows[-1][0]

        # Python 端按类型过滤
        if record_type:
            target_rows = [r for r in all_rows if r[4] == record_type]
        else:
            target_rows = all_rows

        if not target_rows:
            # 这批没有目标类型，推进游标继续
            save_cursor(cursor_file, last_id)
            cursor_id = last_id
            continue

        # 组装 JSON
        records = []
        for r in target_rows:
            records.append({
                "time": str(r[1]) if r[1] else "",
                "send": bool(r[2]),
                "sender": str(r[3]) if r[3] else "",
                "type": str(r[4]) if r[4] else "",
                "content": str(r[5]) if r[5] else "",
            })

        already_scanned += len(records)
        pct = already_scanned / total_pending * 100 if total_pending > 0 else 100

        # HTTP 推送（最多重试3次）
        for attempt in range(3):
            try:
                resp = requests.post(
                    api_url,
                    json={"account": account, "records": records, "source": get_device_id()},
                    timeout=120,
                )
                if resp.ok:
                    break
                print(f"  → HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                print(f"  → 推送失败 (尝试{attempt+1}/3): {e}")
                if attempt < 2:
                    wait = (attempt + 1) * 5
                    print(f"     等待{wait}s后重试...")
                    time.sleep(wait)
        else:
            # 3次全部失败，保留游标不推进，下次从断点继续
            print("  → 3次重试均失败，游标未推进，下次从断点继续")
            break

        data = resp.json()
        newly_inserted = data.get("inserted", 0)
        save_cursor(cursor_file, last_id)
        cursor_id = last_id

        if newly_inserted > 0:
            already_written += newly_inserted
            tag = f"[{record_type}]" if record_type else ""
            print(f"  [{pct:.1f}%] {tag} 实际写入 +{newly_inserted} 条 (累计 {already_written}/{write_target})")
        elif already_scanned % (fetch_size * 10) == 0:
            print(f"  [{pct:.1f}%] 重复跳过，已扫描 {already_scanned}/{total_pending}")

    # --- 最终报告 ---
    if already_written == 0:
        if already_scanned > 0:
            print(f"本轮无新数据，扫描 {already_scanned} 行均为重复")
        else:
            print("无新数据")
    else:
        tag = f"[{record_type}] " if record_type else ""
        print(f"完成: {tag}实际写入 {already_written} 条，共扫描 {already_scanned}/{total_pending} 行")
    return {"written": already_written, "scanned": already_scanned}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="手机端聊天记录推送同步")
    parser.add_argument("--account", default="白晔峰", help="微信账号")
    parser.add_argument("--limit", type=int, default=2000, help="实际写入多少条后停（非扫描数/非游标）")
    parser.add_argument("--type", dest="record_type", default=None, help="只推送此类型（如 Recording）")
    parser.add_argument("--full", action="store_true", help="全量重推（重置游标）")
    parser.add_argument("--db", default="", help="数据库路径")
    parser.add_argument("--stats", action="store_true", help="查看数据库概况")
    parser.add_argument("--debug-mp3", action="store_true", help="(with --stats) 调试 mp3 路径解析")
    parser.add_argument(
        "--api",
        default="https://ollama.strcoder.com/voice/chat/sync",
        help="hcx 推送接口",
    )
    parser.add_argument("--transcribe", action="store_true", help="上传 mp3 至 hcx 语音转文字")
    parser.add_argument("--clean", action="store_true", help="删除本地已转录的 mp3 文件")
    parser.add_argument("--dry-run", action="store_true", help="(with --clean) 仅报告不删除")
    parser.add_argument(
        "--voice-url",
        default="https://ollama.strcoder.com/voice",
        help="hcx voice API 地址",
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
    elif args.transcribe:
        transcribe_records(db_path, args.account, voice_url=args.voice_url, write_target=args.limit)
    elif args.clean:
        clean_transcribed_mp3(db_path, args.account, voice_url=args.voice_url, dry_run=args.dry_run)
    else:
        push_records(db_path, args.account, args.api, write_target=args.limit,
                     record_type=args.record_type, full=args.full)
