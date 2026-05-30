# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     notebook_metadata_filter: jupytext,-kernelspec,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # 微信聊天记录数据库去重

# %% [markdown]
# SQL 级去重：路径标准化 → 统计重复 → DELETE → VACUUM

# %%
"""tc 生产数据库去重工具。

三步：路径标准化 → GROUP BY 去重 → VACUUM 回收空间
支持 --dry-run（默认）和 --confirm

使用：
    python work/dedup_wcitems.py                          # 统计重复
    python work/dedup_wcitems.py --confirm                # 执行去重
    python work/dedup_wcitems.py --account 白晔峰 --confirm
"""

# %%
import argparse
import os
import sqlite3 as lite
import time

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain
    from func.logme import log

# %%
# 已知的绝对路径前缀（历史项目根目录），统一转为相对路径
_PATH_PREFIXES = [
    "/home/baiyefeng/codebase/happyjoplin/",
    "/home/baiyefeng/codebase/everwork/",
    "/home/baiyefeng/happyjoplin/",
]


def _normalize_paths(conn, table):
    """将绝对路径转为相对路径，多个历史前缀逐一处理。"""
    normalized = 0
    for prefix in _PATH_PREFIXES:
        cur = conn.execute(
            f"UPDATE [{table}] SET content = REPLACE(content, ?, '') WHERE content LIKE ?",
            (prefix, prefix + "%"),
        )
        if cur.rowcount > 0:
            log.info(f"  路径标准化 {prefix} → {cur.rowcount} 条")
            normalized += cur.rowcount
    conn.commit()
    return normalized


def _count_duplicates(conn, table):
    """统计当前重复数量。"""
    total = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    unique = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM [{table}] GROUP BY time, send, sender, type, content)"
    ).fetchone()[0]
    dup = total - unique
    return total, unique, dup


def dedup_table(db_path, account, confirm=False):
    """对指定账号表执行去重。

    Args:
        db_path: 数据库文件路径
        account: 微信账号名
        confirm: False=仅统计, True=执行去重+VACUUM

    Returns:
        dict: {total, unique, duplicates, normalized, deleted}
    """
    table = f"wc_{account}"
    conn = lite.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # 1. 事前统计
    total_before, unique_before, dup_before = _count_duplicates(conn, table)
    log.info(f"去重前: {total_before} 行, 唯一 {unique_before}, 重复 {dup_before}")

    if dup_before == 0:
        log.info("无重复数据，跳过")
        conn.close()
        return {
            "total": total_before,
            "unique": unique_before,
            "duplicates": 0,
            "normalized": 0,
            "deleted": 0,
        }

    if not confirm:
        # 估算可释放空间
        db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
        est_freed = db_size_mb * (dup_before / total_before) * 0.7  # 70% 估算
        log.info(
            f"[DRY-RUN] 可去重 {dup_before} 行 ({dup_before/total_before*100:.1f}%), "
            f"估算释放 {est_freed:.0f}MB"
        )
        conn.close()
        return {
            "total": total_before,
            "unique": unique_before,
            "duplicates": dup_before,
            "normalized": 0,
            "deleted": 0,
        }

    # 2. 路径标准化
    n_norm = _normalize_paths(conn, table)

    # 3. 路径标准化后重新统计
    if n_norm > 0:
        total_mid, unique_mid, dup_mid = _count_duplicates(conn, table)
        log.info(f"路径标准化后: {total_mid} 行, 唯一 {unique_mid}, 重复 {dup_mid}")
    else:
        total_mid, dup_mid = total_before, dup_before

    # 4. SQL 去重：保留每条记录的最小 rowid
    log.info(f"开始去重，删除 {dup_mid} 条重复记录...")
    t0 = time.time()
    cur = conn.execute(
        f"DELETE FROM [{table}] WHERE rowid NOT IN ("
        f"  SELECT MIN(rowid) FROM [{table}] GROUP BY time, send, sender, type, content"
        f")"
    )
    deleted = cur.rowcount
    conn.commit()
    elapsed = time.time() - t0
    log.info(f"删除 {deleted} 条重复记录 ({elapsed:.1f}s)")

    # 5. VACUUM 回收空间
    size_before = os.path.getsize(db_path) / (1024 * 1024)
    log.info("VACUUM 回收空间...")
    t0 = time.time()
    conn.execute("VACUUM")
    conn.commit()
    size_after = os.path.getsize(db_path) / (1024 * 1024)
    log.info(
        f"VACUUM {elapsed + time.time() - t0:.1f}s: "
        f"{size_before:.0f}MB → {size_after:.0f}MB (释放 {size_before - size_after:.0f}MB)"
    )

    # 6. 事后统计
    total_after, unique_after, dup_after = _count_duplicates(conn, table)
    log.info(f"去重后: {total_after} 行, 唯一 {unique_after}, 重复 {dup_after}")

    conn.close()
    return {
        "total": total_after,
        "unique": unique_after,
        "duplicates": dup_after,
        "normalized": n_norm,
        "deleted": deleted,
    }


# %%
def cmd_backup(db_path):
    """备份数据库。"""
    backup_path = db_path + f".bak.{int(time.time())}"
    log.info(f"备份: {db_path} → {backup_path}")
    src = lite.connect(db_path)
    dst = lite.connect(backup_path)
    src.backup(dst)
    dst.close()
    src.close()
    size_mb = os.path.getsize(backup_path) / (1024 * 1024)
    log.info(f"备份完成: {size_mb:.0f}MB")
    return backup_path


# %%
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="微信聊天记录数据库去重")
    parser.add_argument("--account", default="白晔峰", help="微信账号")
    parser.add_argument("--confirm", action="store_true", help="确认执行去重（默认 dry-run）")
    parser.add_argument(
        "--db",
        default="",
        help="数据库路径（默认自动检测）",
    )
    args = parser.parse_args()

    if args.db:
        db_path = args.db
    else:
        wcdatapath = getdirmain() / "data" / "webchat"
        import subprocess

        whoami = subprocess.run(["whoami"], capture_output=True, text=True).stdout.strip()
        from func.getid import getdevicename

        dbfilename = f"wcitemsall_({getdevicename()})_({whoami}).db".replace(" ", "_")
        db_path = str(wcdatapath / dbfilename)

    if not os.path.exists(db_path):
        log.error(f"数据库不存在: {db_path}")
        exit(1)

    if args.confirm:
        log.critical(
            f"将要去重数据库 {db_path} 的 wc_{args.account} 表，"
            f"操作不可逆，先备份..."
        )
        cmd_backup(db_path)

    result = dedup_table(db_path, args.account, confirm=args.confirm)
    if args.confirm:
        log.info(f"去重结果: {result}")
    else:
        log.info(f"[DRY-RUN] 结果: {result}")
        log.info("使用 --confirm 执行实际去重")
