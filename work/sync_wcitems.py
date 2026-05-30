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
# # 聊天记录增量同步 — tc/手机 → hcx 合并库

# %%
"""从各活跃主机增量同步聊天记录到 hcx 合并库。

运行方式：
    python work/sync_wcitems.py              # 增量同步所有主机
    python work/sync_wcitems.py --full tc    # 全量重拉 tc
    python work/sync_wcitems.py --status     # 查看同步状态
"""

# %%
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

# %%
import pathmagic

with pathmagic.context():
    from func.first import getdirmain
    from func.logme import log

# %%
# 合并库统一集中在 joplinai/data/ 下（hcx 全局数据中心）
MERGED_DB = str(
    getdirmain().parent / "joplinai" / "data" / "wcitemsall_merged.db"
)

# tc 端配置
TC_HOST = "tc"
TC_PYTHON = "/usr/miniconda3/envs/newlsp/bin/python3"
TC_DB = (
    "/home/baiyefeng/codebase/happyjoplin/data/webchat/"
    "wcitemsall_(腾讯云Ubuntu22.04)_(baiyefeng).db"
)

# 活跃账号列表
ACCOUNTS = ["白晔峰", "heart5"]


# %% [markdown]
# ## 同步游标管理


# %%
def _ensure_cursor_table(conn):
    """创建同步游标表（幂等）。"""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS sync_cursor (
            host    TEXT PRIMARY KEY,
            account TEXT NOT NULL,
            last_id INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.commit()


def _get_cursor(conn, host, account):
    """获取某主机某账号的最后同步 id。"""
    row = conn.execute(
        "SELECT last_id FROM sync_cursor WHERE host=? AND account=?", (host, account)
    ).fetchone()
    return row[0] if row else 0


def _set_cursor(conn, host, account, last_id):
    """更新同步游标。"""
    conn.execute(
        "INSERT OR REPLACE INTO sync_cursor (host, account, last_id, updated_at) VALUES (?, ?, ?, datetime('now','localtime'))",
        (host, account, last_id),
    )
    conn.commit()


# %% [markdown]
# ## tc 增量同步


# %%
def _ssh_tc(cmd):
    """在 tc 上执行 Python 命令，返回 stdout。"""
    # 用 heredoc 避免 shell 转义问题
    full_cmd = f"ssh {TC_HOST} {TC_PYTHON} << 'PYEOF'\n{cmd}\nPYEOF"
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"SSH 失败: {result.stderr.strip()}")
    return result.stdout.strip()


def _get_tc_max_id(account):
    """获取 tc 上某账号表的当前最大 id。"""
    result = _ssh_tc(
        f"import sqlite3; conn=sqlite3.connect('{TC_DB}'); "
        f"r=conn.execute('SELECT MAX(id) FROM [wc_{account}]').fetchone(); "
        f"conn.close(); print(r[0] if r and r[0] else 0)"
    )
    return int(result)


def _pull_tc_incremental(account, since_id):
    """从 tc 拉取 id > since_id 的行，返回 [(time, send, sender, type, content), ...] 列表。

    使用 JSON Lines 管道传输以正确处理特殊字符。
    """
    script = (
        f"import sqlite3, json\n"
        f"conn = sqlite3.connect('{TC_DB}')\n"
        f"rows = conn.execute('SELECT time, send, sender, type, content FROM [wc_{account}] WHERE id>{since_id} ORDER BY id').fetchall()\n"
        f"for r in rows:\n"
        f"    print(json.dumps([str(x) if x is not None else '' for x in r], ensure_ascii=False))\n"
        f"conn.close()"
    )
    result = subprocess.run(
        f"ssh {TC_HOST} {TC_PYTHON} << 'PYEOF'\n{script}\nPYEOF",
        shell=True, capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"拉取 tc 数据失败: {result.stderr}")

    records = []
    for line in result.stdout.strip().split("\n"):
        if line:
            row = json.loads(line)
            records.append((row[0], row[1], row[2], row[3], row[4]))
    return records


def _insert_batch(conn, account, records, source):
    """批量插入记录到合并库，INSERT OR IGNORE。返回实际新增行数。"""
    inserted = 0
    batch = []
    sql = f"INSERT OR IGNORE INTO [wc_{account}] (time, send, sender, type, content, source) VALUES (?,?,?,?,?,?)"
    for r in records:
        batch.append((r[0], r[1], r[2], r[3], r[4], source))
        if len(batch) >= 5000:
            cur = conn.executemany(sql, batch)
            inserted += cur.rowcount
            batch = []
    if batch:
        cur = conn.executemany(sql, batch)
        inserted += cur.rowcount
    conn.commit()
    return inserted


def sync_tc(account="白晔峰"):
    """增量同步 tc 上某账号的聊天记录到 hcx 合并库。"""
    conn = sqlite3.connect(MERGED_DB)
    _ensure_cursor_table(conn)

    cursor = _get_cursor(conn, "tc", account)
    log.info(f"tc/{account} 当前游标: {cursor}")

    max_id = _get_tc_max_id(account)
    log.info(f"tc/{account} 远端最大 id: {max_id}")

    if cursor >= max_id:
        log.info(f"tc/{account} 无新数据，跳过")
        conn.close()
        return {"status": "uptodate", "cursor": cursor, "max_id": max_id, "pulled": 0}

    log.info(f"tc/{account} 拉取 id {cursor+1} → {max_id} ({max_id - cursor} 行)")
    t0 = time.time()
    records = _pull_tc_incremental(account, cursor)
    pull_time = time.time() - t0
    log.info(f"  拉取 {len(records)} 行 ({pull_time:.1f}s)")

    inserted = _insert_batch(conn, account, records, "tc")
    _set_cursor(conn, "tc", account, max_id)
    insert_time = time.time() - t0 - pull_time
    log.info(f"  写入 {inserted} 行 ({insert_time:.1f}s)")

    # 汇总
    total = conn.execute(f"SELECT COUNT(*) FROM [wc_{account}]").fetchone()[0]
    conn.close()
    return {
        "status": "synced",
        "cursor": max_id,
        "max_id": max_id,
        "pulled": len(records),
        "inserted": inserted,
        "total_local": total,
    }


# %% [markdown]
# ## 命令行入口


# %%
def cmd_status():
    """显示同步状态。"""
    conn = sqlite3.connect(MERGED_DB)
    _ensure_cursor_table(conn)
    print("=== 合并库状态 ===")
    for ac in ACCOUNTS:
        cnt = conn.execute(f"SELECT COUNT(*) FROM [wc_{ac}]").fetchone()[0]
        by_source = conn.execute(
            f"SELECT source, COUNT(*) FROM [wc_{ac}] GROUP BY source"
        ).fetchall()
        print(f"  wc_{ac}: {cnt} 行")
        for s, c in by_source:
            print(f"    source={s}: {c}")
    print("\n=== 同步游标 ===")
    cursors = conn.execute("SELECT host, account, last_id, updated_at FROM sync_cursor").fetchall()
    for host, ac, lid, ts in cursors:
        print(f"  {host}/{ac}: last_id={lid} ({ts})")
    if not cursors:
        print("  (无记录)")
    conn.close()


def cmd_sync(host="tc", account="白晔峰"):
    """执行增量同步。"""
    if host == "tc":
        result = sync_tc(account)
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(f"未知主机: {host}")


def cmd_full(host="tc", account="白晔峰"):
    """全量重拉（重置游标为 0）。"""
    conn = sqlite3.connect(MERGED_DB)
    _ensure_cursor_table(conn)
    _set_cursor(conn, host, account, 0)
    conn.close()
    log.info(f"已重置 {host}/{account} 游标为 0，下次同步将全量拉取")
    # 立即执行同步
    cmd_sync(host, account)


# %%
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="聊天记录增量同步")
    parser.add_argument("--status", action="store_true", help="显示同步状态")
    parser.add_argument("--host", default="tc", help="目标主机 (默认 tc)")
    parser.add_argument("--account", default="白晔峰", help="微信账号 (默认 白晔峰)")
    parser.add_argument("--full", action="store_true", help="全量重拉（重置游标）")
    args = parser.parse_args()

    if args.status:
        cmd_status()
    elif args.full:
        cmd_full(args.host, args.account)
    else:
        cmd_sync(args.host, args.account)
