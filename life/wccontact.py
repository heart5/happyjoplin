# -*- coding: utf-8 -*-
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
# # 微信联系人追踪

# %%
"""微信联系人变更追踪与展示，配合webchat_commands中「连更」「连显」命令使用"""

# %%
import pathmagic

with pathmagic.context():
    from func.filedatafunc import getdbname
    from func.getid import gethostuser
    from func.logme import log

import sqlite3
from datetime import datetime

import pandas as pd

# %% [markdown]
# ## updatectdf() — 拉取通讯录并对比变更


# %%
def updatectdf():
    """拉取微信通讯录，与本地SQLite对比，记录变更。

    无参数。owner自动从gethostuser()获取。
    返回变更摘要dict: {new, changed, removed, total}
    """
    import itchat

    owner = gethostuser()
    dbpath = str(getdbname("data/db", owner, "wccontact"))

    itchat.auto_login(hotReload=True, enableCmdQR=False)
    friends = itchat.get_friends(update=True)
    log.info(f"wccontact: 拉取到 {len(friends)} 个好友")

    conn = sqlite3.connect(dbpath)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wc_contact (
            UserName TEXT PRIMARY KEY,
            NickName TEXT,
            RemarkName TEXT,
            Sex INTEGER,
            Province TEXT,
            City TEXT,
            Signature TEXT,
            ContactType TEXT,
            first_seen TEXT,
            last_seen TEXT,
            is_active INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wc_contact_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            changed_at TEXT,
            UserName TEXT,
            field TEXT,
            old_value TEXT,
            new_value TEXT
        )
    """)

    # 读取现有数据
    existing = {}
    cur = conn.execute("SELECT * FROM wc_contact")
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        existing[row[cols.index("UserName")]] = dict(zip(cols, row))

    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    track_fields = [
        "NickName",
        "RemarkName",
        "Sex",
        "Province",
        "City",
        "Signature",
        "ContactType",
    ]

    new_count = 0
    change_count = 0
    current_usernames = set()

    for fr in friends:
        username = fr.get("UserName", "")
        if not username:
            continue
        current_usernames.add(username)

        if username not in existing:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO wc_contact
                    (UserName, NickName, RemarkName, Sex, Province, City,
                     Signature, ContactType, first_seen, last_seen, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    username,
                    fr.get("NickName", ""),
                    fr.get("RemarkName", ""),
                    fr.get("Sex", 0),
                    fr.get("Province", ""),
                    fr.get("City", ""),
                    fr.get("Signature", ""),
                    fr.get("ContactType", ""),
                    today,
                    today,
                ),
            )
            if cur.rowcount > 0:
                new_count += 1
        else:
            for field in track_fields:
                old_val = str(existing[username].get(field, "") or "")
                new_val = str(fr.get(field, "") or "")
                if old_val != new_val:
                    conn.execute(
                        """
                        INSERT INTO wc_contact_log
                            (changed_at, UserName, field, old_value, new_value)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (now, username, field, old_val, new_val),
                    )
                    change_count += 1

            conn.execute(
                """
                UPDATE wc_contact SET
                    NickName=?, RemarkName=?, Sex=?, Province=?, City=?,
                    Signature=?, ContactType=?, last_seen=?, is_active=1
                WHERE UserName=?
                """,
                (
                    fr.get("NickName", ""),
                    fr.get("RemarkName", ""),
                    fr.get("Sex", 0),
                    fr.get("Province", ""),
                    fr.get("City", ""),
                    fr.get("Signature", ""),
                    fr.get("ContactType", ""),
                    today,
                    username,
                ),
            )

    # 标记已删除的联系人
    removed_count = 0
    for username in existing:
        if username not in current_usernames and existing[username].get("is_active"):
            conn.execute("UPDATE wc_contact SET is_active=0 WHERE UserName=?", (username,))
            conn.execute(
                """
                INSERT INTO wc_contact_log
                    (changed_at, UserName, field, old_value, new_value)
                VALUES (?, ?, 'is_active', '1', '0')
                """,
                (now, username),
            )
            removed_count += 1

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM wc_contact WHERE is_active=1").fetchone()[0]
    conn.close()

    summary = {
        "new": new_count,
        "changed": change_count,
        "removed": removed_count,
        "total": total,
    }
    log.info(f"wccontact: 更新完成 — 新增{new_count} 变更{change_count} 删除{removed_count} 当前{total}")
    return summary


# %% [markdown]
# ## getctdf() — 读取活跃联系人


# %%
def getctdf():
    """从SQLite读取活跃联系人，返回DataFrame。"""
    owner = gethostuser()
    dbpath = str(getdbname("data/db", owner, "wccontact"))

    conn = sqlite3.connect(dbpath)
    df = pd.read_sql("SELECT * FROM wc_contact WHERE is_active=1", conn)
    conn.close()
    return df


# %% [markdown]
# ## showwcsimply() — 简化展示


# %%
def showwcsimply(df):
    """简化DataFrame列用于db2img图片展示。

    筛选关键列，中文列名映射，性别转文字。
    """
    sex_map = {0: "未知", 1: "男", 2: "女"}
    col_map = {
        "RemarkName": "备注",
        "NickName": "昵称",
        "Sex": "性别",
        "Province": "省份",
        "City": "城市",
        "Signature": "签名",
    }

    cols = [c for c in col_map if c in df.columns]
    sdf = df[cols].copy()

    if "Sex" in sdf.columns:
        sdf["Sex"] = sdf["Sex"].map(sex_map).fillna("未知")

    sdf.rename(columns={k: v for k, v in col_map.items() if k in cols}, inplace=True)
    return sdf


# %% [markdown]
# ## 主函数

# %%
if __name__ == "__main__":
    summary = updatectdf()
    print(summary)
    df = getctdf()
    print(f"活跃联系人: {len(df)}")
    print(showwcsimply(df).head(10))
