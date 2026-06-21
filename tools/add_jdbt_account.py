"""
添加京东白条账户到数据库。
"""
import sqlite3, sys
sys.path.insert(0, "/data/codebase/happyjoplin")
conn = sqlite3.connect("/data/codebase/happyjoplin/data/ledger.db")
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA foreign_keys=ON")

# 检查是否已存在
r = conn.execute("SELECT id FROM accounts WHERE name='京东白条'").fetchone()
if r:
    print(f"京东白条已存在: id={r['id']}")
else:
    conn.execute("INSERT INTO accounts (name, type, institution, is_active, currency) VALUES (?, ?, ?, 1, 'CNY')",
                 ("京东白条", "loan", "京东白条"))
    print(f"京东白条已添加")

r = conn.execute("SELECT id FROM accounts WHERE name='京东白条'").fetchone()
print(f"京东白条 id: {r['id']}")

# 列出所有贷款账户
rows = conn.execute("SELECT id, name, type, institution FROM accounts WHERE type='loan' ORDER BY id").fetchall()
print("\n所有贷款账户:")
for r in rows:
    print(f"  #{r['id']} {r['name']} | inst={r['institution']}")

conn.commit()
conn.close()
