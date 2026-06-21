"""
重新分类已有流水：基于 merchant 匹配 category_map 更新 database。
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pathmagic
with pathmagic.context():
    from life.ledger.db import Database
    from life.wechat_finance import classify_merchant, load_category_map

def ensure_category(db, name, direction="expense", is_loan=0):
    row = db.fetchone("SELECT id FROM categories WHERE name=?", (name,))
    if row:
        return row["id"]
    return db.insert("categories", {
        "name": name, "direction": direction, "is_loan": is_loan,
    })

def main():
    db = Database()
    cat_map = load_category_map()

    # 确保所有 map 中引用的分类在 DB 中存在
    for cat_name in set(cat_map.values()):
        direction = "expense"
        if cat_name.startswith("收入-"):
            direction = "income"
        elif cat_name.startswith("内部-"):
            direction = "transfer"
        elif cat_name.startswith("借贷-"):
            direction = "expense"
        is_loan = 1 if cat_name.startswith("借贷-") else 0
        ensure_category(db, cat_name, direction, is_loan)

    # 获取所有分类名→ID 映射
    rows = db.fetchall("SELECT id, name FROM categories")
    cat_name_to_id = {r["name"]: r["id"] for r in rows}

    # 重新分类所有未分类且有商户名的记录
    rows = db.fetchall(
        "SELECT f.id, f.merchant FROM account_flows f "
        "JOIN categories c ON f.category_id=c.id "
        "WHERE c.name='未分类-其他' AND f.merchant != '' AND f.merchant IS NOT NULL"
    )
    print(f"待重新分类: {len(rows)} 条")

    updated = 0
    for r in rows:
        new_cat_name = classify_merchant(r["merchant"], cat_map)
        if new_cat_name and new_cat_name != "未分类-其他":
            new_cat_id = cat_name_to_id.get(new_cat_name)
            if new_cat_id:
                db.execute("UPDATE account_flows SET category_id=? WHERE id=?", (new_cat_id, r["id"]))
                updated += 1

    # 提交事务
    db.conn.commit()
    print(f"已更新: {updated} 条")

    # 验证
    r = db.fetchone("SELECT COUNT(*) as cnt FROM account_flows f JOIN categories c ON f.category_id=c.id WHERE c.name='未分类-其他'")
    r1 = db.fetchone("SELECT COUNT(*) as c FROM account_flows f JOIN categories c ON f.category_id=c.id WHERE c.name='未分类-其他' AND (f.merchant IS NULL OR f.merchant = '')")
    r2 = db.fetchone("SELECT COUNT(*) as c FROM account_flows f JOIN categories c ON f.category_id=c.id WHERE c.name='未分类-其他' AND f.merchant != ''")
    print(f"剩余未分类: {r['cnt']} 条 (空商户: {r1['c']}, 有商户: {r2['c']})")

if __name__ == "__main__":
    main()
