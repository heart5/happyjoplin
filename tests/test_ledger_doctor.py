from life.ledger.db import Database
from life.ledger.doctor import collect, render


def _db(tmp_path):
    db = Database(str(tmp_path / "doctor.db"))
    db.execute("INSERT INTO accounts (book,name,type,opening_date,"
               "opening_balance,identity_key) VALUES"
               " ('personal','测试储蓄卡','bank','2026-01-01',1000,'doc-bank')")
    db.execute("INSERT INTO accounts (book,name,type,identity_key) VALUES"
               " ('work','测试钱包','payment','doc-wallet')")
    bank = db.fetchone(
        "SELECT id FROM accounts WHERE identity_key='doc-bank'")["id"]
    wallet = db.fetchone(
        "SELECT id FROM accounts WHERE identity_key='doc-wallet'")["id"]
    rows = [
        ("personal", "2026-10-02", 12.5, bank, "outflow", "consumption",
         None, "美团", "sms", "k1"),
        ("personal", "2026-10-02", 12.5, bank, "outflow", "consumption",
         None, "微信转账", "wechat_main", "k2"),
        ("work", "2026-10-03", 5.0, wallet, "outflow", "consumption",
         None, None, "wechat_work", "k3"),
    ]
    for book, date, amount, acct, direction, tx_type, cat, merchant, src, key in rows:
        db.execute(
            "INSERT INTO account_flows (book,tx_date,amount,account_id,"
            "direction,tx_type,category_id,merchant,source,dedup_key)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (book, date, amount, acct, direction, tx_type, cat, merchant, src, key))
    db.execute(
        "INSERT INTO account_balances (account_id,year,month,opening_balance,"
        "total_inflow,total_outflow,closing_balance)"
        " VALUES (?,2026,10,1000,0,-33,-33)", (bank,))
    db.execute(
        "INSERT INTO import_batches (book,source,period,md5,status,row_count)"
        " VALUES ('work','wechat_work','2026-10','x','failed',0)")
    return db


def test_collect_counts(tmp_path):
    stats = collect(_db(tmp_path), ("personal", "work"), ["2026-09", "2026-10"])
    assert stats["flow_count"] == 3
    assert stats["cons_n"] == 3
    assert stats["uncat_n"] == 3
    assert stats["empty_merchant_n"] == 1
    assert len(stats["transfer_suspects"]) == 1
    assert len(stats["duplicates"]) == 1
    assert stats["duplicates"][0]["cross_source"] is True
    assert len(stats["negative_balances"]) == 1
    assert [b["source"] for b in stats["bad_batches"]] == ["wechat_work"]


def test_render_contains_sections(tmp_path):
    body = render(collect(_db(tmp_path), ("personal", "work"), ["2026-10"]))
    assert body.startswith("# 财务数据体检")
    for section in ("## 一、分类与商户质量", "## 三、疑似口径错误",
                    "## 四、疑似重复", "## 五、账户与余额",
                    "## 六、导入批次异常"):
        assert section in body
