from life.ledger.db import Database
from life.ledger.reports.charts import build_monthly_charts
from life.ledger.reports.monthly import generate_monthly_report


def _db(tmp_path):
    db = Database(str(tmp_path / "charts.db"))

    def cat(name, direction):
        row = db.fetchone("SELECT id FROM categories WHERE name=?", (name,))
        if row:
            return row["id"]
        return db.insert("categories", {"name": name, "direction": direction})

    food = cat("餐饮-正餐", "expense")
    taxi = cat("交通-打车", "expense")
    salary = cat("收入-工资", "income")
    refund = cat("收入-退款", "income")
    db.execute(
        "INSERT INTO accounts (book,name,type,opening_date,opening_balance,"
        "identity_key) VALUES ('personal','测试储蓄卡','bank','2026-01-01',"
        "10000,'charts-bank')")
    bank = db.fetchone("SELECT id FROM accounts WHERE identity_key='charts-bank'")["id"]
    rows = [
        ("2026-09-05", 100.0, "outflow", "consumption", food, "美团"),
        ("2026-10-01", 80.0, "outflow", "consumption", food, "美团"),
        ("2026-10-03", 30.0, "outflow", "consumption", taxi, "滴滴出行"),
        ("2026-10-10", 9000.0, "inflow", "income", salary, ""),
        ("2026-10-12", 25.0, "inflow", "income", refund, "退款"),
    ]
    for i, (tx_date, amount, direction, tx_type, cat, merchant) in enumerate(rows):
        db.execute(
            "INSERT INTO account_flows (book,tx_date,amount,account_id,direction,"
            "tx_type,category_id,merchant,source,dedup_key)"
            " VALUES ('personal',?,?,?,?,?,?,?,'sms',?)",
            (tx_date, amount, bank, direction, tx_type, cat, merchant, f"k{i}"))
    return db, tmp_path


def test_report_has_chart_placeholders_and_comparisons(tmp_path):
    db, _ = _db(tmp_path)
    body = generate_monthly_report(db, 2026, 10, book="personal")
    for label in ("cat_donut", "merchant_bar", "daily_spend", "month_trend"):
        assert "{{chart:" + label + "}}" in body
    assert "消费环比" in body
    assert "消费同比" in body


def test_charts_written(tmp_path):
    db, base = _db(tmp_path)
    charts = build_monthly_charts(db, "personal", 2026, 10, base / "out")
    for label in ("cat_donut", "merchant_bar", "daily_spend", "month_trend"):
        assert label in charts
        data = open(charts[label], "rb").read()
        assert data.startswith(b"\x89PNG")
