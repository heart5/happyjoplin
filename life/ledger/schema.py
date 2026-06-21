# 个人财务系统 — 数据库 Schema

SCHEMA_SQL = """

-- 账户表
CREATE TABLE IF NOT EXISTS accounts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    type        TEXT    NOT NULL CHECK(type IN (
                    'bank_debit', 'bank_credit', 'wechat_wallet',
                    'alipay', 'loan', 'cash'
                )),
    bank        TEXT,
    card_suffix TEXT,
    institution TEXT,
    currency    TEXT    DEFAULT 'CNY',
    is_active   INTEGER DEFAULT 1,
    notes       TEXT,
    created_at  TEXT    DEFAULT (datetime('now','localtime')),
    updated_at  TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_bank_card
    ON accounts(bank, card_suffix)
    WHERE bank IS NOT NULL AND card_suffix IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_loan
    ON accounts(institution)
    WHERE type = 'loan';

CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_type_unique
    ON accounts(type)
    WHERE type IN ('wechat_wallet', 'alipay');

-- 分类表
CREATE TABLE IF NOT EXISTS categories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    parent_id   INTEGER REFERENCES categories(id),
    direction   TEXT    NOT NULL DEFAULT 'expense'
                CHECK(direction IN ('expense','income','transfer')),
    is_loan     INTEGER DEFAULT 0,
    sort_order  INTEGER DEFAULT 0
);

-- 核心流水表
CREATE TABLE IF NOT EXISTS account_flows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_date         TEXT    NOT NULL,
    tx_time         TEXT,
    amount          REAL    NOT NULL CHECK(amount > 0),
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    direction       TEXT    NOT NULL CHECK(direction IN ('inflow','outflow')),
    linked_flow_id  INTEGER REFERENCES account_flows(id),
    tx_type         TEXT    NOT NULL DEFAULT 'expense'
                    CHECK(tx_type IN (
                        'expense', 'income', 'transfer',
                        'loan_disbursement', 'loan_repayment'
                    )),
    category_id     INTEGER REFERENCES categories(id),
    merchant        TEXT,
    description     TEXT,
    counterparty    TEXT,
    source          TEXT    DEFAULT 'manual'
                    CHECK(source IN ('wechat','sms','manual','alipay')),
    source_group_id TEXT,
    raw_data        TEXT,
    is_reconciled   INTEGER DEFAULT 0,
    notes           TEXT,
    created_at      TEXT    DEFAULT (datetime('now','localtime')),
    updated_at      TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_flows_date       ON account_flows(tx_date);
CREATE INDEX IF NOT EXISTS idx_flows_account    ON account_flows(account_id);
CREATE INDEX IF NOT EXISTS idx_flows_category   ON account_flows(category_id);
CREATE INDEX IF NOT EXISTS idx_flows_source_grp ON account_flows(source_group_id);

-- 月度余额快照
CREATE TABLE IF NOT EXISTS account_balances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    opening_balance REAL    NOT NULL DEFAULT 0,
    closing_balance REAL    NOT NULL DEFAULT 0,
    total_inflow    REAL    DEFAULT 0,
    total_outflow   REAL    DEFAULT 0,
    is_estimated    INTEGER DEFAULT 1,
    notes           TEXT,
    updated_at      TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(account_id, year, month)
);

-- 净资产快照
CREATE TABLE IF NOT EXISTS net_worth_snapshots (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date     TEXT    NOT NULL UNIQUE,
    total_assets      REAL    NOT NULL,
    total_liabilities REAL    NOT NULL,
    net_worth         REAL    NOT NULL,
    details           TEXT,
    created_at        TEXT    DEFAULT (datetime('now','localtime'))
);

-- 商户分类映射
CREATE TABLE IF NOT EXISTS merchant_category_map (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    merchant    TEXT    NOT NULL UNIQUE,
    category_id INTEGER NOT NULL REFERENCES categories(id),
    priority    INTEGER DEFAULT 0,
    created_at  TEXT    DEFAULT (datetime('now','localtime'))
);

-- 调整日志
CREATE TABLE IF NOT EXISTS adjustment_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_id         INTEGER REFERENCES account_flows(id),
    field           TEXT    NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    reason          TEXT,
    adjusted_at     TEXT    DEFAULT (datetime('now','localtime'))
);

"""

SEED_CATEGORIES = [
    # 支出分类
    ("餐饮-外卖", "expense"), ("餐饮-正餐", "expense"), ("餐饮-饮品", "expense"),
    ("交通-网约车", "expense"), ("交通-公共交通", "expense"), ("交通-加油", "expense"), ("交通-充电", "expense"),
    ("购物-电商", "expense"), ("购物-超市", "expense"), ("购物-便利店", "expense"), ("购物-其他", "expense"),
    ("居住-房租", "expense"), ("居住-物业", "expense"), ("居住-水电", "expense"),
    ("医疗-药品", "expense"), ("医疗-就诊", "expense"),
    ("社交-红包", "expense"), ("社交-转账", "expense"),
    ("娱乐-其他", "expense"),
    # 收入分类
    ("收入-工资", "income"), ("收入-报销", "income"), ("收入-退款", "income"), ("收入-理财", "income"), ("收入-其他", "income"),
    # 贷款分类
    ("借贷-放款", "income"), ("借贷-还款", "expense"), ("借贷-其他", "expense"),
    # 内部往来
    ("内部-转账", "transfer"),
]

SEED_ACCOUNTS = [
    ("微信零钱", "wechat_wallet", None, None, None),
    ("支付宝", "alipay", None, None, None),
]

# 预置贷款账户（按机构名匹配，与 sms_finance.py 平台列表对应）
SEED_LOAN_ACCOUNTS = [
    ("洋钱罐", "洋钱罐"),
    ("借呗", "借呗"),
    ("微粒贷", "微粒贷"),
    ("小赢卡贷", "小赢卡贷"),
    ("美团借钱", "美团借钱"),
    ("京东金融", "京东金融"),
    ("中邮钱包", "中邮消金"),
]


def init_db(conn):
    """初始化数据库：建表 + 种子数据。"""
    conn.executescript(SCHEMA_SQL)

    # 种子分类
    existing = {r["name"] for r in conn.execute("SELECT name FROM categories").fetchall()}
    for name, direction in SEED_CATEGORIES:
        if name not in existing:
            conn.execute(
                "INSERT INTO categories (name, direction, is_loan) VALUES (?, ?, ?)",
                (name, direction, 1 if name.startswith("借贷-") else 0),
            )

    # 种子账户（特殊类型：微信零钱/支付宝）
    for row in conn.execute("SELECT type FROM accounts WHERE type IN ('wechat_wallet','alipay')").fetchall():
        existing.add(f"acct:{row['type']}")
    for name, acct_type, bank, suffix, institution in SEED_ACCOUNTS:
        key = f"acct:{acct_type}"
        if key not in existing:
            conn.execute(
                "INSERT INTO accounts (name, type, bank, card_suffix, institution) VALUES (?, ?, ?, ?, ?)",
                (name, acct_type, bank, suffix, institution),
            )
            existing.add(key)

    # 种子贷款账户（按 institution 去重）
    for name, institution in SEED_LOAN_ACCOUNTS:
        row = conn.execute(
            "SELECT id FROM accounts WHERE type='loan' AND institution=?",
            (institution,),
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT INTO accounts (name, type, institution, notes) VALUES (?, 'loan', ?, '预置账户')",
                (name, institution),
            )

    conn.commit()
