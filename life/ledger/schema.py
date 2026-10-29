# 个人财务系统 — 数据库 Schema（finance.db，复式账本）

SCHEMA_VERSION = 3

ACCOUNT_FLOWS_SQL = """
CREATE TABLE IF NOT EXISTS account_flows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    book            TEXT    NOT NULL CHECK(book IN ('personal','work')),
    tx_date         TEXT    NOT NULL,
    tx_time         TEXT,
    amount          REAL    NOT NULL CHECK(amount > 0),
    account_id      INTEGER REFERENCES accounts(id),
    direction       TEXT    NOT NULL CHECK(direction IN ('inflow','outflow')),
    tx_type         TEXT    NOT NULL DEFAULT 'consumption'
                    CHECK(tx_type IN (
                        'consumption', 'income', 'transfer',
                        'loan_disbursement', 'loan_repayment',
                        'credit_card_repayment', 'refund', 'fee', 'adjustment'
                    )),
    category_id     INTEGER REFERENCES categories(id),
    merchant        TEXT,
    description     TEXT,
    counterparty    TEXT,
    source          TEXT    NOT NULL
                    CHECK(source IN ('sms','wechat_main','wechat_work','notification','manual')),
    source_event_id TEXT,
    dedup_key       TEXT    NOT NULL UNIQUE,
    source_group_id TEXT,
    pair_key        TEXT,
    linked_flow_id  INTEGER REFERENCES account_flows(id),
    raw_data        TEXT,
    is_reconciled   INTEGER DEFAULT 0,
    notes           TEXT,
    created_at      TEXT    DEFAULT (datetime('now','localtime')),
    updated_at      TEXT    DEFAULT (datetime('now','localtime'))
);
"""

FLOW_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_flows_book_date ON account_flows(book, tx_date);
CREATE INDEX IF NOT EXISTS idx_flows_account   ON account_flows(account_id);
CREATE INDEX IF NOT EXISTS idx_flows_category  ON account_flows(category_id);
CREATE INDEX IF NOT EXISTS idx_flows_source    ON account_flows(source, tx_date);
CREATE INDEX IF NOT EXISTS idx_flows_pair      ON account_flows(pair_key);
"""

_ACCOUNT_FLOWS_NEW_SQL = ACCOUNT_FLOWS_SQL.replace(
    "CREATE TABLE IF NOT EXISTS account_flows",
    "CREATE TABLE account_flows_new",
)

_ACCOUNT_FLOWS_COLUMNS = (
    "id", "book", "tx_date", "tx_time", "amount", "account_id", "direction",
    "tx_type", "category_id", "merchant", "description", "counterparty",
    "source", "source_event_id", "dedup_key", "source_group_id", "pair_key",
    "linked_flow_id", "raw_data", "is_reconciled", "notes",
    "created_at", "updated_at",
)

SCHEMA_SQL = """

-- 账户表（book 分离个人/工作号；identity_key 保证同一账套内账户唯一）
CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    book            TEXT    NOT NULL CHECK(book IN ('personal','work')),
    name            TEXT    NOT NULL,
    type            TEXT    NOT NULL CHECK(type IN (
                        'bank', 'payment', 'credit_card',
                        'loan', 'investment', 'cash'
                    )),
    bank            TEXT,
    card_suffix     TEXT,
    institution     TEXT,
    platform        TEXT,
    currency        TEXT    DEFAULT 'CNY',
    opening_balance REAL    DEFAULT 0,
    opening_date    TEXT,
    aliases         TEXT,
    is_active       INTEGER DEFAULT 1,
    notes           TEXT,
    identity_key    TEXT    NOT NULL UNIQUE,
    created_at      TEXT    DEFAULT (datetime('now','localtime')),
    updated_at      TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_accounts_book_type ON accounts(book, type);

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

-- 核心流水表（dedup_key 全局唯一，重复导入幂等）
""" + ACCOUNT_FLOWS_SQL + FLOW_INDEX_SQL + """

-- 通知观察信号（还款提醒/剩余应还/额度；不入账，保留历史快照）
CREATE TABLE IF NOT EXISTS notification_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    book            TEXT    NOT NULL DEFAULT 'personal',
    source_event_id TEXT    NOT NULL,
    notif_id        INTEGER,
    device          TEXT,
    package         TEXT,
    bank            TEXT,
    card_suffix     TEXT,
    signal_type     TEXT    NOT NULL CHECK(signal_type IN (
                        'available_credit', 'remaining_due', 'minimum_payment',
                        'due_date', 'credit_limit', 'paid_amount'
                    )),
    amount          REAL,
    value_text      TEXT,
    content_hash    TEXT,
    observed_at     TEXT,
    raw_text        TEXT,
    created_at      TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(book, source_event_id, signal_type, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_signals_card
    ON notification_signals(book, card_suffix, signal_type, observed_at);

-- 导入批次（内容 md5 未变则整批跳过）
CREATE TABLE IF NOT EXISTS import_batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    book        TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    period      TEXT    NOT NULL,
    md5         TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'running'
                CHECK(status IN ('running','ok','failed')),
    row_count   INTEGER DEFAULT 0,
    error       TEXT,
    started_at  TEXT    DEFAULT (datetime('now','localtime')),
    finished_at TEXT,
    UNIQUE(book, source, period)
);

-- 月度余额快照（opening 含账户期初 + 历史流水）
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

-- 净资产快照（personal / work / combined）
CREATE TABLE IF NOT EXISTS net_worth_snapshots (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    book              TEXT    NOT NULL DEFAULT 'combined'
                      CHECK(book IN ('personal','work','combined')),
    snapshot_date     TEXT    NOT NULL,
    total_assets      REAL    NOT NULL,
    total_liabilities REAL    NOT NULL,
    net_worth         REAL    NOT NULL,
    details           TEXT,
    created_at        TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(book, snapshot_date)
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
    flow_id         INTEGER REFERENCES account_flows(id) ON DELETE SET NULL,
    field           TEXT    NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    reason          TEXT,
    adjusted_at     TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER NOT NULL,
    applied_at  TEXT    DEFAULT (datetime('now','localtime'))
);

"""

SEED_CATEGORIES = [
    ("餐饮-外卖", "expense"), ("餐饮-正餐", "expense"), ("餐饮-饮品", "expense"),
    ("交通-网约车", "expense"), ("交通-公共交通", "expense"), ("交通-加油", "expense"), ("交通-充电", "expense"), ("交通-其他", "expense"),
    ("购物-电商", "expense"), ("购物-超市", "expense"), ("购物-便利店", "expense"), ("购物-其他", "expense"),
    ("居住-房租", "expense"), ("居住-物业", "expense"), ("居住-水电", "expense"), ("居住-燃气", "expense"),
    ("医疗-药品", "expense"), ("医疗-就诊", "expense"),
    ("社交-红包", "expense"), ("社交-转账", "expense"),
    ("娱乐-其他", "expense"),
    ("通讯-话费", "expense"),
    ("住宿-酒店", "expense"),
    ("保险-人寿", "expense"),
    ("金融-其他", "expense"), ("金融-手续费", "expense"), ("金融-贷款", "expense"),
    ("技术-云服务", "expense"),
    ("收入-工资", "income"), ("收入-报销", "income"), ("收入-退款", "income"),
    ("收入-理财", "income"), ("收入-其他", "income"), ("收入-转账", "income"),
    ("借贷-放款", "income"), ("借贷-还款", "expense"), ("借贷-其他", "expense"),
    ("内部-转账", "transfer"),
    ("未分类-其他", "expense"),
]

# 预置账户：book, name, type, bank, card_suffix, institution, platform, is_active
SEED_ACCOUNTS = [
    ("personal", "微信零钱", "payment", None, None, None, "wechat", 1),
    ("personal", "支付宝", "payment", None, None, None, "alipay", 1),
    ("personal", "招商银行储蓄卡", "bank", "招商银行", "9929", None, None, 1),
    ("personal", "交通银行储蓄卡", "bank", "交通银行", "5631", None, None, 1),
    ("personal", "农业银行储蓄卡", "bank", "农业银行", "8574", None, None, 1),
    ("personal", "建设银行储蓄卡", "bank", "建设银行", "7939", None, None, 1),
    ("personal", "招商银行信用卡", "credit_card", "招商银行", "8256", None, None, 1),
    ("personal", "交通银行信用卡", "credit_card", "交通银行", "2349", None, None, 1),
    ("personal", "建设银行信用卡-9170", "credit_card", "建设银行", "9170", None, None, 1),
    ("personal", "建设银行信用卡-2133", "credit_card", "建设银行", "2133", None, None, 1),
    ("personal", "建设银行信用卡-2620", "credit_card", "建设银行", "2620", None, None, 1),
    ("personal", "广发信用卡-7717", "credit_card", "广发银行", "7717", None, None, 1),
    ("personal", "广发信用卡-3708", "credit_card", "广发银行", "3708", None, None, 1),
    ("personal", "光大银行信用卡-3624", "credit_card", "光大银行", "3624", None, None, 1),
    ("personal", "光大银行信用卡-2621", "credit_card", "光大银行", "2621", None, None, 1),
    ("personal", "浦发银行信用卡-3669", "credit_card", "浦发银行", "3669", None, None, 1),
    ("personal", "洋钱罐", "loan", None, None, "洋钱罐", None, 1),
    ("personal", "借呗", "loan", None, None, "借呗", None, 1),
    ("personal", "微粒贷", "loan", None, None, "微粒贷", None, 1),
    ("personal", "小赢卡贷", "loan", None, None, "小赢卡贷", None, 1),
    ("personal", "美团借钱", "loan", None, None, "美团借钱", None, 1),
    ("personal", "京东金融", "loan", None, None, "京东金融", None, 1),
    ("personal", "中邮钱包", "loan", None, None, "中邮消金", None, 1),
    ("personal", "宜享花", "loan", None, None, "宜享花", None, 1),
    ("personal", "木吉网络", "loan", None, None, "木吉网络", None, 1),
    ("personal", "花呗", "loan", None, None, "花呗", None, 1),
    ("personal", "招联金融", "loan", None, None, "招联金融", None, 1),
    ("personal", "南京银行(白条分分卡)", "loan", None, None, "南京银行", None, 1),
    ("personal", "京东白条", "loan", None, None, "京东白条", None, 1),
    ("personal", "美团月付", "loan", None, None, "美团月付", None, 1),
    ("personal", "分期乐", "loan", None, None, "分期乐", None, 1),
    ("personal", "马上消费金融", "loan", None, None, "马上消费金融", None, 1),
    ("work", "工作号微信零钱", "payment", None, None, None, "wechat", 1),
]

# 旧库机构名归一
LOAN_ALIASES = {
    "洋钱罐借款": "洋钱罐",
    "洋钱罐借钱": "洋钱罐",
    "中邮钱包": "中邮消金",
    "中邮消费金融": "中邮消金",
    "中邮消费": "中邮消金",
    "马上消费": "马上消费金融",
    "海尔消费": "海尔消费金融",
}


def make_identity_key(book, acct_type, bank=None, card_suffix=None,
                      institution=None, platform=None) -> str:
    return "|".join([
        book or "", acct_type or "", bank or "", card_suffix or "",
        institution or "", platform or "",
    ])


def _migrate_flows_source(conn):
    """v1→v2：SQLite 不能改 CHECK，只能重建 account_flows。

    老库按 sqlite_master 里的建表 SQL 判断，已迁移或新库直接跳过。
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='account_flows'"
    ).fetchone()
    if row is None:
        return
    sql = row[0] or ""
    if "'notification'" in sql:
        return

    cols = ", ".join(_ACCOUNT_FLOWS_COLUMNS)
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(_ACCOUNT_FLOWS_NEW_SQL)
        conn.execute(
            f"INSERT INTO account_flows_new ({cols})"
            f" SELECT {cols} FROM account_flows"
        )
        conn.execute("DROP TABLE account_flows")
        conn.execute("ALTER TABLE account_flows_new RENAME TO account_flows")
        for stmt in FLOW_INDEX_SQL.split(";"):
            if stmt.strip():
                conn.execute(stmt)
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(f"account_flows 迁移后外键异常：{violations[:5]}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


_ADJUSTMENT_LOG_SQL = """
CREATE TABLE adjustment_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_id         INTEGER REFERENCES account_flows(id) ON DELETE SET NULL,
    field           TEXT    NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    reason          TEXT,
    adjusted_at     TEXT    DEFAULT (datetime('now','localtime'))
)
"""


def _migrate_adjustment_log(conn):
    """调整日志不阻断流水重建：流水平台每月整月重建，删除时把 flow_id 置空。"""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='adjustment_log'"
    ).fetchone()
    if row is None or "ON DELETE SET NULL" in (row[0] or ""):
        return
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("ALTER TABLE adjustment_log RENAME TO adjustment_log_old")
        conn.execute(_ADJUSTMENT_LOG_SQL)
        conn.execute(
            "INSERT INTO adjustment_log"
            " (id, flow_id, field, old_value, new_value, reason, adjusted_at)"
            " SELECT id, flow_id, field, old_value, new_value, reason, adjusted_at"
            " FROM adjustment_log_old"
        )
        conn.execute("DROP TABLE adjustment_log_old")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def init_db(conn):
    """初始化数据库：建表 + 迁移 + 种子数据。"""
    conn.executescript(SCHEMA_SQL)
    _migrate_flows_source(conn)
    _migrate_adjustment_log(conn)

    existing = {r["name"] for r in conn.execute("SELECT name FROM categories").fetchall()}
    for name, direction in SEED_CATEGORIES:
        if name not in existing:
            conn.execute(
                "INSERT INTO categories (name, direction, is_loan) VALUES (?, ?, ?)",
                (name, direction, 1 if name.startswith("借贷-") else 0),
            )

    for row in SEED_ACCOUNTS:
        book, name, acct_type, bank, suffix, inst, platform, active = row
        key = make_identity_key(book, acct_type, bank, suffix, inst, platform)
        conn.execute(
            """INSERT INTO accounts
               (book, name, type, bank, card_suffix, institution, platform,
                is_active, identity_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(identity_key) DO NOTHING""",
            (book, name, acct_type, bank, suffix, inst, platform, active, key),
        )

    if not conn.execute("SELECT 1 FROM schema_version WHERE version=?",
                        (SCHEMA_VERSION,)).fetchone():
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    conn.commit()
