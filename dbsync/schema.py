"""目标库 schema 定义。

目标 SQLite 里同时存放：
  - 业务表（orders / order_items，带 version 列）
  - tombstones   删除墓碑：记住已删主键的版本，防止旧插入事件复活数据
  - sync_meta    消费进度（applied_offset 等），与业务数据同一事务提交
  - conflicts    冲突记录（同主键同版本不同内容）
"""

# 业务表 -> 业务列（不含 version，第一列必须是主键 id）
BUSINESS_TABLES = {
    "orders": ["id", "customer", "amount", "status"],
    "order_items": ["id", "order_id", "sku", "qty", "price"],
}

DDL = """
CREATE TABLE IF NOT EXISTS orders (
    id       TEXT PRIMARY KEY,
    customer TEXT,
    amount   REAL,
    status   TEXT,
    version  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id       TEXT PRIMARY KEY,
    order_id TEXT,
    sku      TEXT,
    qty      INTEGER,
    price    REAL,
    version  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tombstones (
    table_name TEXT NOT NULL,
    pk         TEXT NOT NULL,
    version    INTEGER NOT NULL,
    txid       TEXT,
    deleted_at TEXT NOT NULL,
    PRIMARY KEY (table_name, pk)
);

CREATE TABLE IF NOT EXISTS sync_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS conflicts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    txid        TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    pk          TEXT NOT NULL,
    version     INTEGER NOT NULL,
    existing    TEXT,
    incoming    TEXT,
    detected_at TEXT NOT NULL,
    UNIQUE (txid, table_name, pk, version)
);
"""


def ensure_schema(con):
    """建表（幂等），并初始化消费进度元数据。"""
    con.executescript(DDL)
    con.execute("INSERT OR IGNORE INTO sync_meta(key, value) VALUES ('applied_offset', '0')")
    con.execute("INSERT OR IGNORE INTO sync_meta(key, value) VALUES ('last_txid', '')")
    con.execute("INSERT OR IGNORE INTO sync_meta(key, value) VALUES ('applied_at', '')")
    con.commit()
