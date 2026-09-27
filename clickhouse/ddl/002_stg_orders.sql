-- Latest state of each Postgres orders row. version = source.lsn of the change:
-- monotonic for one row, never compared with other tables (ADR-0006).
CREATE TABLE IF NOT EXISTS shopflow.stg_orders
(
    order_id    UInt64,
    customer_id UInt64,
    status      LowCardinality(String),
    created_at  DateTime64(3, 'UTC'),
    updated_at  DateTime64(3, 'UTC'),
    version     UInt64,
    is_deleted  UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY order_id;
