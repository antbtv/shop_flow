-- Latest state of each Postgres inventory row, like stg_orders (ADR-0006, ADR-0009).
CREATE TABLE IF NOT EXISTS shopflow.stg_inventory
(
    product_id   UInt64,
    warehouse_id UInt32,
    quantity     Int32,
    updated_at   DateTime64(3, 'UTC'),
    event_time   DateTime64(3, 'UTC'),
    version      UInt64,
    is_deleted   UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (product_id, warehouse_id);
