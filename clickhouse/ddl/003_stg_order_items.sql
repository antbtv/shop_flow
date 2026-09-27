-- Latest state of each Postgres order_items row. version = source.lsn (ADR-0006).
-- fact_orders is built on top of stg_orders and stg_order_items in M3.
CREATE TABLE IF NOT EXISTS shopflow.stg_order_items
(
    order_item_id  UInt64,
    order_id       UInt64,
    product_id     UInt64,
    quantity       UInt32,
    price_at_order Decimal(10, 2),
    version        UInt64,
    is_deleted     UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY order_item_id;
