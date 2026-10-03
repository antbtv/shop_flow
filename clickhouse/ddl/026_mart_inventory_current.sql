-- Current stock per (product, warehouse) (FR-7, ADR-0011). Filled only by
-- mart_inventory_current_mv. Live rows of stg_inventory (is_deleted = 0) with the current
-- product from dim_products ('unknown' category and name without a version). is_low = 1 when
-- quantity < 20: the generator starts a shelf at 20..200 and restocks the ten emptiest ones
-- (generator/model.py), so a shelf under the minimum start stock is low. Negative stock is a
-- data quality problem (FR-9), it is shown as is and counts as low.
CREATE TABLE IF NOT EXISTS shopflow.mart_inventory_current
(
    product_id       UInt64,
    warehouse_id     UInt32,
    product_name     String,
    category         LowCardinality(String),
    quantity         Int32,
    is_low           UInt8,
    updated_at       DateTime64(3, 'UTC'),
    refreshed_at     DateTime64(3, 'UTC'),
    source_watermark DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (product_id, warehouse_id);
