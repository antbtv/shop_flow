-- Version journal of products for SCD2 (FR-3, ADR-0009). Same rules as stg_customer_versions.
CREATE TABLE IF NOT EXISTS shopflow.stg_product_versions
(
    product_id  UInt64,
    valid_from  DateTime64(6, 'UTC'),
    name        String,
    category    LowCardinality(String),
    price       Decimal(10, 2),
    is_snapshot UInt8,
    is_deleted  UInt8,
    event_time  DateTime64(3, 'UTC'),
    version     UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (product_id, valid_from);
