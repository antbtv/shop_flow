-- SCD2 history of product prices (FR-3). Same rules as dim_customers (ADR-0006).
CREATE TABLE IF NOT EXISTS shopflow.dim_products
(
    product_id UInt64,
    name       String,
    category   LowCardinality(String),
    price      Decimal(10, 2),
    valid_from DateTime64(6, 'UTC'),
    valid_to   Nullable(DateTime64(6, 'UTC')),
    is_current UInt8,
    version    UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (product_id, valid_from);
