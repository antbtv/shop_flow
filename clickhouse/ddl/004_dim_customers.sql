-- SCD2 history of customers (FR-3). Closing a version rewrites the row (customer_id, valid_from)
-- with a larger version (source.lsn); the handler rules are in ADR-0006, filled in M3.
CREATE TABLE IF NOT EXISTS shopflow.dim_customers
(
    customer_id UInt64,
    name        String,
    address     String,
    segment     LowCardinality(String),
    -- source.ts_us of the change that opened the version
    valid_from  DateTime64(6, 'UTC'),
    valid_to    Nullable(DateTime64(6, 'UTC')),
    is_current  UInt8,
    version     UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (customer_id, valid_from);
