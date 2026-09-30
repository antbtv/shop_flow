-- Version journal of customers for SCD2 (FR-3, ADR-0009): one row per change of a Postgres row.
-- Key (customer_id, valid_from = source.ts_us) collapses several UPDATEs of one transaction and
-- Debezium replays to the highest LSN. dim_customers is rebuilt from it by a refreshable MV,
-- ordered by version (LSN), not by valid_from: the laptop clock may step back.
CREATE TABLE IF NOT EXISTS shopflow.stg_customer_versions
(
    customer_id UInt64,
    valid_from  DateTime64(6, 'UTC'),
    name        String,
    address     String,
    segment     LowCardinality(String),
    -- op = r: a snapshot row; the first version of a key from it opens at 1970-01-01
    is_snapshot UInt8,
    -- op = d: closes the previous version, is not a version itself
    is_deleted  UInt8,
    -- source.ts_ms, lag window for FR-8/FR-9 and the mart watermark
    event_time  DateTime64(3, 'UTC'),
    version     UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (customer_id, valid_from);
