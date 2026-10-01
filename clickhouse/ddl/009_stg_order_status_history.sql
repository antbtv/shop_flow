-- Order status transitions for the funnel (FR-5, ADR-0009): op = c/r and op = u with a changed
-- status. version (LSN) is part of the key: a repeated snapshot or a repeated status adds a row
-- instead of overwriting the real transition; the mart takes min(changed_at) per status.
CREATE TABLE IF NOT EXISTS shopflow.stg_order_status_history
(
    order_id    UInt64,
    status      LowCardinality(String),
    -- source.ts_ms of the change
    changed_at  DateTime64(3, 'UTC'),
    is_snapshot UInt8,
    version     UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (order_id, status, version);
