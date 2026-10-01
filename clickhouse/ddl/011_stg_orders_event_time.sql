-- ADR-0009: event_time (source.ts_ms) for the FR-8/FR-9 lag window and the mart watermark.
-- Idempotent. The explicit DEFAULT lets the running Spark job keep inserting without the
-- column until 3.7.
ALTER TABLE shopflow.stg_orders
ADD COLUMN IF NOT EXISTS event_time DateTime64(3, 'UTC')
DEFAULT toDateTime64(0, 3, 'UTC') AFTER updated_at;
