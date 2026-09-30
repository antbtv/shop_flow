-- ADR-0009: event_time (source.ts_ms) for the FR-9 orphan check window. Idempotent.
ALTER TABLE shopflow.stg_order_items
ADD COLUMN IF NOT EXISTS event_time DateTime64(3, 'UTC')
DEFAULT toDateTime64(0, 3, 'UTC') AFTER price_at_order;
