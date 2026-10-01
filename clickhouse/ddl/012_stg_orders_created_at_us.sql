-- ADR-0009: created_at in microseconds, so ASOF JOIN to dim_products does not pick the old
-- price for an order started in the same millisecond as a price change commit. Rows written
-- before keep millisecond precision. Idempotent: a no-op once the column is DateTime64(6).
ALTER TABLE shopflow.stg_orders MODIFY COLUMN created_at DateTime64(6, 'UTC');
