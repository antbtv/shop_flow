-- quantity follows Postgres INT (signed): a negative value must reach ClickHouse so that
-- FR-9 checks can see it, not overflow or be quarantined (ADR-0008). Idempotent: a no-op
-- once the column is already Int32.
ALTER TABLE shopflow.stg_order_items MODIFY COLUMN quantity Int32;
