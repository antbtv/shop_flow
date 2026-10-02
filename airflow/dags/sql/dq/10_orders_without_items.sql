-- table: stg_orders
-- hint: an order with no lines: look for its items in raw_events (quarantine)
-- hint: and reload with spark-jobs/backfill_from_raw.py
-- The reverse orphan (M3 dq-tester). Orders before 2026-09-29 18:15 UTC were created before
-- the generator wrote order_items (3.3): 10462 such orders exist in Postgres too, they are not
-- an error. The same cut is FACT_PRICE_SINCE in scripts/check_pipeline.sh. Lag window 15 min.
WITH ordered AS (
    SELECT DISTINCT order_id
    FROM shopflow.stg_order_items FINAL
    WHERE is_deleted = 0
)

SELECT
    count() AS violations,
    groupArray(10)(toString(o.order_id)) AS sample_keys -- noqa: LT01
FROM shopflow.stg_orders AS o FINAL
WHERE
    o.is_deleted = 0
    AND o.created_at >= toDateTime64('2026-09-29 18:15:00', 6, 'UTC')
    AND o.event_time < now64(3) - INTERVAL 15 MINUTE
    AND o.order_id NOT IN (SELECT ordered.order_id FROM ordered)
