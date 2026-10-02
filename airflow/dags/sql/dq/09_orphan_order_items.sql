-- table: stg_order_items
-- hint: the order is missing in stg_orders: look for it in raw_events (quarantine, ADR-0008)
-- hint: and reload with spark-jobs/backfill_from_raw.py
-- FR-9 "no orphan order_items". Only lines older than the lag window (15 min, ADR-0006): the
-- orders and order_items topics arrive without a common order. Deleted orders count as present:
-- a delete is not an orphan.
WITH known_orders AS (
    SELECT DISTINCT order_id FROM shopflow.stg_orders
)

SELECT
    count() AS violations,
    groupArray(10)(toString(i.order_item_id)) AS sample_keys -- noqa: LT01
FROM shopflow.stg_order_items AS i FINAL
WHERE
    i.is_deleted = 0
    AND i.event_time < now64(3) - INTERVAL 15 MINUTE
    AND i.order_id NOT IN (SELECT known_orders.order_id FROM known_orders)
