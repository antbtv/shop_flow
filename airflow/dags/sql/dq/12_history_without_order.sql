-- table: stg_order_status_history
-- hint: the order is missing in stg_orders while its transition is kept: quarantine is per
-- hint: table (ADR-0008); look in raw_events, reload with spark-jobs/backfill_from_raw.py
-- Found in 4.9: an order quarantined for stg_orders (NULL customer_id) still writes its status
-- history. The funnel starts from stg_orders and does not count it, but the gap must be seen.
WITH history AS (
    SELECT
        order_id,
        max(changed_at) AS last_change
    FROM shopflow.stg_order_status_history FINAL
    GROUP BY order_id
),

known_orders AS (
    SELECT DISTINCT order_id FROM shopflow.stg_orders
)

SELECT
    count() AS violations,
    groupArray(10)(toString(history.order_id)) AS sample_keys -- noqa: LT01
FROM history
WHERE
    history.last_change < now64(3) - INTERVAL 15 MINUTE
    AND history.order_id NOT IN (SELECT known_orders.order_id FROM known_orders)
