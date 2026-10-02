-- table: stg_orders, stg_order_status_history
-- hint: the funnel and fact_orders disagree on the order status:
-- hint: compare raw_events of the order with both tables
-- The current status (stg_orders, what fact_orders shows) must equal the last status in the
-- history by version (LSN), M3 dq-tester. Only orders settled for the lag window: the history
-- row and the order row are written by the same batch, but a replay may land between them.
WITH last_status AS (
    SELECT
        order_id,
        argMax(status, version) AS status
    FROM shopflow.stg_order_status_history FINAL
    GROUP BY order_id
)

SELECT
    count() AS violations,
    groupArray(10)(toString(o.order_id)) AS sample_keys -- noqa: LT01
FROM shopflow.stg_orders AS o FINAL
INNER JOIN last_status AS h ON o.order_id = h.order_id
WHERE
    o.is_deleted = 0
    AND o.event_time < now64(3) - INTERVAL 15 MINUTE
    AND o.status != h.status
