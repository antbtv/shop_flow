-- table: stg_orders
-- hint: same key and LSN with different content: a bug in the Spark transform
-- hint: or two sources writing the table
-- Replays of one event (same key and version) are normal and collapse in ReplacingMergeTree;
-- rows that share key and version but differ in content would collapse arbitrarily (ADR-0009).
WITH conflicts AS (
    SELECT order_id
    FROM shopflow.stg_orders
    GROUP BY order_id, version
    HAVING uniqExact(customer_id, status, created_at, updated_at, is_deleted) > 1
)

SELECT
    count() AS violations,
    groupArray(10)(toString(order_id)) AS sample_keys -- noqa: LT01
FROM conflicts
