-- table: stg_order_items
-- hint: same key and LSN with different content: a bug in the Spark transform
-- hint: or two sources writing the table
-- min != max of a row hash per (key, version): two numbers per group instead of a set
-- (uniqExact went past the 512 MiB airflow_reader limit at 3M lines, bench 4.17).
WITH conflicts AS (
    SELECT order_item_id
    FROM shopflow.stg_order_items
    GROUP BY order_item_id, version
    HAVING
        min(cityHash64(order_id, product_id, quantity, price_at_order, is_deleted))
        != max(cityHash64(order_id, product_id, quantity, price_at_order, is_deleted))
)

SELECT
    count() AS violations,
    groupArray(10)(toString(order_item_id)) AS sample_keys -- noqa: LT01
FROM conflicts
