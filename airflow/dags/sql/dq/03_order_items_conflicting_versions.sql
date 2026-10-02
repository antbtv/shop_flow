-- table: stg_order_items
-- hint: same key and LSN with different content: a bug in the Spark transform
-- hint: or two sources writing the table
WITH conflicts AS (
    SELECT order_item_id
    FROM shopflow.stg_order_items
    GROUP BY order_item_id, version
    HAVING uniqExact(order_id, product_id, quantity, price_at_order, is_deleted) > 1
)

SELECT
    count() AS violations,
    groupArray(10)(toString(order_item_id)) AS sample_keys -- noqa: LT01
FROM conflicts
