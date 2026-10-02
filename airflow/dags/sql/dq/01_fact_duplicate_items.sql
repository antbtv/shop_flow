-- table: fact_orders
-- hint: a join fans out an order line: look for two current product versions
-- hint: (dim_products) or duplicate live rows after FINAL
-- FR-9 "no duplicates by order key" where analysts read: one fact row per order line.
WITH dup AS (
    SELECT order_item_id
    FROM shopflow.fact_orders
    GROUP BY order_item_id
    HAVING count() > 1
)

SELECT
    count() AS violations,
    groupArray(10)(toString(order_item_id)) AS sample_keys -- noqa: LT01
FROM dup
