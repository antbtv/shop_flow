-- table: mart_revenue_daily
-- hint: negative revenue in the mart: a negative line (check 05) or a wrong join in fact_orders
SELECT
    count() AS violations,
    groupArray(10)(concat(toString(order_date), ':', category)) AS sample_keys -- noqa: LT01
FROM shopflow.mart_revenue_daily
WHERE revenue < 0 OR revenue_net < 0
