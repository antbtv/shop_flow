-- Full recompute of mart_revenue_daily every 2 minutes, after dim_products_mv (DEPENDS ON):
-- a new product or price is in the dimension before the mart reads it, and the refreshes of
-- the chain never hold memory at the same time (ADR-0009, stand 3.5). An incremental MV would
-- count ReplacingMergeTree updates and replays twice.
CREATE MATERIALIZED VIEW IF NOT EXISTS shopflow.mart_revenue_daily_mv -- noqa: PRS
REFRESH EVERY 2 MINUTE DEPENDS ON shopflow.dim_products_mv SETTINGS refresh_retries = 3
TO shopflow.mart_revenue_daily
AS
SELECT
    order_date,
    ifNull(category, 'unknown') AS category,
    uniqExact(order_id) AS orders,
    sum(quantity) AS items,
    sum(amount) AS revenue,
    sumIf(amount, status != 'cancelled') AS revenue_net,
    now64(3, 'UTC') AS refreshed_at,
    max(max(event_time)) OVER () AS source_watermark
FROM shopflow.fact_orders
GROUP BY order_date, category
SETTINGS max_memory_usage = 805306368, max_threads = 2;
