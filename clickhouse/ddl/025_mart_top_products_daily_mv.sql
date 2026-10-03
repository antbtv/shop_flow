-- Full recompute of mart_top_products_daily every 2 minutes, after mart_cohort_retention_mv:
-- the dependency only orders the refreshes (ADR-0009, ADR-0011). Second pass over fact_orders
-- per period next to mart_revenue_daily_mv: a conscious exception to the ADR-0009 threshold,
-- re-checked by the bench of task 5.3.
CREATE MATERIALIZED VIEW IF NOT EXISTS shopflow.mart_top_products_daily_mv -- noqa: PRS
REFRESH EVERY 2 MINUTE DEPENDS ON shopflow.mart_cohort_retention_mv SETTINGS refresh_retries = 3
TO shopflow.mart_top_products_daily
AS
WITH
names AS (
    SELECT
        product_id,
        argMax(name, version) AS product_name
    FROM shopflow.dim_products
    WHERE is_current = 1
    GROUP BY product_id
),

sales AS (
    SELECT
        order_date,
        product_id,
        ifNull(category, 'unknown') AS category,
        sum(quantity) AS units,
        sumIf(quantity, status != 'cancelled') AS units_net,
        sum(amount) AS revenue,
        sumIf(amount, status != 'cancelled') AS revenue_net,
        max(event_time) AS watermark
    FROM shopflow.fact_orders
    GROUP BY order_date, product_id, category
)

SELECT
    s.order_date AS order_date,
    s.product_id AS product_id,
    s.category AS category,
    ifNull(n.product_name, 'unknown') AS product_name,
    s.units AS quantity,
    s.units_net AS quantity_net,
    s.revenue AS revenue,
    s.revenue_net AS revenue_net,
    now64(3, 'UTC') AS refreshed_at,
    max(s.watermark) OVER () AS source_watermark
FROM sales AS s
LEFT JOIN names AS n ON s.product_id = n.product_id
SETTINGS max_memory_usage = 805306368, max_threads = 2, join_use_nulls = 1;
