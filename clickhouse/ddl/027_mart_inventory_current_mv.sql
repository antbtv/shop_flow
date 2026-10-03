-- Full recompute of mart_inventory_current every 2 minutes, after mart_top_products_daily_mv
-- (ADR-0011). stg_inventory FINAL is small (products x warehouses), no fact pass.
-- LOW_STOCK_THRESHOLD = 20, see the table comment.
CREATE MATERIALIZED VIEW IF NOT EXISTS shopflow.mart_inventory_current_mv -- noqa: PRS
REFRESH EVERY 2 MINUTE DEPENDS ON shopflow.mart_top_products_daily_mv
SETTINGS refresh_retries = 3
TO shopflow.mart_inventory_current
AS
WITH
current_products AS (
    SELECT
        product_id,
        argMax(name, version) AS product_name,
        argMax(category, version) AS category
    FROM shopflow.dim_products
    WHERE is_current = 1
    GROUP BY product_id
)

SELECT
    i.product_id AS product_id,
    i.warehouse_id AS warehouse_id,
    ifNull(p.product_name, 'unknown') AS product_name,
    ifNull(p.category, 'unknown') AS category,
    i.quantity AS quantity,
    i.quantity < 20 AS is_low,
    i.updated_at AS updated_at,
    now64(3, 'UTC') AS refreshed_at,
    max(i.event_time) OVER () AS source_watermark
FROM shopflow.stg_inventory AS i FINAL
LEFT JOIN current_products AS p ON i.product_id = p.product_id
WHERE i.is_deleted = 0
SETTINGS max_memory_usage = 805306368, max_threads = 2, join_use_nulls = 1;
