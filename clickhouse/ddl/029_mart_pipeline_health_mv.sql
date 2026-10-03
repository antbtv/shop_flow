-- Full recompute of mart_pipeline_health every 2 minutes, last in the chain (ADR-0011): it shows
-- the state of the refreshes before it, its own row is the one from the previous cycle.
-- max(event_time) is a scan of one column of each staging table, cheap at the sizes of ADR-0009.
CREATE MATERIALIZED VIEW IF NOT EXISTS shopflow.mart_pipeline_health_mv -- noqa: PRS
REFRESH EVERY 2 MINUTE DEPENDS ON shopflow.mart_inventory_current_mv SETTINGS refresh_retries = 3
TO shopflow.mart_pipeline_health
AS
SELECT
    'source' AS kind,
    name,
    last_event_time,
    CAST(NULL, 'Nullable(String)') AS status,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS last_success_time,
    CAST(NULL, 'Nullable(String)') AS last_error,
    now64(3, 'UTC') AS refreshed_at
FROM (
    SELECT 'raw_events' AS name, maxOrNull(event_time) AS last_event_time FROM shopflow.raw_events
    UNION ALL
    SELECT 'stg_orders', maxOrNull(event_time) FROM shopflow.stg_orders
    UNION ALL
    SELECT 'stg_order_items', maxOrNull(event_time) FROM shopflow.stg_order_items
    UNION ALL
    SELECT 'stg_inventory', maxOrNull(event_time) FROM shopflow.stg_inventory
    UNION ALL
    SELECT 'stg_customer_versions', maxOrNull(event_time) FROM shopflow.stg_customer_versions
    UNION ALL
    SELECT 'stg_product_versions', maxOrNull(event_time) FROM shopflow.stg_product_versions
)
UNION ALL
SELECT
    'view' AS kind,
    view AS name,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS last_event_time,
    status,
    toNullable(toDateTime64(last_success_time, 3, 'UTC')) AS last_success_time,
    if(exception = '', CAST(NULL, 'Nullable(String)'), exception) AS last_error,
    now64(3, 'UTC') AS refreshed_at
FROM system.view_refreshes
WHERE database = 'shopflow'
SETTINGS max_memory_usage = 268435456, max_threads = 2;
