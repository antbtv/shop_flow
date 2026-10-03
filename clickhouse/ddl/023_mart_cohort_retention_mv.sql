-- Full recompute of mart_cohort_retention every 2 minutes, after mart_funnel_daily_mv: the
-- dependency only orders the refreshes, so two marts never hold memory at once (ADR-0009,
-- ADR-0011). Reads stg_orders only (customer, month, status): no items, no ASOF.
-- One row per (customer, month) is enough: the cohort is the earliest month of the customer.
CREATE MATERIALIZED VIEW IF NOT EXISTS shopflow.mart_cohort_retention_mv -- noqa: PRS
REFRESH EVERY 2 MINUTE DEPENDS ON shopflow.mart_funnel_daily_mv SETTINGS refresh_retries = 3
TO shopflow.mart_cohort_retention
AS
WITH
active AS (
    SELECT
        customer_id,
        toStartOfMonth(created_at) AS order_month
    FROM shopflow.stg_orders FINAL
    -- An order dated in the future (a broken clock, a manual insert) is neither a cohort start nor
    -- a return; a negative month distance would also wrap in range(toUInt32(...)) below.
    WHERE status != 'cancelled' AND created_at <= now('UTC')
    GROUP BY customer_id, order_month
),

tagged AS (
    SELECT
        order_month,
        min(order_month) OVER (PARTITION BY customer_id) AS cohort_month
    FROM active
),

counts AS (
    SELECT
        cohort_month,
        toUInt16(dateDiff('month', cohort_month, order_month)) AS period,
        count() AS returned
    FROM tagged
    GROUP BY cohort_month, period
),

cohorts AS (
    SELECT
        cohort_month,
        returned AS customers
    FROM counts
    WHERE period = 0
),

grid AS (
    SELECT
        cohort_month,
        customers,
        arrayJoin(range(toUInt32(
            dateDiff('month', cohort_month, toStartOfMonth(now('UTC'))) + 1
        ))) AS period
    FROM cohorts
)

SELECT
    g.cohort_month AS cohort_month,
    toUInt16(g.period) AS period,
    g.customers AS customers,
    ifNull(c.returned, 0) AS returned,
    addMonths(g.cohort_month, g.period) = toStartOfMonth(now('UTC')) AS is_partial,
    now64(3, 'UTC') AS refreshed_at,
    (SELECT max(event_time) FROM shopflow.stg_orders) AS source_watermark
FROM grid AS g
LEFT JOIN counts AS c ON g.cohort_month = c.cohort_month AND toUInt16(g.period) = c.period
SETTINGS max_memory_usage = 805306368, max_threads = 2, join_use_nulls = 1;
