-- Full recompute of mart_funnel_daily every 2 minutes, after mart_revenue_daily_mv: the
-- dependency only orders the refreshes, so two marts never hold memory at once (ADR-0009).
-- First time of each status: min(changed_at), a later snapshot or a repeated status does not
-- move it. Day and created_at come from stg_orders, not from the first history row (for a
-- snapshot that would be the snapshot time).
CREATE MATERIALIZED VIEW IF NOT EXISTS shopflow.mart_funnel_daily_mv -- noqa: PRS
REFRESH EVERY 2 MINUTE DEPENDS ON shopflow.mart_revenue_daily_mv SETTINGS refresh_retries = 3
TO shopflow.mart_funnel_daily
AS
WITH
history AS (
    SELECT
        order_id,
        max(multiIf(
            status = 'created', 1, status = 'paid', 2, status = 'shipped', 3,
            status = 'delivered', 4, 0
        )) AS history_level,
        max(status = 'cancelled') AS was_cancelled,
        minIfOrNull(changed_at, status = 'paid' AND is_snapshot = 0) AS paid_at,
        minIfOrNull(changed_at, status = 'delivered' AND is_snapshot = 0) AS delivered_at,
        max(changed_at) AS last_change
    FROM shopflow.stg_order_status_history FINAL
    GROUP BY order_id
),

orders AS (
    SELECT
        o.order_id AS order_id,
        o.created_at AS created_at,
        o.event_time AS event_time,
        h.paid_at AS paid_at,
        h.delivered_at AS delivered_at,
        greatest(
            ifNull(h.history_level, 0),
            multiIf(
                o.status = 'created', 1, o.status = 'paid', 2, o.status = 'shipped', 3,
                o.status = 'delivered', 4, 0
            )
        ) AS level,
        o.status = 'cancelled' OR ifNull(h.was_cancelled, 0) = 1 AS is_cancelled,
        toDate(o.created_at) AS order_date,
        greatest(o.event_time, ifNull(h.last_change, o.event_time)) AS watermark
    FROM shopflow.stg_orders AS o FINAL
    LEFT JOIN history AS h ON o.order_id = h.order_id
)

SELECT
    order_date,
    count() AS created,
    countIf(level >= 2) AS paid,
    countIf(level >= 3) AS shipped,
    countIf(level >= 4) AS delivered,
    countIf(is_cancelled) AS cancelled,
    quantileIf(0.5)(dateDiff('millisecond', created_at, paid_at) / 1000, paid_at IS NOT NULL)
        AS to_paid_median_s,
    quantileIf(0.9)(dateDiff('millisecond', created_at, paid_at) / 1000, paid_at IS NOT NULL)
        AS to_paid_p90_s,
    quantileIf(0.5)(
        dateDiff('millisecond', paid_at, delivered_at) / 1000,
        paid_at IS NOT NULL AND delivered_at IS NOT NULL
    ) AS paid_to_delivered_median_s,
    quantileIf(0.9)(
        dateDiff('millisecond', paid_at, delivered_at) / 1000,
        paid_at IS NOT NULL AND delivered_at IS NOT NULL
    ) AS paid_to_delivered_p90_s,
    now64(3, 'UTC') AS refreshed_at,
    max(max(watermark)) OVER () AS source_watermark
FROM orders
GROUP BY order_date
SETTINGS max_memory_usage = 805306368, max_threads = 2, join_use_nulls = 1;
