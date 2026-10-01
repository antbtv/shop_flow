-- Order funnel by day of order creation (FR-5, ADR-0009). Filled only by mart_funnel_daily_mv.
-- "Reached" is monotonic along created -> paid -> shipped -> delivered: a delivered order counts
-- as paid and shipped even without those rows (snapshot). cancelled is separate: a paid order may
-- be cancelled later, so it counts in both paid and cancelled. Durations use only real
-- transitions (not snapshot rows), in seconds; NULL for a day without such orders (not 0).
CREATE TABLE IF NOT EXISTS shopflow.mart_funnel_daily
(
    order_date                 Date,
    created                    UInt64,
    paid                       UInt64,
    shipped                    UInt64,
    delivered                  UInt64,
    cancelled                  UInt64,
    to_paid_median_s           Nullable(Float64),
    to_paid_p90_s              Nullable(Float64),
    paid_to_delivered_median_s Nullable(Float64),
    paid_to_delivered_p90_s    Nullable(Float64),
    refreshed_at               DateTime64(3, 'UTC'),
    source_watermark           DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY order_date;
