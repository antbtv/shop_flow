-- Customer retention by cohort (FR-6, ADR-0011). Filled only by mart_cohort_retention_mv:
-- every refresh replaces the whole table. Cohort = UTC month of the customer's first order that
-- is not cancelled (created, paid, shipped and delivered all count); period = whole months since
-- that month. returned = customers of the cohort with a non-cancelled order in that month, so in
-- period 0 returned = customers and a second order in the same month is not a return.
-- Retention = returned / customers. The grid is full (periods 0..now, returned = 0 when nobody
-- came back), so a heatmap has zeros, not holes. is_partial = 1 on the diagonal of the current
-- month, which is not over yet. Cohorts move backwards in time if a first order is cancelled later.
CREATE TABLE IF NOT EXISTS shopflow.mart_cohort_retention
(
    cohort_month     Date,
    period           UInt16,
    customers        UInt64,
    returned         UInt64,
    is_partial       UInt8,
    refreshed_at     DateTime64(3, 'UTC'),
    source_watermark DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (cohort_month, period);
