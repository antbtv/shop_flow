-- Revenue by day and category (FR-4, ADR-0009). Filled only by mart_revenue_daily_mv: every
-- refresh replaces the whole table. Day = UTC date of order creation; category as of the order
-- (SCD2), 'unknown' if the product had no version yet. orders counts distinct orders per
-- category: an order with lines in two categories counts in both, so do not sum it over them.
-- refreshed_at and source_watermark (max event_time of the fact) measure the mart lag (NFR-3).
CREATE TABLE IF NOT EXISTS shopflow.mart_revenue_daily
(
    order_date       Date,
    category         LowCardinality(String),
    orders           UInt64,
    items            Int64,
    revenue          Decimal(38, 2),
    -- without cancelled orders
    revenue_net      Decimal(38, 2),
    refreshed_at     DateTime64(3, 'UTC'),
    source_watermark DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (order_date, category);
