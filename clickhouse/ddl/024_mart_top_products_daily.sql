-- Top products by day (FR-7, ADR-0011). Filled only by mart_top_products_daily_mv: every refresh
-- replaces the whole table. Grain: UTC day of order creation x product x category as of the
-- order (SCD2, 'unknown' if the product had no version yet). The 7 and 30 day windows are sums
-- over days in the panel, so a window needs no refresh of its own. A product that changed
-- category inside a day has two rows: group by product_id. quantity and revenue count all orders,
-- quantity_net and revenue_net skip cancelled ones. product_name is the current name from
-- dim_products ('unknown' without a version): the dashboard user has no grant on dim_*.
CREATE TABLE IF NOT EXISTS shopflow.mart_top_products_daily
(
    order_date       Date,
    product_id       UInt64,
    category         LowCardinality(String),
    product_name     String,
    quantity         Int64,
    quantity_net     Int64,
    revenue          Decimal(38, 2),
    revenue_net      Decimal(38, 2),
    refreshed_at     DateTime64(3, 'UTC'),
    source_watermark DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (order_date, product_id, category);
