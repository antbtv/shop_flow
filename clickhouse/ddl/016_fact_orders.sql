-- Order lines fact (ADR-0009): a view, one row per order_items row of an existing order.
-- ASOF LEFT JOIN picks the product version valid at order creation (point-in-time join to the
-- SCD2 dimension): category and list_price as of created_at. created_at is DateTime64(6):
-- with milliseconds an order started in the same ms as a price change gets the old price.
-- join_use_nulls: a miss (no version yet) gives NULL, not '' and 0.
-- INNER JOIN hides items whose order has not arrived yet (topic lag) or is deleted: data
-- quality checks (FR-9) read stg_*, not this view. Marts read it inside refreshable MVs.
-- Explicit aliases: without them ClickHouse may name a column "i.order_id".
-- noqa: disable=AL09
CREATE VIEW IF NOT EXISTS shopflow.fact_orders AS
SELECT
    i.order_item_id AS order_item_id,
    i.order_id AS order_id,
    o.customer_id AS customer_id,
    i.product_id AS product_id,
    o.status AS status,
    o.created_at AS created_at,
    i.quantity AS quantity,
    i.price_at_order AS price_at_order,
    p.category AS category,
    p.price AS list_price,
    toDate(o.created_at) AS order_date,
    i.quantity * toDecimal64(i.price_at_order, 2) AS amount,
    greatest(i.event_time, o.event_time) AS event_time
FROM shopflow.stg_order_items AS i FINAL
INNER JOIN shopflow.stg_orders AS o FINAL ON i.order_id = o.order_id
ASOF LEFT JOIN shopflow.dim_products AS p
    ON i.product_id = p.product_id AND o.created_at >= p.valid_from
SETTINGS join_use_nulls = 1;
