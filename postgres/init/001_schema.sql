-- OLTP schema (PRD 5.1). Runs once, on an empty data volume.
-- Schema changes after the first start are applied manually (see ADR-0005: Spark and ClickHouse change together).

CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    address     TEXT,
    segment     TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    category   TEXT NOT NULL,
    price      NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id    BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers (customer_id),
    status      TEXT NOT NULL, -- created | paid | shipped | delivered | cancelled
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id  BIGSERIAL PRIMARY KEY,
    order_id       BIGINT NOT NULL REFERENCES orders (order_id),
    product_id     BIGINT NOT NULL REFERENCES products (product_id),
    quantity       INT NOT NULL,
    price_at_order NUMERIC(10, 2) NOT NULL
);

CREATE TABLE inventory (
    product_id   BIGINT NOT NULL REFERENCES products (product_id),
    warehouse_id INT NOT NULL,
    quantity     INT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, warehouse_id)
);

-- Full old row in UPDATE/DELETE events: needed for SCD2, DQ and debugging (ADR-0005).
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;

-- Created by the schema owner so the Debezium role needs no ownership (publication.autocreate.mode=disabled).
CREATE PUBLICATION shopflow_cdc FOR TABLE customers, products, orders, order_items, inventory;
