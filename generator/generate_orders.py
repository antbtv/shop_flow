"""Synthetic OLTP load for ShopFlow: orders with items, status changes, customers, prices, stock.

Connection comes from libpq-style env vars (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
POSTGRES_USER, POSTGRES_PASSWORD). Run through compose so secrets stay in .env:

    docker compose -f docker-compose.laptop.yml --profile generator run --rm generator --duration 60

Every action is its own transaction, like independent requests to a shop backend. Actions
run one after another: a price change commits before the next order starts, which the
price_at_order = list price check in fact_orders relies on (ADR-0009).
"""

import argparse
import logging
import os
import random
import signal
import time
from collections import Counter
from datetime import UTC, datetime, timedelta

import psycopg

from generator.model import (
    OPEN_STATUSES,
    WAREHOUSES,
    Customer,
    Product,
    StockRow,
    change_customer,
    change_product,
    choose_action,
    choose_order_lines,
    initial_stock,
    load_multiplier,
    new_customer,
    new_product,
    next_status,
    restock_amount,
)

log = logging.getLogger("generator")
MSK = timedelta(hours=3)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--rate", type=float, default=2.0, help="actions per second at load 1.0")
    p.add_argument("--duration", type=float, default=0, help="seconds to run, 0 = forever")
    p.add_argument("--seed", type=int, default=None, help="random seed for reproducible runs")
    p.add_argument("--customers", type=int, default=50, help="seed customers if table is empty")
    p.add_argument("--products", type=int, default=100, help="top up the catalog to this size")
    p.add_argument(
        "--no-seasonality", action="store_true", help="constant rate, no hourly multiplier"
    )
    return p.parse_args(argv)


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    )


def insert_customer(conn: psycopg.Connection, c: Customer) -> None:
    conn.execute(
        "INSERT INTO customers (name, email, address, segment) VALUES (%s, %s, %s, %s)",
        (c.name, c.email, c.address, c.segment),
    )


def seed_customers(conn: psycopg.Connection, n: int, rng: random.Random) -> None:
    (count,) = conn.execute("SELECT count(*) FROM customers").fetchone()
    if count:
        return
    with conn.transaction():
        for _ in range(n):
            insert_customer(conn, new_customer(rng))
    log.info("seeded %d customers", n)


def insert_product(conn: psycopg.Connection, p: Product, rng: random.Random) -> None:
    """A product and its stock in every warehouse, in the caller's transaction."""
    (product_id,) = conn.execute(
        "INSERT INTO products (name, category, price) VALUES (%s, %s, %s) RETURNING product_id",
        (p.name, p.category, p.price),
    ).fetchone()
    for warehouse_id in WAREHOUSES:
        conn.execute(
            "INSERT INTO inventory (product_id, warehouse_id, quantity) VALUES (%s, %s, %s)",
            (product_id, warehouse_id, initial_stock(rng)),
        )


def seed_products(conn: psycopg.Connection, n: int, rng: random.Random) -> None:
    """Top up rather than seed only an empty table: a leftover test row must not block it."""
    (count,) = conn.execute("SELECT count(*) FROM products").fetchone()
    missing = n - count
    if missing <= 0:
        return
    with conn.transaction():
        for _ in range(missing):
            insert_product(conn, new_product(rng), rng)
    log.info("seeded %d products", missing)


def seed_inventory(conn: psycopg.Connection, rng: random.Random) -> None:
    """Stock for (product, warehouse) pairs that have none, e.g. products from before M3."""
    missing = conn.execute(
        "SELECT p.product_id, w.warehouse_id FROM products p "
        "CROSS JOIN unnest(%s::int[]) AS w(warehouse_id) "
        "LEFT JOIN inventory i USING (product_id, warehouse_id) "
        "WHERE i.product_id IS NULL ORDER BY 1, 2",
        (list(WAREHOUSES),),
    ).fetchall()
    if not missing:
        return
    with conn.transaction():
        for product_id, warehouse_id in missing:
            conn.execute(
                "INSERT INTO inventory (product_id, warehouse_id, quantity) VALUES (%s, %s, %s)",
                (product_id, warehouse_id, initial_stock(rng)),
            )
    log.info("seeded %d inventory rows", len(missing))


def random_customer_id(conn: psycopg.Connection) -> int | None:
    row = conn.execute(
        "SELECT customer_id FROM customers ORDER BY random() LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def new_order(conn: psycopg.Connection, rng: random.Random) -> bool:
    """Order, its items and the stock write-off in one transaction; no stock, no order."""
    with conn.transaction():
        customer_id = random_customer_id(conn)
        if customer_id is None:
            return False
        rows = conn.execute(
            "SELECT i.product_id, i.warehouse_id, i.quantity, p.price "
            "FROM inventory i JOIN products p USING (product_id) WHERE i.quantity > 0 "
            "ORDER BY random() LIMIT 20 FOR UPDATE OF i SKIP LOCKED"
        ).fetchall()
        lines = choose_order_lines([StockRow(*row) for row in rows], rng)
        if not lines:
            return False
        (order_id,) = conn.execute(
            "INSERT INTO orders (customer_id, status) VALUES (%s, 'created') RETURNING order_id",
            (customer_id,),
        ).fetchone()
        for line in lines:
            conn.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, price_at_order) "
                "VALUES (%s, %s, %s, %s)",
                (order_id, line.product_id, line.quantity, line.price),
            )
            conn.execute(
                "UPDATE inventory SET quantity = quantity - %s, updated_at = now() "
                "WHERE product_id = %s AND warehouse_id = %s",
                (line.quantity, line.product_id, line.warehouse_id),
            )
    return True


def advance_order(conn: psycopg.Connection, rng: random.Random) -> bool:
    with conn.transaction():
        # SKIP LOCKED: several generators can run without blocking each other.
        row = conn.execute(
            "SELECT order_id, status FROM orders WHERE status = ANY(%s) "
            "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED",
            (list(OPEN_STATUSES),),
        ).fetchone()
        if row is None:
            return False
        order_id, status = row
        new = next_status(status, rng)
        conn.execute(
            "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
            (new, order_id),
        )
    return True


def update_customer(conn: psycopg.Connection, rng: random.Random) -> bool:
    with conn.transaction():
        row = conn.execute(
            "SELECT customer_id, name, email, address, segment FROM customers "
            "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED"
        ).fetchone()
        if row is None:
            return False
        customer_id, *fields = row
        changed = change_customer(Customer(*fields), rng)
        conn.execute(
            "UPDATE customers SET address = %s, segment = %s, updated_at = now() "
            "WHERE customer_id = %s",
            (changed.address, changed.segment, customer_id),
        )
    return True


def add_customer(conn: psycopg.Connection, rng: random.Random) -> bool:
    insert_customer(conn, new_customer(rng))
    return True


def update_product(conn: psycopg.Connection, rng: random.Random) -> bool:
    with conn.transaction():
        row = conn.execute(
            "SELECT product_id, name, category, price FROM products "
            "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED"
        ).fetchone()
        if row is None:
            return False
        product_id, *fields = row
        changed = change_product(Product(*fields), rng)
        conn.execute(
            "UPDATE products SET category = %s, price = %s, updated_at = now() "
            "WHERE product_id = %s",
            (changed.category, changed.price, product_id),
        )
    return True


def add_product(conn: psycopg.Connection, rng: random.Random) -> bool:
    with conn.transaction():
        insert_product(conn, new_product(rng), rng)
    return True


def restock(conn: psycopg.Connection, rng: random.Random) -> bool:
    """Refill one of the ten emptiest shelves."""
    with conn.transaction():
        row = conn.execute(
            "SELECT product_id, warehouse_id FROM "
            "(SELECT product_id, warehouse_id FROM inventory ORDER BY quantity LIMIT 10) low "
            "ORDER BY random() LIMIT 1"
        ).fetchone()
        if row is None:
            return False
        conn.execute(
            "UPDATE inventory SET quantity = quantity + %s, updated_at = now() "
            "WHERE product_id = %s AND warehouse_id = %s",
            (restock_amount(rng), *row),
        )
    return True


ACTIONS = {
    "new_order": new_order,
    "advance_order": advance_order,
    "update_customer": update_customer,
    "new_customer": add_customer,
    "update_product": update_product,
    "new_product": add_product,
    "restock": restock,
}


def run(args: argparse.Namespace) -> Counter:
    rng = random.Random(args.seed)
    stats: Counter = Counter()
    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    with connect() as conn:
        seed_customers(conn, args.customers, rng)
        seed_products(conn, args.products, rng)
        seed_inventory(conn, rng)
        started = time.monotonic()
        last_report = started
        while not stop:
            now = time.monotonic()
            if args.duration and now - started >= args.duration:
                break
            action = choose_action(rng)
            if ACTIONS[action](conn, rng):
                stats[action] += 1
            if now - last_report >= 60:
                log.info("actions so far: %s", dict(stats))
                last_report = now
            hour = (datetime.now(UTC) + MSK).hour
            load = 1.0 if args.no_seasonality else load_multiplier(hour)
            # Exponential gaps give a Poisson stream instead of a metronome.
            time.sleep(rng.expovariate(args.rate * load))
    log.info("done: %s", dict(stats))
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(parse_args())


if __name__ == "__main__":
    main()
