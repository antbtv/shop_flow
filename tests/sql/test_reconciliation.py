"""Reconciliation logic (FR-8, ADR-0010) on a throwaway Postgres and ClickHouse.

The first test is the one that matters most: identical data must give identical buckets in all
five tables, i.e. the canonical row and the 64-bit fingerprint agree across the two databases.
ClickHouse rows are written the way Spark writes them: NULL text as '', updated_at in ms.
"""

from datetime import UTC, datetime, timedelta

import pytest
from shopflow_checks.reconciliation import (
    TABLES,
    TABLES_BY_NAME,
    pipeline_lagging,
    postgres_cutoff,
    recheck_extra,
    reconcile_table,
)

CUTOFF = datetime(2026, 10, 1, tzinfo=UTC)
OLD = datetime(2026, 9, 1, 8, 30, 15, 123456, tzinfo=UTC)
HOT = CUTOFF + timedelta(minutes=5)

PG_TABLES = "order_items, inventory, orders, products, customers"
CH_TABLES = ("stg_customer_versions", "stg_product_versions", "stg_orders", "stg_order_items",
             "stg_inventory", "raw_events")


def lit(value) -> str:
    """ClickHouse literal for the mirror inserts."""
    if value is None:
        return "''"  # Spark writes NULL text as '' into non-Nullable String (ADR-0010)
    if isinstance(value, datetime):
        return f"'{value.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f}'"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return str(value)


def ms(ts: datetime) -> datetime:
    """DateTime64(3): Spark keeps milliseconds."""
    return ts.replace(microsecond=ts.microsecond // 1000 * 1000)


class Db:
    def __init__(self, pg, ch):
        self.conn, self.ch = pg, ch

    def pg(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []

    def ch_rows(self, sql):
        return self.ch.rows(sql)

    def ch_insert(self, table, columns, rows):
        values = ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in rows)
        self.ch.query(f"INSERT INTO shopflow.{table} ({columns}) VALUES {values}")

    def mirror(self, version=100):
        """Copy the current Postgres state into ClickHouse as the stream would."""
        customers = self.pg("SELECT customer_id, name, address, segment, updated_at"
                            " FROM customers")
        self.ch_insert(
            "stg_customer_versions",
            "customer_id, valid_from, name, address, segment, is_snapshot, is_deleted,"
            " event_time, version",
            [(c, u, n, a, s, 0, 0, ms(u), version) for c, n, a, s, u in customers],
        )
        products = self.pg("SELECT product_id, name, category, price, updated_at FROM products")
        self.ch_insert(
            "stg_product_versions",
            "product_id, valid_from, name, category, price, is_snapshot, is_deleted,"
            " event_time, version",
            [(p, u, n, c, pr, 0, 0, ms(u), version) for p, n, c, pr, u in products],
        )
        orders = self.pg("SELECT order_id, customer_id, status, created_at, updated_at"
                         " FROM orders")
        self.ch_insert(
            "stg_orders",
            "order_id, customer_id, status, created_at, updated_at, version, is_deleted,"
            " event_time",
            [(o, c, s, cr, ms(u), version, 0, ms(u)) for o, c, s, cr, u in orders],
        )
        items = self.pg("SELECT order_item_id, order_id, product_id, quantity, price_at_order"
                        " FROM order_items")
        self.ch_insert(
            "stg_order_items",
            "order_item_id, order_id, product_id, quantity, price_at_order, version,"
            " is_deleted, event_time",
            [(*row, version, 0, ms(OLD)) for row in items],
        )
        inventory = self.pg("SELECT product_id, warehouse_id, quantity, updated_at"
                            " FROM inventory")
        self.ch_insert(
            "stg_inventory",
            "product_id, warehouse_id, quantity, updated_at, event_time, version, is_deleted",
            [(p, w, q, ms(u), ms(u), version, 0) for p, w, q, u in inventory],
        )

    def reconcile(self, name, cutoff=CUTOFF):
        return reconcile_table(TABLES_BY_NAME[name], self.pg, self.ch_rows, cutoff)


@pytest.fixture
def db(pg, ch):
    d = Db(pg, ch)
    d.pg(f"TRUNCATE {PG_TABLES} RESTART IDENTITY CASCADE")
    for table in CH_TABLES:
        ch.query(f"TRUNCATE TABLE shopflow.{table}")
    seed(d)
    return d


def seed(db):
    """Values that break naive formatting: NULL vs '', quotes, Cyrillic, cents, microseconds."""
    for i in range(1, 41):
        address = None if i % 7 == 0 else f"Ёлкино, ул. О'Нил \\{i}"
        segment = None if i % 5 == 0 else ("vip" if i % 2 else "retail")
        db.pg("INSERT INTO customers (customer_id, name, email, address, segment, updated_at)"
              " VALUES (%s, %s, %s, %s, %s, %s)",
              (i, f"Анна {i}", f"a{i}@example.com", address, segment,
               OLD + timedelta(seconds=i, microseconds=i * 7)))
    for i in range(1, 16):
        price = ["0.10", "12345678.99", "1.00", "19.99", "5.05"][i % 5]
        db.pg("INSERT INTO products (product_id, name, category, price, updated_at)"
              " VALUES (%s, %s, %s, %s, %s)",
              (i, f"Товар «{i}»", "food" if i % 2 else "toys", price,
               OLD + timedelta(minutes=i, microseconds=999)))
        for w in (1, 2, 3):
            db.pg("INSERT INTO inventory (product_id, warehouse_id, quantity, updated_at)"
                  " VALUES (%s, %s, %s, %s)",
                  (i, w, i * w - 5, OLD + timedelta(seconds=i * w, microseconds=500)))
    for i in range(1, 61):
        db.pg("INSERT INTO orders (order_id, customer_id, status, created_at, updated_at)"
              " VALUES (%s, %s, %s, %s, %s)",
              (i, i % 40 + 1, ["created", "paid", "delivered"][i % 3],
               OLD + timedelta(hours=i, microseconds=123), OLD + timedelta(hours=i, seconds=1)))
        for j in (1, 2):
            db.pg("INSERT INTO order_items (order_item_id, order_id, product_id, quantity,"
                  " price_at_order) VALUES (%s, %s, %s, %s, %s)",
                  (i * 10 + j, i, (i + j) % 15 + 1, j * (1 if i % 9 else -1), "3.33"))
    db.mirror()


def test_identical_data_gives_identical_buckets_in_every_table(db):
    for spec in TABLES:
        result = db.reconcile(spec.name)
        assert result.buckets_differ == 0, spec.name
        assert result.violations == 0, spec.name
        assert result.pg_rows == result.ch_rows > 0, spec.name


def test_row_missing_in_clickhouse_is_a_violation_with_a_hint(db):
    db.ch_insert("stg_orders", "order_id, customer_id, status, created_at, updated_at, version,"
                 " is_deleted, event_time", [(7, 8, "paid", OLD, ms(OLD), 999, 1, ms(OLD))])
    result = db.reconcile("orders")
    assert result.missing_in_ch == ["7"]
    assert result.violations == 1
    assert "raw_events" in result.details()["hint"]


def test_different_value_is_a_violation(db):
    # A newer journal version in ClickHouse with an address Postgres never had.
    db.ch_insert("stg_customer_versions",
                 "customer_id, valid_from, name, address, segment, is_snapshot, is_deleted,"
                 " event_time, version",
                 [(3, OLD + timedelta(days=1), "Анна 3", "Wrong", "vip", 0, 0, ms(OLD), 500)])
    result = db.reconcile("customers")
    assert result.different == ["3"]
    assert result.missing_in_ch == result.extra_in_ch == []


def test_cents_and_milliseconds_are_compared(db):
    # One cent and one millisecond are differences, not formatting noise.
    db.ch_insert("stg_order_items", "order_item_id, order_id, product_id, quantity,"
                 " price_at_order, version, is_deleted, event_time",
                 [(11, 1, 3, 1, "3.34", 999, 0, ms(OLD))])
    db.ch_insert("stg_inventory", "product_id, warehouse_id, quantity, updated_at, event_time,"
                 " version, is_deleted",
                 [(2, 1, -3, ms(OLD) + timedelta(seconds=2, milliseconds=1), ms(OLD), 999, 0)])
    assert db.reconcile("order_items").different == ["11"]
    assert db.reconcile("inventory").different == ["2:1"]


def test_row_changed_after_the_cutoff_is_in_flight(db):
    # Postgres moved on after T, ClickHouse still has the old version: not a violation.
    db.pg("UPDATE customers SET address = 'New', updated_at = %s WHERE customer_id = 4", (HOT,))
    db.pg("UPDATE orders SET status = 'paid', updated_at = %s WHERE order_id = 9", (HOT,))
    for name, key in (("customers", "4"), ("orders", "9")):
        result = db.reconcile(name)
        assert result.violations == 0, name
        assert result.in_flight >= 1, name
        assert key not in result.different


def test_deleted_row_is_extra_until_its_tombstone_arrives(db):
    db.pg("DELETE FROM order_items WHERE order_id = 12")
    db.pg("DELETE FROM orders WHERE order_id = 12")
    result = db.reconcile("orders")
    assert result.extra_in_ch == ["12"]
    spec = TABLES_BY_NAME["orders"]
    assert recheck_extra(spec, ["12"], db.pg, db.ch_rows) == ["12"]
    # The tombstone arrives: the key is gone from the live rows of ClickHouse.
    db.ch_insert("stg_orders", "order_id, customer_id, status, created_at, updated_at, version,"
                 " is_deleted, event_time", [(12, 13, "created", OLD, ms(OLD), 999, 1, ms(OLD))])
    assert recheck_extra(spec, ["12"], db.pg, db.ch_rows) == []
    assert db.reconcile("orders").violations == 0


def test_items_of_an_order_created_after_the_cutoff_are_not_compared_yet(db):
    db.pg("INSERT INTO orders (order_id, customer_id, status, created_at, updated_at)"
          " VALUES (100, 1, 'created', %s, %s)", (HOT, HOT))
    db.pg("INSERT INTO order_items (order_item_id, order_id, product_id, quantity,"
          " price_at_order) VALUES (1001, 100, 1, 1, 1.00)")
    assert db.reconcile("orders").violations == 0
    assert db.reconcile("order_items").violations == 0


def test_item_whose_order_is_missing_in_clickhouse_is_still_compared(db):
    # Order 20 quarantined: missing in stg_orders, its items are there and must still match.
    db.ch_insert("stg_orders", "order_id, customer_id, status, created_at, updated_at, version,"
                 " is_deleted, event_time", [(20, 21, "paid", OLD, ms(OLD), 999, 1, ms(OLD))])
    assert db.reconcile("orders").missing_in_ch == ["20"]
    items = db.reconcile("order_items")
    assert items.violations == 0
    assert items.pg_rows == items.ch_rows


def test_deleted_customer_in_both_databases_matches(db):
    # Customer 41 has no orders (the seed's orders reference 1..40), so it can be deleted.
    db.pg("INSERT INTO customers (customer_id, name, email, updated_at)"
          " VALUES (41, 'Bob', 'b@example.com', %s)", (OLD,))
    db.mirror(version=200)
    assert db.reconcile("customers").violations == 0
    db.pg("DELETE FROM customers WHERE customer_id = 41")
    db.ch_insert("stg_customer_versions",
                 "customer_id, valid_from, name, address, segment, is_snapshot, is_deleted,"
                 " event_time, version",
                 [(41, OLD + timedelta(days=2), "Bob", "", "", 0, 1, ms(OLD), 600)])
    assert db.reconcile("customers").violations == 0


def test_postgres_cutoff_is_whole_seconds_fifteen_minutes_ago(db):
    cutoff = postgres_cutoff(db.pg)
    ((now,),) = db.pg("SELECT now()")
    assert cutoff.microsecond == 0
    assert timedelta(minutes=15) <= now - cutoff < timedelta(minutes=15, seconds=1)


def test_lagging_when_settled_changes_never_reached_clickhouse(db):
    cutoff = postgres_cutoff(db.pg)
    # Changed 2 min after T (13 min ago): the stream should have delivered it long ago.
    db.pg("UPDATE orders SET updated_at = %s WHERE order_id = 1", (cutoff + timedelta(minutes=2),))
    assert pipeline_lagging(db.pg, db.ch_rows, cutoff)
    db.ch.query("INSERT INTO shopflow.raw_events (topic, kafka_partition, kafka_offset,"
                " source_lsn, event_key, event_time, op, payload) VALUES ('cdc.public.orders',"
                f" 0, 1, 1, '{{}}', {lit(cutoff + timedelta(minutes=2))}, 'u', '{{}}')")
    assert not pipeline_lagging(db.pg, db.ch_rows, cutoff)


def test_fresh_changes_alone_are_not_lagging(db):
    cutoff = postgres_cutoff(db.pg)
    # Changed a minute ago: may still be on its way (NFR-3 allows 5 min).
    db.pg("UPDATE orders SET updated_at = now() - interval '1 minute' WHERE order_id = 1")
    assert not pipeline_lagging(db.pg, db.ch_rows, cutoff)
