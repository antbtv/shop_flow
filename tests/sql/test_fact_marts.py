"""Regression tests for fact_orders (ASOF to SCD2) and the daily marts (FR-4, FR-5, ADR-0009).

Rows go into stg_* as Spark writes them, dim_products comes from its journal through the
refreshable MV, the marts are refreshed by hand in their DEPENDS ON order.
"""

import pytest

DAY = "2026-10-01"
T0 = "2026-10-01 09:00:00.000000"
T1 = "2026-10-01 10:00:00.000000"
T2 = "2026-10-01 11:00:00.000000"
T2_MINUS_1US = "2026-10-01 10:59:59.999999"
T3 = "2026-10-01 12:00:00.000000"
T4 = "2026-10-01 13:00:00.000000"

TABLES = ("stg_orders", "stg_order_items", "stg_product_versions", "stg_order_status_history",
          "dim_products", "mart_revenue_daily", "mart_funnel_daily")


@pytest.fixture(autouse=True)
def clean(ch):
    for name in TABLES:
        ch.query(f"TRUNCATE TABLE shopflow.{name}")


def products(ch, *rows):
    """rows: (product_id, valid_from, category, price, version, is_snapshot)."""
    values = ", ".join(
        f"({pid}, '{vf}', 'p{pid}', '{cat}', {price}, {snap}, 0, '{vf[:23]}', {ver})"
        for pid, vf, cat, price, ver, snap in rows
    )
    ch.query(
        "INSERT INTO shopflow.stg_product_versions (product_id, valid_from, name, category,"
        f" price, is_snapshot, is_deleted, event_time, version) VALUES {values}"
    )
    ch.refresh("dim_products_mv")


def orders(ch, *rows):
    """rows: (order_id, status, created_at, version, is_deleted)."""
    values = ", ".join(
        f"({oid}, 1, '{status}', '{created}', '{created[:23]}', {ver}, {deleted}, '{created[:23]}')"
        for oid, status, created, ver, deleted in rows
    )
    ch.query(
        "INSERT INTO shopflow.stg_orders (order_id, customer_id, status, created_at, updated_at,"
        f" version, is_deleted, event_time) VALUES {values}"
    )


def items(ch, *rows):
    """rows: (order_item_id, order_id, product_id, quantity, price_at_order)."""
    values = ", ".join(
        f"({iid}, {oid}, {pid}, {qty}, {price}, 1, 0, '{T0[:23]}')"
        for iid, oid, pid, qty, price in rows
    )
    ch.query(
        "INSERT INTO shopflow.stg_order_items (order_item_id, order_id, product_id, quantity,"
        f" price_at_order, version, is_deleted, event_time) VALUES {values}"
    )


def history(ch, *rows):
    """rows: (order_id, status, changed_at, is_snapshot, version)."""
    values = ", ".join(
        f"({oid}, '{status}', '{at[:23]}', {snap}, {ver})" for oid, status, at, snap, ver in rows
    )
    ch.query(
        "INSERT INTO shopflow.stg_order_status_history (order_id, status, changed_at,"
        f" is_snapshot, version) VALUES {values}"
    )


def refresh_marts(ch):
    ch.refresh("mart_revenue_daily_mv")
    ch.refresh("mart_funnel_daily_mv")


@pytest.fixture
def priced(ch):
    # Product 1: snapshot price 10.00 (opens at 1970), 12.00 from T2. Product 2 has no version.
    products(ch, (1, T0, "food", "10.00", 100, 1), (1, T2, "food", "12.00", 200, 0))


def fact(ch):
    return ch.rows(
        "SELECT order_item_id, ifNull(category, 'NULL'), ifNull(toString(list_price), 'NULL'),"
        " toString(amount) FROM shopflow.fact_orders ORDER BY order_item_id"
    )


def test_asof_picks_the_price_valid_at_order_creation(ch, priced):
    orders(
        ch,
        (1, "created", T1, 1, 0),  # before the change: old price
        (2, "created", T2, 1, 0),  # exactly at the change: new price (created_at >= valid_from)
        (3, "created", T2_MINUS_1US, 1, 0),  # 1 µs before: old price, needs DateTime64(6)
    )
    items(ch, (11, 1, 1, 2, "10.00"), (12, 2, 1, 1, "12.00"), (13, 3, 1, 3, "10.00"))
    assert fact(ch) == [
        ("11", "food", "10", "20"),
        ("12", "food", "12", "12"),
        ("13", "food", "10", "30"),
    ]


def test_order_in_the_same_millisecond_after_a_price_change_gets_the_new_price(ch):
    # The M3 bug: price changed at .000500, order created at .000700. Truncated to ms the
    # order (.000) looks older than the change and would get the old price.
    products(ch, (3, T0, "food", "10.00", 100, 1), (3, "2026-10-01 11:00:00.000500", "food",
                                                    "15.00", 200, 0))
    orders(ch, (1, "created", "2026-10-01 11:00:00.000700", 1, 0))
    items(ch, (11, 1, 3, 1, "15.00"))
    assert fact(ch) == [("11", "food", "15", "15")]


def test_product_without_version_gives_null_not_empty(ch, priced):
    orders(ch, (1, "created", T1, 1, 0))
    items(ch, (11, 1, 2, 1, "5.00"))
    assert fact(ch) == [("11", "NULL", "NULL", "5")]


def test_items_of_missing_or_deleted_orders_are_not_in_the_fact(ch, priced):
    orders(ch, (1, "created", T1, 1, 0), (2, "created", T1, 1, 0), (2, "created", T1, 2, 1))
    items(ch, (11, 1, 1, 1, "10.00"), (12, 2, 1, 1, "10.00"), (13, 99, 1, 1, "10.00"))
    assert [r[0] for r in fact(ch)] == ["11"]


def test_revenue_mart_by_day_and_category(ch, priced):
    orders(ch, (1, "paid", T1, 1, 0), (2, "cancelled", T1, 1, 0))
    items(
        ch,
        (11, 1, 1, 2, "10.00"),  # food 20.00
        (12, 1, 2, 1, "5.00"),  # unknown 5.00
        (13, 2, 1, 1, "10.00"),  # food 10.00, cancelled
    )
    refresh_marts(ch)
    assert ch.rows(
        "SELECT toString(order_date), category, orders, items, toString(revenue),"
        " toString(revenue_net) FROM shopflow.mart_revenue_daily ORDER BY category"
    ) == [
        (DAY, "food", "2", "3", "30", "20"),
        (DAY, "unknown", "1", "1", "5", "5"),
    ]


def funnel(ch):
    return ch.rows(
        "SELECT created, paid, shipped, delivered, cancelled,"
        " ifNull(toString(to_paid_median_s), 'NULL'),"
        " ifNull(toString(paid_to_delivered_median_s), 'NULL')"
        f" FROM shopflow.mart_funnel_daily WHERE order_date = '{DAY}'"
    )


def test_funnel_is_monotonic_and_uses_first_transition_times(ch):
    orders(
        ch,
        (1, "delivered", T0, 5, 0),
        (2, "delivered", T0, 5, 0),
        (3, "cancelled", T0, 5, 0),
        (4, "paid", T0, 5, 0),
        (5, "created", T0, 5, 0),
    )
    history(
        ch,
        # 1: created -> paid (+1 h) -> delivered (+3 h), shipped row missing: still counted
        (1, "created", T0, 0, 1), (1, "paid", T1, 0, 2), (1, "delivered", T3, 0, 3),
        # 2: known only from the snapshot as delivered: counted, but no durations
        (2, "delivered", T0, 1, 1),
        # 3: paid, then cancelled: counts in paid and in cancelled
        (3, "created", T0, 0, 1), (3, "paid", T2, 0, 2), (3, "cancelled", T3, 0, 3),
        # 4: paid twice (a repeat): the first time wins
        (4, "created", T0, 0, 1), (4, "paid", T1, 0, 2), (4, "paid", T4, 0, 3),
        (5, "created", T0, 0, 1),
    )
    refresh_marts(ch)
    # to_paid: orders 1, 4 -> 3600 s, order 3 -> 7200 s; median 3600.
    # paid_to_delivered: order 1 only -> 7200 s.
    assert funnel(ch) == [("5", "4", "2", "2", "1", "3600", "7200")]


def test_day_without_real_transitions_has_null_durations(ch):
    orders(ch, (1, "delivered", T0, 5, 0))
    history(ch, (1, "delivered", T0, 1, 1))
    refresh_marts(ch)
    assert funnel(ch) == [("1", "1", "1", "1", "0", "NULL", "NULL")]


def test_order_without_history_counts_by_its_current_status(ch):
    # History not arrived yet (topic lag): stg_orders.status alone places the order.
    orders(ch, (1, "shipped", T0, 5, 0))
    refresh_marts(ch)
    assert funnel(ch) == [("1", "1", "1", "0", "0", "NULL", "NULL")]
