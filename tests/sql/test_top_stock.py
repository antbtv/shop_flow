"""Regression tests for the FR-7 marts and the pipeline health mart (ADR-0011).

mart_top_products_daily, mart_inventory_current and mart_pipeline_health are filled by
refreshable MVs: rows go into stg_* as Spark writes them, the MVs are refreshed by hand.
"""

import pytest

T0 = "2026-10-01 09:00:00.000"
T1 = "2026-10-01 10:00:00.000"
T2 = "2026-10-01 11:00:00.000"
T3 = "2026-10-01 12:00:00.000"

TABLES = ("stg_orders", "stg_order_items", "stg_product_versions", "stg_inventory",
          "dim_products", "mart_top_products_daily", "mart_inventory_current",
          "mart_pipeline_health")


@pytest.fixture(autouse=True)
def clean(ch):
    for name in TABLES:
        ch.query(f"TRUNCATE TABLE shopflow.{name}")


def products(ch, *rows):
    """rows: (product_id, valid_from, name, category, version, is_snapshot)."""
    values = ", ".join(
        f"({pid}, '{vf}', '{name}', '{cat}', 10.00, {snap}, 0, '{vf[:23]}', {ver})"
        for pid, vf, name, cat, ver, snap in rows
    )
    ch.query(
        "INSERT INTO shopflow.stg_product_versions (product_id, valid_from, name, category,"
        f" price, is_snapshot, is_deleted, event_time, version) VALUES {values}"
    )
    ch.refresh("dim_products_mv")


def sale(ch, order_id, status, created, lines, version=1):
    """lines: (order_item_id, product_id, quantity, price)."""
    ch.query(
        "INSERT INTO shopflow.stg_orders (order_id, customer_id, status, created_at, updated_at,"
        f" version, is_deleted, event_time) VALUES ({order_id}, 1, '{status}', '{created}',"
        f" '{created[:23]}', {version}, 0, '{created[:23]}')"
    )
    values = ", ".join(
        f"({iid}, {order_id}, {pid}, {qty}, {price}, 1, 0, '{created[:23]}')"
        for iid, pid, qty, price in lines
    )
    ch.query(
        "INSERT INTO shopflow.stg_order_items (order_item_id, order_id, product_id, quantity,"
        f" price_at_order, version, is_deleted, event_time) VALUES {values}"
    )


def top(ch):
    ch.refresh("mart_top_products_daily_mv")
    return ch.rows(
        "SELECT toString(order_date), product_id, category, product_name, quantity, quantity_net,"
        " toDecimalString(revenue, 2), toDecimalString(revenue_net, 2)"
        " FROM shopflow.mart_top_products_daily ORDER BY order_date, product_id, category"
    )


def stock(ch, *rows):
    """rows: (product_id, warehouse_id, quantity, version, is_deleted)."""
    values = ", ".join(
        f"({pid}, {wh}, {qty}, '{T1}', '{T1}', {ver}, {deleted})"
        for pid, wh, qty, ver, deleted in rows
    )
    ch.query(
        "INSERT INTO shopflow.stg_inventory (product_id, warehouse_id, quantity, updated_at,"
        f" event_time, version, is_deleted) VALUES {values}"
    )


def inventory(ch):
    ch.refresh("mart_inventory_current_mv")
    return ch.rows(
        "SELECT product_id, warehouse_id, product_name, category, quantity, is_low"
        " FROM shopflow.mart_inventory_current ORDER BY product_id, warehouse_id"
    )


def test_top_sums_by_day_product_and_skips_cancelled_in_net(ch):
    products(ch, (1, T0, "apple", "food", 100, 1), (2, T0, "pen", "office", 101, 1))
    sale(ch, 1, "paid", T1, [(1, 1, 2, "5.00"), (2, 2, 1, "3.00")])
    sale(ch, 2, "delivered", T2, [(3, 1, 3, "5.00")])
    sale(ch, 3, "cancelled", T3, [(4, 1, 10, "5.00")])
    assert top(ch) == [
        ("2026-10-01", "1", "food", "apple", "15", "5", "75.00", "25.00"),
        ("2026-10-01", "2", "office", "pen", "1", "1", "3.00", "3.00"),
    ]


def test_category_change_inside_a_day_gives_two_rows(ch):
    products(ch, (1, T0, "apple", "food", 100, 1), (1, T2, "apple", "snacks", 200, 0))
    sale(ch, 1, "paid", T1, [(1, 1, 1, "5.00")])
    sale(ch, 2, "paid", T3, [(2, 1, 2, "5.00")])
    got = top(ch)
    assert [(r[2], r[4]) for r in got] == [("food", "1"), ("snacks", "2")]
    assert {r[3] for r in got} == {"apple"}


def test_product_name_is_the_current_one_and_unknown_without_a_version(ch):
    products(ch, (1, T0, "old name", "food", 100, 1), (1, T2, "new name", "food", 200, 0))
    sale(ch, 1, "paid", T1, [(1, 1, 1, "5.00"), (2, 9, 4, "2.00")])
    got = top(ch)
    assert [(r[1], r[2], r[3]) for r in got] == [("1", "food", "new name"),
                                                 ("9", "unknown", "unknown")]


def test_top_is_empty_without_orders(ch):
    assert top(ch) == []


def test_inventory_marks_low_stock_at_the_threshold(ch):
    products(ch, (1, T0, "apple", "food", 100, 1))
    stock(ch, (1, 1, 19, 1, 0), (1, 2, 20, 1, 0), (1, 3, -1, 1, 0))
    assert inventory(ch) == [
        ("1", "1", "apple", "food", "19", "1"),
        ("1", "2", "apple", "food", "20", "0"),
        ("1", "3", "apple", "food", "-1", "1"),
    ]


def test_inventory_uses_the_latest_version_and_hides_deleted_rows(ch):
    products(ch, (1, T0, "apple", "food", 100, 1))
    stock(ch, (1, 1, 5, 1, 0), (1, 2, 50, 1, 0))
    stock(ch, (1, 1, 80, 2, 0), (1, 2, 50, 2, 1))
    assert inventory(ch) == [("1", "1", "apple", "food", "80", "0")]


def test_inventory_of_a_product_without_a_version_is_unknown(ch):
    stock(ch, (7, 1, 100, 1, 0))
    assert inventory(ch) == [("7", "1", "unknown", "unknown", "100", "0")]


def test_inventory_watermark_is_the_newest_event_time(ch):
    products(ch, (1, T0, "apple", "food", 100, 1))
    stock(ch, (1, 1, 5, 1, 0))
    inventory(ch)
    assert ch.rows("SELECT uniqExact(source_watermark), toString(max(source_watermark))"
                   " FROM shopflow.mart_inventory_current") == [("1", T1)]


def health(ch):
    ch.refresh("mart_pipeline_health_mv")
    return ch.rows(
        "SELECT kind, name, ifNull(toString(last_event_time), 'NULL'), ifNull(status, 'NULL'),"
        " ifNull(toString(last_error), 'NULL') FROM shopflow.mart_pipeline_health"
        " ORDER BY kind, name"
    )


def test_health_reports_newest_event_time_per_source(ch):
    sale(ch, 1, "paid", T1, [(1, 1, 1, "5.00")])
    sale(ch, 2, "paid", T3, [(2, 1, 1, "5.00")])
    got = {(r[0], r[1]): r for r in health(ch)}
    assert got[("source", "stg_orders")][2] == T3
    assert got[("source", "stg_order_items")][2] == T3
    # Empty tables are NULL, not 1970-01-01 (that would look like a stuck pipeline of 56 years).
    assert got[("source", "stg_inventory")][2] == "NULL"
    assert got[("source", "raw_events")][2] == "NULL"
    # A source row has no refresh state.
    assert got[("source", "stg_orders")][3] == "NULL"


def test_health_lists_every_refreshable_view_with_its_state(ch):
    ch.refresh("mart_revenue_daily_mv")
    got = {(r[0], r[1]): r for r in health(ch)}
    views = {name for kind, name in got if kind == "view"}
    assert {"dim_products_mv", "mart_revenue_daily_mv", "mart_cohort_retention_mv",
            "mart_top_products_daily_mv", "mart_inventory_current_mv",
            "mart_pipeline_health_mv"} <= views
    row = got[("view", "mart_revenue_daily_mv")]
    assert row[2] == "NULL" and row[3] != "NULL"
    assert row[4] == "NULL"
