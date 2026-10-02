"""FR-9 data quality checks (airflow/dags/sql/dq/*.sql) on the throwaway ClickHouse.

Clean data gives zero everywhere; each planted problem fires its own check and only it.
Problems inside the 15-minute lag window, and orders older than the order_items rollout, do
not fire.
"""

from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest
from shopflow_checks.dq import load_checks, run_check

clickhouse_connect = pytest.importorskip("clickhouse_connect")

NOW = datetime.now(UTC)
OLD = NOW - timedelta(hours=1)  # settled: outside the lag window
FRESH = NOW - timedelta(minutes=2)  # inside the lag window
CREATED = datetime(2026, 10, 1, 10, tzinfo=UTC)  # after the order_items rollout
BEFORE_ITEMS = datetime(2026, 9, 28, 10, tzinfo=UTC)

TABLES = ("stg_orders", "stg_order_items", "stg_order_status_history", "stg_inventory",
          "stg_product_versions", "stg_customer_versions", "dim_customers", "dim_products",
          "mart_revenue_daily")
CHECKS = {c.name: c for c in load_checks()}


@pytest.fixture
def client(ch):
    for table in TABLES:
        ch.query(f"TRUNCATE TABLE shopflow.{table}")
    url = urlparse(ch.url)
    c = clickhouse_connect.get_client(host=url.hostname, port=url.port, username="admin",
                                      password="test-only", database="shopflow")
    seed(c, ch)
    return c


def insert(client, table, columns, rows):
    client.insert(f"shopflow.{table}", rows, column_names=columns.split())


def order(client, order_id, status="paid", created=CREATED, event=OLD, version=10, deleted=0):
    insert(client, "stg_orders",
           "order_id customer_id status created_at updated_at version is_deleted event_time",
           [[order_id, 1, status, created, created, version, deleted, event]])


def item(client, item_id, order_id, quantity=1, price="9.99", event=OLD, version=10):
    from decimal import Decimal

    insert(client, "stg_order_items",
           "order_item_id order_id product_id quantity price_at_order version is_deleted"
           " event_time", [[item_id, order_id, 1, quantity, Decimal(price), version, 0, event]])


def history(client, order_id, status, at=OLD, version=10):
    insert(client, "stg_order_status_history", "order_id status changed_at is_snapshot version",
           [[order_id, status, at, 0, version]])


def seed(client, ch):
    """A small consistent world: two orders with lines and history, a product, stock."""
    from decimal import Decimal

    insert(client, "stg_product_versions",
           "product_id valid_from name category price is_snapshot is_deleted event_time version",
           [[1, BEFORE_ITEMS, "Tea", "food", Decimal("9.99"), 1, 0, OLD, 1]])
    insert(client, "stg_customer_versions",
           "customer_id valid_from name address segment is_snapshot is_deleted event_time version",
           [[1, BEFORE_ITEMS, "Ann", "Moscow", "retail", 1, 0, OLD, 1]])
    insert(client, "stg_inventory",
           "product_id warehouse_id quantity updated_at event_time version is_deleted",
           [[1, 1, 50, OLD, OLD, 1, 0]])
    for oid in (1, 2):
        order(client, oid)
        item(client, oid * 10, oid)
        history(client, oid, "created", version=5)
        history(client, oid, "paid", version=10)
    # An old order from before the order_items rollout, without lines: not an error.
    order(client, 3, status="created", created=BEFORE_ITEMS)
    history(client, 3, "created")
    ch.refresh("dim_customers_mv")
    ch.refresh("dim_products_mv")
    ch.refresh("mart_revenue_daily_mv")


def results(client):
    ch = lambda sql: client.query(sql).result_rows  # noqa: E731
    return {name: run_check(check, ch) for name, check in CHECKS.items()}


def fired(client):
    return {name: r.details["sample"] for name, r in results(client).items() if r.violations}


def test_every_check_has_a_table_and_a_hint():
    assert len(CHECKS) == 12
    for check in CHECKS.values():
        assert check.table and len(check.hint) > 20, check.name


def test_clean_data_fires_nothing(client):
    assert fired(client) == {}
    assert all(r.status == "ok" and r.details == {} for r in results(client).values())


def test_conflicting_content_for_one_key_and_version(client):
    order(client, 1, status="cancelled")  # same order_id and version 10 as the seed, new content
    insert(client, "stg_order_items",
           "order_item_id order_id product_id quantity price_at_order version is_deleted"
           " event_time", [[10, 1, 1, 7, 9.99, 10, 0, OLD]])
    hits = fired(client)
    assert hits["orders_conflicting_versions"] == ["1"]
    assert hits["order_items_conflicting_versions"] == ["10"]


def test_replay_of_the_same_event_is_not_a_conflict(client):
    order(client, 1)
    item(client, 10, 1)
    assert fired(client) == {}


def test_two_current_versions_in_a_dimension(client):
    insert(client, "dim_customers",
           "customer_id name address segment valid_from valid_to is_current version",
           [[1, "Ann", "Kazan", "vip", CREATED, None, 1, 2]])
    assert fired(client) == {"dim_multiple_current": ["customer:1"]}


def test_negative_amounts_fire_their_checks(client):
    item(client, 99, 2, quantity=-1)
    insert(client, "stg_product_versions",
           "product_id valid_from name category price is_snapshot is_deleted event_time version",
           [[1, CREATED, "Tea", "food", -1, 0, 0, OLD, 2]])
    insert(client, "stg_inventory",
           "product_id warehouse_id quantity updated_at event_time version is_deleted",
           [[1, 2, -5, OLD, OLD, 1, 0]])
    insert(client, "mart_revenue_daily",
           "order_date category orders items revenue revenue_net refreshed_at source_watermark",
           [[CREATED.date(), "food", 1, 1, -10, -10, NOW, NOW]])
    hits = fired(client)
    assert hits["negative_order_items"] == ["99"]
    assert hits["negative_prices"] == ["1"]
    assert hits["negative_inventory"] == ["1:2"]
    assert hits["negative_revenue"] == ["2026-10-01:food"]


def test_orphan_line_fires_only_after_the_lag_window(client):
    item(client, 500, 50)  # order 50 never arrived, line is an hour old
    item(client, 600, 60, event=FRESH)  # order 60 may still be on its way
    assert fired(client) == {"orphan_order_items": ["500"]}


def test_deleted_order_is_not_an_orphan_parent(client):
    order(client, 2, version=20, deleted=1)
    hits = fired(client)
    assert "orphan_order_items" not in hits


def test_order_without_lines_after_the_rollout(client):
    order(client, 70)  # settled, after the rollout, no lines
    order(client, 71, event=FRESH)  # lines may still be on their way
    assert fired(client) == {"orders_without_items": ["70"]}


def test_current_status_differs_from_the_last_transition(client):
    history(client, 2, "delivered", version=30)  # stg_orders still says paid
    assert fired(client) == {"status_differs_from_history": ["2"]}


def test_history_of_an_order_missing_in_stg_orders(client):
    history(client, 80, "created")  # order 80 quarantined for stg_orders (4.9)
    history(client, 81, "created", at=FRESH)
    assert fired(client) == {"history_without_order": ["80"]}
