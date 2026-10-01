"""Tests for spark-jobs/shopflow_stream/transforms.py on a local SparkSession.

Skipped where pyspark is not installed (the Stop hook runs the system python3).
Full run: .venv/bin/python -m pytest -q
"""

import json
import os
import re
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from shopflow_stream.transforms import (  # noqa: E402
    CUSTOMER_VERSIONS,
    INVENTORY,
    ORDER_ITEMS,
    ORDERS,
    PRODUCT_VERSIONS,
    STATUS_HISTORY_COLUMNS,
    STATUS_HISTORY_TABLE,
    STG_SPECS,
    latest_per_key,
    latest_status_rows,
    raw_violations,
    status_history_rows,
    stg_rows,
    to_raw_events,
)

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
    ]
)

# Real UPDATE from transaction 773 (task 1.5), trimmed to the fields Debezium always sends.
ORDER_UPDATE = {
    "before": {
        "order_id": 1,
        "customer_id": 1,
        "status": "paid",
        "created_at": "2026-09-26T18:53:17.039686Z",
        "updated_at": "2026-09-26T18:53:17.041478Z",
    },
    "after": {
        "order_id": 1,
        "customer_id": 1,
        "status": "shipped",
        "created_at": "2026-09-26T18:53:17.039686Z",
        "updated_at": "2026-09-26T18:53:17.042351Z",
    },
    "source": {"ts_ms": 1790448797042, "ts_us": 1790448797042638, "lsn": 26741552, "txId": 773},
    "op": "u",
}


@pytest.fixture(scope="module")
def spark():
    # collect() converts timestamps to the local zone of the Python process: pin it to UTC.
    os.environ["TZ"] = "UTC"
    time.tzset()
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-transforms")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def envelope(op, before=None, after=None, lsn=100, ts_ms=1790448797042, ts_us=None):
    source = {"lsn": lsn, "ts_ms": ts_ms, "ts_us": ts_ms * 1000 if ts_us is None else ts_us}
    return {"before": before, "after": after, "source": source, "op": op}


def order(order_id=1, customer_id=1, status="created", **overrides):
    row = {
        "order_id": order_id,
        "customer_id": customer_id,
        "status": status,
        "created_at": "2026-09-26T18:53:17.039686Z",
        "updated_at": "2026-09-26T18:53:17.039686Z",
    }
    return row | overrides


def item(order_item_id=1, quantity=2, price="19.99", **overrides):
    row = {
        "order_item_id": order_item_id,
        "order_id": 1,
        "product_id": 1,
        "quantity": quantity,
        "price_at_order": price,
    }
    return row | overrides


def kafka_df(spark, messages, table="orders"):
    """messages: (key dict | None, value dict | str | None) in offset order."""
    rows = []
    for offset, (key, value) in enumerate(messages):
        if isinstance(value, dict):
            value = json.dumps(value)
        rows.append(
            (
                None if key is None else json.dumps(key).encode(),
                None if value is None else value.encode(),
                f"cdc.public.{table}",
                0,
                offset,
            )
        )
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def raw(spark, messages, table="orders"):
    return to_raw_events(kafka_df(spark, messages, table))


def by_pk(df, pk):
    return {row[pk]: row.asDict() for row in df.collect()}


def test_raw_events_keep_position_payload_and_contract_fields(spark):
    df = kafka_df(spark, [({"order_id": 1}, ORDER_UPDATE)])
    row = to_raw_events(df).collect()[0]
    assert (row.topic, row.kafka_partition, row.kafka_offset) == ("cdc.public.orders", 0, 0)
    assert row.source_lsn == 26741552
    assert row.op == "u"
    assert row.event_time == datetime(2026, 9, 26, 18, 53, 17, 42000)
    assert row.event_key == '{"order_id": 1}'
    assert json.loads(row.payload) == ORDER_UPDATE
    assert row.ingested_at is not None


def test_raw_violations_catch_contract_breaks_only(spark):
    no_lsn = envelope("c", after=order(2))
    del no_lsn["source"]["lsn"]
    df = raw(
        spark,
        [
            ({"order_id": 1}, envelope("c", after=order(1))),
            ({"order_id": 2}, no_lsn),
            ({"order_id": 3}, None),  # tombstone: disabled by contract
            ({"order_id": 4}, "not json"),
            ({"order_id": 5}, envelope(None, after=order(5))),
            ({"order_id": 6}, envelope("t")),  # truncate: valid raw event, ignored by stg
        ],
    )
    assert sorted(r.kafka_offset for r in raw_violations(df).collect()) == [1, 2, 3, 4]


def test_stg_create_update_snapshot_take_after(spark):
    df = raw(
        spark,
        [
            ({"order_id": 1}, envelope("c", after=order(1), lsn=10)),
            ({"order_id": 1}, ORDER_UPDATE),
            ({"order_id": 2}, envelope("r", after=order(2, status="paid"), lsn=5)),
        ],
    )
    rows = stg_rows(df, ORDERS).collect()
    assert [r.quarantine_reason for r in rows] == [None, None, None]
    update = rows[1]
    assert (update.order_id, update.status, update.version, update.is_deleted) == (
        1,
        "shipped",
        26741552,
        0,
    )
    assert update.updated_at == datetime(2026, 9, 26, 18, 53, 17, 42351)
    assert rows[2].status == "paid"


def test_stg_delete_takes_pk_from_key_and_defaults_when_before_is_missing(spark):
    df = raw(
        spark,
        [
            ({"order_id": 7}, envelope("d", before=order(7, status="cancelled"), lsn=20)),
            ({"order_id": 8}, envelope("d", before=None, lsn=21)),  # REPLICA IDENTITY DEFAULT
        ],
    )
    rows = by_pk(stg_rows(df, ORDERS), "order_id")
    assert rows[7]["is_deleted"] == 1 and rows[7]["status"] == "cancelled"
    assert rows[8]["is_deleted"] == 1 and rows[8]["quarantine_reason"] is None
    assert rows[8]["customer_id"] == 0 and rows[8]["status"] == ""
    assert rows[8]["created_at"] == datetime(1970, 1, 1)


def test_stg_order_items_types(spark):
    df = raw(
        spark,
        [({"order_item_id": 1}, envelope("c", after=item(1, quantity=-2, price="0.01")))],
        table="order_items",
    )
    row = stg_rows(df, ORDER_ITEMS).collect()[0]
    assert row.quarantine_reason is None
    assert (row.quantity, row.price_at_order) == (-2, Decimal("0.01"))


def test_stg_ignores_other_topics_and_non_row_ops(spark):
    df = (
        raw(spark, [({"order_id": 1}, envelope("c", after=order(1)))])
        .unionByName(
            raw(spark, [({"order_item_id": 1}, envelope("c", after=item(1)))], table="order_items")
        )
        .unionByName(raw(spark, [(None, envelope("t"))]))
    )
    assert [r.order_id for r in stg_rows(df, ORDERS).collect()] == [1]


@pytest.mark.parametrize(
    ("key", "after", "lsn", "reason"),
    [
        ({"order_id": None}, order(1), 1, "bad order_id"),
        ({"order_id": -1}, order(1), 1, "bad order_id"),
        ({"order_id": 1}, order(1), None, "bad version"),
        ({"order_id": 1}, None, 1, "no after"),
        ({"order_id": 1}, order(1, customer_id=None), 1, "bad customer_id"),
        ({"order_id": 1}, order(1, customer_id=-5), 1, "bad customer_id"),
        ({"order_id": 1}, order(1, created_at="yesterday"), 1, "bad created_at"),
        ({"order_id": 1}, order(1, status=None), 1, "bad status"),
    ],
)
def test_stg_quarantines_rows_that_do_not_fit_clickhouse_types(spark, key, after, lsn, reason):
    df = raw(spark, [(key, envelope("u", after=after, lsn=lsn))])
    assert stg_rows(df, ORDERS).collect()[0].quarantine_reason == reason


@pytest.mark.parametrize(
    ("after", "reason"),
    [
        (item(1, price="abc"), "bad price_at_order"),
        (item(1, price="123456789.99"), "bad price_at_order"),  # overflows Decimal(10,2)
        (item(1, quantity=2**31), "bad quantity"),
        (item(1, product_id=None), "bad product_id"),
    ],
)
def test_stg_quarantines_bad_order_items(spark, after, reason):
    df = raw(spark, [({"order_item_id": 1}, envelope("c", after=after))], table="order_items")
    assert stg_rows(df, ORDER_ITEMS).collect()[0].quarantine_reason == reason


def test_latest_per_key_two_updates_in_one_transaction(spark):
    # Transaction 773: two UPDATEs of one row, LSN 26741552 < 26741704 (task 1.5).
    second = json.loads(json.dumps(ORDER_UPDATE))
    second["source"]["lsn"] = 26741704
    second["after"]["status"] = "delivered"
    df = raw(spark, [({"order_id": 1}, second), ({"order_id": 1}, ORDER_UPDATE)])
    rows = latest_per_key(stg_rows(df, ORDERS), ORDERS).collect()
    assert len(rows) == 1
    assert (rows[0].status, rows[0].version) == ("delivered", 26741704)
    assert rows[0].asDict().keys() == set(ORDERS.columns)


def test_latest_per_key_debezium_replay_is_deterministic(spark):
    # After a Connect crash the same change arrives again with the same LSN, later offset.
    df = raw(
        spark,
        [
            ({"order_id": 1}, envelope("u", after=order(1, status="paid"), lsn=50)),
            ({"order_id": 1}, envelope("u", after=order(1, status="PAID-REPLAY"), lsn=50)),
            ({"order_id": 2}, envelope("c", after=order(2), lsn=40)),
            ({"order_id": 2}, envelope("d", before=order(2), lsn=41)),
        ],
    )
    rows = by_pk(latest_per_key(stg_rows(df, ORDERS), ORDERS), "order_id")
    assert rows[1]["status"] == "PAID-REPLAY"
    assert rows[2]["is_deleted"] == 1 and rows[2]["version"] == 41


def test_quarantined_newest_version_leaves_last_valid_one(spark):
    # Dedup runs after quarantine (ADR-0008): stg keeps the last valid state of the key,
    # the newer bad event stays in raw_events for a reload.
    df = raw(
        spark,
        [
            ({"order_id": 1}, envelope("u", after=order(1, status="paid"), lsn=60)),
            ({"order_id": 1}, envelope("u", after=order(1, customer_id=None), lsn=61)),
        ],
    )
    rows = stg_rows(df, ORDERS)
    valid = rows.where(rows.quarantine_reason.isNull())
    latest = latest_per_key(valid, ORDERS).collect()
    assert [(r.status, r.version) for r in latest] == [("paid", 60)]
    assert rows.where(rows.quarantine_reason.isNotNull()).count() == 1


# --- M3: event_time, composite keys, SCD2 journals, status history (ADR-0009) ---

DDL = Path(__file__).resolve().parents[1] / "clickhouse" / "ddl"


def ddl_columns(table):
    """Columns of shopflow.<table> after all migrations: CREATE TABLE plus ADD COLUMN."""
    columns = []
    for path in sorted(DDL.glob("[0-9][0-9][0-9]_*.sql")):
        sql = re.sub(r"--[^\n]*", "", path.read_text())
        create = re.search(
            rf"CREATE TABLE IF NOT EXISTS shopflow\.{table}\s*\((.*?)\)\s*ENGINE", sql, re.S
        )
        if create:
            columns += re.findall(r"^\s*(\w+)\s+\S", create.group(1), re.M)
        if re.search(rf"ALTER TABLE shopflow\.{table}\b", sql):
            columns += re.findall(r"ADD COLUMN IF NOT EXISTS (\w+)", sql)
    return columns


@pytest.mark.parametrize("spec", STG_SPECS, ids=lambda s: s.target_table)
def test_spec_columns_match_clickhouse_ddl(spec):
    # The connector appends by name: a column missing in Spark is silently DEFAULT, an extra
    # one fails the batch. Keep the specs and the DDL in step.
    assert set(spec.columns) == set(ddl_columns(spec.target_table))


def test_status_history_columns_match_clickhouse_ddl():
    assert set(STATUS_HISTORY_COLUMNS) == set(ddl_columns(STATUS_HISTORY_TABLE))


def test_stg_event_time_is_source_ts_ms(spark):
    df = raw(spark, [({"order_id": 1}, ORDER_UPDATE)])
    row = stg_rows(df, ORDERS).collect()[0]
    assert row.event_time == datetime(2026, 9, 26, 18, 53, 17, 42000)


def test_stg_created_at_keeps_microseconds(spark):
    # ASOF JOIN to dim_products needs microseconds (ADR-0009, stand 3.5).
    df = raw(spark, [({"order_id": 1}, envelope("c", after=order(1)))])
    assert stg_rows(df, ORDERS).collect()[0].created_at == datetime(2026, 9, 26, 18, 53, 17, 39686)


def stock(product_id=1, warehouse_id=1, quantity=10):
    return {
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "quantity": quantity,
        "updated_at": "2026-09-29T18:16:05.123456Z",
    }


def test_inventory_composite_key(spark):
    df = raw(
        spark,
        [
            ({"product_id": 1, "warehouse_id": 1}, envelope("c", after=stock(1, 1, 10), lsn=1)),
            ({"product_id": 1, "warehouse_id": 2}, envelope("c", after=stock(1, 2, 20), lsn=2)),
            ({"product_id": 1, "warehouse_id": 1}, envelope("u", after=stock(1, 1, 7), lsn=3)),
            ({"product_id": 1, "warehouse_id": 2}, envelope("d", before=None, lsn=4)),
        ],
        table="inventory",
    )
    rows = stg_rows(df, INVENTORY)
    assert rows.where(rows.quarantine_reason.isNotNull()).count() == 0
    latest = {
        (r.product_id, r.warehouse_id): r.asDict()
        for r in latest_per_key(rows, INVENTORY).collect()
    }
    assert latest[(1, 1)]["quantity"] == 7 and latest[(1, 1)]["version"] == 3
    assert latest[(1, 2)]["is_deleted"] == 1 and latest[(1, 2)]["version"] == 4
    assert set(latest[(1, 1)]) == set(INVENTORY.columns)


@pytest.mark.parametrize(
    ("key", "reason"),
    [
        ({"product_id": 1, "warehouse_id": -1}, "bad warehouse_id"),
        ({"product_id": 1, "warehouse_id": 2**31}, "bad warehouse_id"),
        ({"product_id": 1}, "bad warehouse_id"),
        ({"product_id": -1, "warehouse_id": 1}, "bad product_id"),
    ],
)
def test_inventory_quarantines_bad_keys(spark, key, reason):
    df = raw(spark, [(key, envelope("c", after=stock()))], table="inventory")
    assert stg_rows(df, INVENTORY).collect()[0].quarantine_reason == reason


def customer(customer_id=1, address="Kazan, Mira st., 1", segment="vip", **overrides):
    row = {
        "customer_id": customer_id,
        "name": "Anna Ivanova",
        "email": "anna@example.com",
        "address": address,
        "segment": segment,
        "updated_at": "2026-09-26T18:53:17.039686Z",
    }
    return row | overrides


T0 = 1790448797042638  # source.ts_us, microseconds


def test_journal_valid_from_is_ts_us_and_snapshot_flag(spark):
    df = raw(
        spark,
        [
            ({"customer_id": 1}, envelope("r", after=customer(1), lsn=10, ts_us=T0)),
            ({"customer_id": 2}, envelope("c", after=customer(2), lsn=11, ts_us=T0 + 1)),
        ],
        table="customers",
    )
    rows = by_pk(stg_rows(df, CUSTOMER_VERSIONS), "customer_id")
    assert rows[1]["valid_from"] == datetime(2026, 9, 26, 18, 53, 17, 42638)
    assert (rows[1]["is_snapshot"], rows[2]["is_snapshot"]) == (1, 0)
    assert rows[2]["valid_from"] == datetime(2026, 9, 26, 18, 53, 17, 42639)
    assert "email" not in rows[1]


def test_journal_keeps_history_and_collapses_one_transaction(spark):
    # Three transactions of one customer in one batch; the second has two UPDATEs with one
    # ts_us. Grouping by PK alone would keep one row (architect review, ADR-0009).
    df = raw(
        spark,
        [
            ({"customer_id": 1}, envelope("c", after=customer(1, address="A"), lsn=100, ts_us=T0)),
            (
                {"customer_id": 1},
                envelope("u", after=customer(1, address="C"), lsn=300, ts_us=T0 + 2),
            ),
            (
                {"customer_id": 1},
                envelope("u", after=customer(1, address="D"), lsn=310, ts_us=T0 + 2),
            ),
            (
                {"customer_id": 1},
                envelope("d", before=customer(1, address="D"), lsn=500, ts_us=T0 + 4),
            ),
        ],
        table="customers",
    )
    rows = latest_per_key(stg_rows(df, CUSTOMER_VERSIONS), CUSTOMER_VERSIONS).orderBy("valid_from")
    got = [(r.address, r.version, r.is_deleted) for r in rows.collect()]
    assert got == [("A", 100, 0), ("D", 310, 0), ("D", 500, 1)]


def test_journal_debezium_replay_is_deterministic(spark):
    df = raw(
        spark,
        [
            ({"customer_id": 1}, envelope("u", after=customer(1, segment="new"), lsn=50, ts_us=T0)),
            (
                {"customer_id": 1},
                envelope("u", after=customer(1, segment="REPLAY"), lsn=50, ts_us=T0),
            ),
        ],
        table="customers",
    )
    rows = latest_per_key(stg_rows(df, CUSTOMER_VERSIONS), CUSTOMER_VERSIONS).collect()
    assert [(r.segment, r.version) for r in rows] == [("REPLAY", 50)]


def test_journal_nullable_attributes_become_empty_not_quarantine(spark):
    df = raw(
        spark,
        [({"customer_id": 1}, envelope("c", after=customer(1, address=None, segment=None)))],
        table="customers",
    )
    row = stg_rows(df, CUSTOMER_VERSIONS).collect()[0]
    assert row.quarantine_reason is None
    assert (row.address, row.segment) == ("", "")


def test_journal_quarantines_missing_ts_us_and_bad_price(spark):
    no_ts = envelope("u", after=customer(1), lsn=5)
    del no_ts["source"]["ts_us"]
    df = raw(spark, [({"customer_id": 1}, no_ts)], table="customers")
    assert stg_rows(df, CUSTOMER_VERSIONS).collect()[0].quarantine_reason == "bad valid_from"

    product = {"product_id": 1, "name": "Nord Kettle 1", "category": "home", "price": "abc"}
    df = raw(spark, [({"product_id": 1}, envelope("u", after=product))], table="products")
    assert stg_rows(df, PRODUCT_VERSIONS).collect()[0].quarantine_reason == "bad price"


def test_product_journal_price_and_category(spark):
    product = {"product_id": 7, "name": "Nord Kettle 1", "category": "home", "price": "4713.21"}
    df = raw(spark, [({"product_id": 7}, envelope("u", after=product, lsn=9))], table="products")
    row = latest_per_key(stg_rows(df, PRODUCT_VERSIONS), PRODUCT_VERSIONS).collect()[0]
    assert (row.category, row.price, row.version) == ("home", Decimal("4713.21"), 9)
    assert set(row.asDict()) == set(PRODUCT_VERSIONS.columns)


def history(spark, messages):
    rows = status_history_rows(raw(spark, messages))
    return rows


def test_status_history_rows_only_for_status_changes(spark):
    rows = history(
        spark,
        [
            ({"order_id": 1}, envelope("c", after=order(1), lsn=10)),
            (
                {"order_id": 1},
                envelope("u", before=order(1), after=order(1, status="paid"), lsn=20),
            ),
            # UPDATE of another column: no transition
            (
                {"order_id": 1},
                envelope(
                    "u",
                    before=order(1, status="paid"),
                    after=order(1, status="paid", customer_id=2),
                    lsn=30,
                ),
            ),
            ({"order_id": 1}, envelope("d", before=order(1, status="paid"), lsn=40)),
            ({"order_id": 2}, envelope("r", after=order(2, status="delivered"), lsn=5)),
            # before missing (REPLICA IDENTITY DEFAULT): keep, the mart takes the first time
            ({"order_id": 3}, envelope("u", before=None, after=order(3, status="shipped"), lsn=50)),
        ],
    ).collect()
    got = sorted((r.order_id, r.status, r.version, r.is_snapshot) for r in rows)
    assert got == [
        (1, "created", 10, 0),
        (1, "paid", 20, 0),
        (2, "delivered", 5, 1),
        (3, "shipped", 50, 0),
    ]
    assert all(r.quarantine_reason is None for r in rows)
    assert rows[0].changed_at == datetime(2026, 9, 26, 18, 53, 17, 42000)


def test_status_history_replay_collapses_repeated_snapshot_stays(spark):
    rows = history(
        spark,
        [
            (
                {"order_id": 1},
                envelope("u", before=order(1), after=order(1, status="paid"), lsn=20),
            ),
            (
                {"order_id": 1},
                envelope("u", before=order(1), after=order(1, status="paid"), lsn=20),
            ),
            ({"order_id": 1}, envelope("r", after=order(1, status="paid"), lsn=900)),
        ],
    )
    latest = latest_status_rows(rows).collect()
    assert sorted((r.status, r.version, r.is_snapshot) for r in latest) == [
        ("paid", 20, 0),
        ("paid", 900, 1),
    ]
    assert set(latest[0].asDict()) == set(STATUS_HISTORY_COLUMNS)


def test_status_history_quarantine_and_other_topics(spark):
    df = raw(
        spark,
        [
            ({"order_id": 1}, envelope("c", after=order(1, status=None), lsn=10)),
            ({"order_id": -5}, envelope("c", after=order(-5), lsn=11)),
        ],
    ).unionByName(
        raw(spark, [({"order_item_id": 1}, envelope("c", after=item(1)))], table="order_items")
    )
    rows = sorted(status_history_rows(df).collect(), key=lambda r: r.kafka_offset)
    assert [r.quarantine_reason for r in rows] == ["bad status", "bad order_id"]
