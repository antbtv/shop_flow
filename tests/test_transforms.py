"""Tests for spark-jobs/shopflow_stream/transforms.py on a local SparkSession.

Skipped where pyspark is not installed (the Stop hook runs the system python3).
Full run: .venv/bin/python -m pytest -q
"""

import json
import os
import time
from datetime import datetime
from decimal import Decimal

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
    ORDER_ITEMS,
    ORDERS,
    latest_per_key,
    raw_violations,
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


def envelope(op, before=None, after=None, lsn=100, ts_ms=1790448797042):
    return {"before": before, "after": after, "source": {"lsn": lsn, "ts_ms": ts_ms}, "op": op}


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
    df = raw(spark, [({"order_id": 1}, envelope("c", after=order(1)))]).unionByName(
        raw(spark, [({"order_item_id": 1}, envelope("c", after=item(1)))], table="order_items")
    ).unionByName(raw(spark, [(None, envelope("t"))]))
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
