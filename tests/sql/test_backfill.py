"""backfill_from_raw.py end to end (3.8, ADR-0009): raw_events -> stg_* and journals via Spark.

Runs the job in the shopflow-spark image against the throwaway ClickHouse, with spark-jobs/
mounted from the working tree (the image may hold an older copy). Opt-in, about a minute:
    SHOPFLOW_SPARK_TESTS=1 .venv/bin/python -m pytest -q tests/sql/test_backfill.py
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPARK_IMAGE = "shopflow-spark:latest"
SPARK_PASSWORD = "spark-test-only"

pytestmark = pytest.mark.skipif(
    os.environ.get("SHOPFLOW_SPARK_TESTS") != "1", reason="set SHOPFLOW_SPARK_TESTS=1"
)

TABLES = ("raw_events", "stg_orders", "stg_order_items", "stg_inventory",
          "stg_customer_versions", "stg_product_versions", "stg_order_status_history")

# 2026-10-01 10:00, 11:00, 12:00 UTC and 2026-10-02 10:00 UTC, in ms.
T1, T2, T3, NEXT_DAY = 1790848800000, 1790852400000, 1790856000000, 1790935200000


def ts(ms: int) -> str:
    """Debezium ISO timestamp (time.precision.mode=adaptive_time_microseconds, ADR-0005)."""
    from datetime import UTC, datetime

    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def order(order_id, status, ms, customer_id=1):
    return {"order_id": order_id, "customer_id": customer_id, "status": status,
            "created_at": ts(T1), "updated_at": ts(ms)}


def customer(address, ms):
    return {"customer_id": 1, "name": "Ann", "email": "ann@example.com", "address": address,
            "segment": "retail", "updated_at": ts(ms)}


def event(table, offset, key, op, ms, lsn, before=None, after=None):
    payload = {"before": before, "after": after, "op": op,
               "source": {"lsn": lsn, "ts_ms": ms, "ts_us": ms * 1000 + offset}}
    return (f"cdc.public.{table}", offset, lsn, json.dumps(key), ms, op, json.dumps(payload))


EVENTS = [
    # order 1: snapshot as created, then paid
    event("orders", 0, {"order_id": 1}, "r", T1, 100, after=order(1, "created", T1)),
    event("orders", 1, {"order_id": 1}, "u", T2, 200,
          before=order(1, "created", T1), after=order(1, "paid", T2)),
    # order 2: created, then deleted (tombstone row in stg, hidden by FINAL)
    event("orders", 2, {"order_id": 2}, "c", T1, 110, after=order(2, "created", T1)),
    event("orders", 3, {"order_id": 2}, "d", T3, 300, before=order(2, "created", T1)),
    # order 3: NULL in a NOT NULL column -> quarantine, stays only in raw_events
    event("orders", 4, {"order_id": 3}, "c", T2, 210, after=order(3, "created", T2, None)),
    # order 4: the next day, outside --to
    event("orders", 5, {"order_id": 4}, "c", NEXT_DAY, 400, after=order(4, "created", NEXT_DAY)),
    # customer 1: snapshot, then a new address
    event("customers", 0, {"customer_id": 1}, "r", T1, 120, after=customer("Moscow", T1)),
    event("customers", 1, {"customer_id": 1}, "u", T3, 310,
          before=customer("Moscow", T1), after=customer("Kazan", T3)),
]


@pytest.fixture
def spark_ch(ch):
    if subprocess.run(["docker", "image", "inspect", SPARK_IMAGE],
                      capture_output=True).returncode != 0:
        pytest.skip(f"image {SPARK_IMAGE} not available")
    # The writer as on Pi5: create-ch-users.sh with this network as its only allowed subnet.
    env = {**os.environ, "CLICKHOUSE_URL": ch.url, "CLICKHOUSE_USER": "admin",
           "CLICKHOUSE_PASSWORD": "test-only", "CLICKHOUSE_SPARK_PASSWORD": SPARK_PASSWORD,
           "LAN_SUBNET": ch.subnet, "PI5_COMPOSE_SUBNET": ch.subnet,
           "CLICKHOUSE_AIRFLOW_PASSWORD": "airflow-test-only"}
    subprocess.run([str(ROOT / "scripts/create-ch-users.sh")], cwd=ROOT, env=env,
                   check=True, capture_output=True)
    for name in TABLES:
        ch.query(f"TRUNCATE TABLE shopflow.{name}")
    values = ", ".join(
        f"('{topic}', 0, {off}, {lsn}, '{key}', fromUnixTimestamp64Milli({ms}, 'UTC'), '{op}',"
        f" '{payload}')"
        for topic, off, lsn, key, ms, op, payload in EVENTS
    )
    ch.query("INSERT INTO shopflow.raw_events (topic, kafka_partition, kafka_offset, source_lsn,"
             f" event_key, event_time, op, payload) VALUES {values}")
    return ch


def backfill(ch, *args):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", ch.network,
         "-e", f"PI5_HOST={ch.container}", "-e", f"CLICKHOUSE_SPARK_PASSWORD={SPARK_PASSWORD}",
         "-e", "TZ=UTC", "-v", f"{ROOT}/spark-jobs:/opt/shopflow:ro",
         "--entrypoint", "/opt/spark/bin/spark-submit", SPARK_IMAGE,
         "/opt/shopflow/backfill_from_raw.py", *args],
        capture_output=True, text=True, timeout=600,
    )


def snapshot_of_targets(ch):
    return {
        "orders": ch.rows("SELECT order_id, status FROM shopflow.stg_orders FINAL ORDER BY 1"),
        "history": ch.rows("SELECT order_id, status, is_snapshot"
                           " FROM shopflow.stg_order_status_history FINAL ORDER BY 1, version"),
        "customers": ch.rows("SELECT address, is_snapshot, toString(valid_from)"
                             " FROM shopflow.stg_customer_versions FINAL ORDER BY version"),
    }


def test_backfill_writes_one_day_like_the_stream_and_is_repeatable(spark_ch):
    ch = spark_ch
    first = backfill(ch, "--from", "2026-10-01", "--to", "2026-10-01")
    assert first.returncode == 0, first.stdout[-2000:] + first.stderr[-2000:]
    assert "day=2026-10-01 raw_rows=7" in first.stdout

    after_first = snapshot_of_targets(ch)
    assert after_first == {
        # 2 deleted (tombstone hidden by FINAL), 3 quarantined, 4 is outside the range
        "orders": [("1", "paid")],
        # Quarantine is per target (ADR-0008): order 3 misses customer_id, which stg_orders
        # needs and the history does not, so its transition is kept. The funnel starts from
        # stg_orders and LEFT JOINs the history, so it does not count order 3.
        "history": [("1", "created", "1"), ("1", "paid", "0"), ("2", "created", "0"),
                    ("3", "created", "0")],
        "customers": [
            ("Moscow", "1", "2026-10-01 10:00:00.000000"),
            ("Kazan", "0", "2026-10-01 12:00:00.000001"),
        ],
    }
    # The quarantined event stays in raw_events for a later reload.
    assert ch.rows("SELECT count() FROM shopflow.raw_events FINAL"
                   " WHERE event_key = '{\"order_id\": 3}'") == [("1",)]

    second = backfill(ch, "--from", "2026-10-01", "--to", "2026-10-01")
    assert second.returncode == 0, second.stdout[-2000:] + second.stderr[-2000:]
    assert snapshot_of_targets(ch) == after_first

    # The journal feeds SCD2 as the stream's does: snapshot version opens at 1970.
    ch.refresh("dim_customers_mv")
    assert ch.rows("SELECT address, toString(valid_from), is_current FROM shopflow.dim_customers"
                   " ORDER BY version") == [
        ("Moscow", "1970-01-01 00:00:00.000000", "0"),
        ("Kazan", "2026-10-01 12:00:00.000001", "1"),
    ]
