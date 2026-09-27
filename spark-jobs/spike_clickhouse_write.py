"""Spike for task 2.4: write through the ClickHouse connector and record how it behaves.

Run: docker compose -f docker-compose.laptop.yml --profile spark run --rm --no-deps spark \
         /opt/spark/bin/spark-submit /opt/shopflow/spike_clickhouse_write.py <case>
Cases:
  raw          3 rows into raw_events, ingested_at omitted (server DEFAULT)
  raw_ingested 1 row into raw_events with ingested_at set in Spark
  stg          2 rows into stg_order_items: Decimal, negative Int32, UInt64 from LongType
  null_uint    null into a non-Nullable UInt64 column (raw_events.source_lsn)
  negative_uint -1 into a UInt64 column (raw_events.kafka_offset)
All rows use topic 'test.spike' or order_item_id >= 900000000; clean them up afterwards.
"""

import sys
import time

from pyspark.sql import SparkSession
from shopflow_stream.sink import catalog_conf, table

RAW_COLUMNS = (
    "topic, CAST(kafka_partition AS INT) kafka_partition, "
    "CAST(kafka_offset AS BIGINT) kafka_offset, CAST(source_lsn AS BIGINT) source_lsn, event_key, "
    "to_timestamp(event_time_iso) event_time, op, payload"
)


def raw_rows(spark, rows):
    values = ", ".join(rows)
    return spark.sql(
        f"SELECT {RAW_COLUMNS} FROM VALUES {values} "
        "AS t(topic, kafka_partition, kafka_offset, source_lsn, event_key, event_time_iso, op, "
        "payload)"
    )


def main(case: str) -> None:
    builder = SparkSession.builder.appName(f"shopflow-spike-{case}")
    for key, value in catalog_conf().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if case == "raw":
        df = raw_rows(spark, [
            "('test.spike', 0, 0, 100, '{\"id\":1}', '2026-09-27T12:34:56.123456Z', 'c', '{}')",
            "('test.spike', 0, 1, 101, '{\"id\":2}', '2026-09-27T23:59:59.999Z', 'u', '{}')",
            # LSN above 2^53: must survive without float rounding.
            "('test.spike', 0, 2, 9007199254740993, '{\"id\":3}', '2026-09-28T00:00:00Z', "
            "'d', '{}')",
        ])
    elif case == "raw_ingested":
        df = raw_rows(spark, [
            "('test.spike', 0, 3, 103, '{\"id\":4}', '2026-09-27T12:00:00.5Z', 'c', '{}')",
        ]).selectExpr("*", "current_timestamp() AS ingested_at")
    elif case == "stg":
        df = spark.sql(
            "SELECT CAST(id AS BIGINT) order_item_id, CAST(1 AS BIGINT) order_id, "
            "CAST(2 AS BIGINT) product_id, CAST(q AS INT) quantity, "
            "CAST(p AS DECIMAL(10,2)) price_at_order, CAST(v AS BIGINT) version, "
            "CAST(0 AS TINYINT) is_deleted "
            "FROM VALUES (900000001, 3, '19.99', 10), (900000002, -2, '0.01', 11) AS t(id, q, p, v)"
        )
    elif case == "null_uint":
        df = raw_rows(spark, [
            "('test.spike', 0, 10, NULL, '{\"id\":10}', '2026-09-27T12:00:00Z', 'c', '{}')",
        ])
    elif case == "negative_uint":
        df = raw_rows(spark, [
            "('test.spike', 0, -1, 110, '{\"id\":11}', '2026-09-27T12:00:00Z', 'c', '{}')",
        ])
    else:
        raise SystemExit(f"unknown case {case}")

    target = "stg_order_items" if case == "stg" else "raw_events"
    df.printSchema()
    started = time.monotonic()
    try:
        df.writeTo(table(target)).append()
        print(f"SPIKE {case}: OK in {time.monotonic() - started:.1f}s")
    except Exception as exc:  # noqa: BLE001 - the spike records any failure
        first = " / ".join(line.strip() for line in str(exc).splitlines()[:6])[:900]
        print(f"SPIKE {case}: FAILED after {time.monotonic() - started:.1f}s: {first}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
