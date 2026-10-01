"""Backfill staging tables from raw_events (ADR-0009): history that the stream did not write.

Reads raw_events on Pi5 through the ClickHouse catalog, day by day, and runs every day through
the same parsing, quarantine and dedup as the streaming job (write_stg, write_status_history).
Safe to repeat and to run next to the stream: every target is a ReplacingMergeTree keyed by
the ADR-0006/0009 keys. Covers what raw_events still holds (30 days, NFR-5).

Also the reload path after a quarantine fix (ADR-0008). Run with the job's image and env:
    docker compose -f docker-compose.laptop.yml run --rm --no-deps \\
        --entrypoint /opt/spark/bin/spark-submit spark /opt/shopflow/backfill_from_raw.py \\
        --from 2026-09-26 --to 2026-09-30 [--tables stg_inventory,stg_customer_versions]
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from shopflow_stream.sink import catalog_conf, table
from shopflow_stream.transforms import STATUS_HISTORY_TABLE, STG_SPECS, TOPIC_PREFIX
from streaming_to_clickhouse import write_status_history, write_stg

log = logging.getLogger("shopflow.backfill")

TARGETS = [spec.target_table for spec in STG_SPECS] + [STATUS_HISTORY_TABLE]
RAW_COLUMNS = ("topic", "kafka_offset", "event_key", "event_time", "op", "payload")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    today = datetime.now(timezone.utc).date()
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--from", dest="start", type=date.fromisoformat, default=today - timedelta(30))
    p.add_argument("--to", dest="end", type=date.fromisoformat, default=today)
    p.add_argument(
        "--tables",
        type=lambda s: s.split(","),
        default=TARGETS,
        help="comma-separated targets, default all: " + ",".join(TARGETS),
    )
    args = p.parse_args(argv)
    unknown = set(args.tables) - set(TARGETS)
    if unknown:
        p.error(f"unknown tables: {sorted(unknown)}")
    if args.start > args.end:
        p.error("--from is after --to")
    return args


def raw_day(spark: SparkSession, day: date, topics: list[str]) -> DataFrame:
    """raw_events of one UTC day (one partition, toYYYYMMDD(event_time)) for the topics.
    Duplicates of Spark replays are harmless: the dedup below and ClickHouse collapse them."""
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return (
        spark.table(table("raw_events"))
        .where(F.col("topic").isin(*topics))
        .where((F.col("event_time") >= start) & (F.col("event_time") < start + timedelta(1)))
        .select(*RAW_COLUMNS)
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    args = parse_args(argv)
    specs = [s for s in STG_SPECS if s.target_table in args.tables]
    history = STATUS_HISTORY_TABLE in args.tables
    topics = sorted({s.topic for s in specs} | ({TOPIC_PREFIX + "orders"} if history else set()))

    builder = SparkSession.builder.appName("shopflow-backfill-from-raw")
    for key, value in catalog_conf().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("backfill %s..%s tables=%s", args.start, args.end, ",".join(args.tables))

    day = args.start
    while day <= args.end:
        # batch_id in the quarantine log: the day as YYYYMMDD.
        batch_id = int(day.strftime("%Y%m%d"))
        raw = raw_day(spark, day, topics).persist()
        try:
            rows = raw.count()
            if rows:
                for spec in specs:
                    write_stg(raw, spec, batch_id)
                if history:
                    write_status_history(raw, batch_id)
            log.info("day=%s raw_rows=%s", day, rows)
        finally:
            raw.unpersist()
        day += timedelta(1)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
