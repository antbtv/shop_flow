"""Streaming job: Debezium topics in Kafka -> ClickHouse on Pi5 (ADR-0008).

One query, one checkpoint. Each micro-batch is written with keys from ADR-0006, so a replayed
batch (same batchId, same offset range after a restart) collapses in ClickHouse. Per batch:
raw_events, then stg_* and the SCD2 journals, then the order status history (ADR-0009).

Run by spark-jobs/entrypoint.sh (waits for ClickHouse /ping first):
    /opt/spark/bin/spark-submit /opt/shopflow/streaming_to_clickhouse.py
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from shopflow_stream.liveness import Heartbeat, ProgressListener, start_watchdog
from shopflow_stream.sink import catalog_conf, table
from shopflow_stream.transforms import (
    STATUS_HISTORY_TABLE,
    STG_SPECS,
    StgSpec,
    latest_per_key,
    latest_status_rows,
    raw_violations,
    status_history_rows,
    stg_rows,
    to_raw_events,
)

log = logging.getLogger("shopflow.stream")

TABLES = ("customers", "products", "orders", "order_items", "inventory")
# An explicit list: heartbeat topics and new topics are never picked up silently (ADR-0008).
TOPICS = ",".join(f"cdc.public.{t}" for t in TABLES)

CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "/checkpoints/streaming_to_clickhouse")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000"))
# "false" only for a manual recovery after a stop longer than Kafka retention (runbook).
FAIL_ON_DATA_LOSS = os.environ.get("FAIL_ON_DATA_LOSS", "true")
# Longer than one socket timeout (120 s) plus client retries, shorter than NFR-3 (5 min) x 2.
WATCHDOG_TIMEOUT_S = float(os.environ.get("WATCHDOG_TIMEOUT_S", "600"))
VIOLATION_SAMPLE = 5
QUARANTINE_SAMPLE = 5


class ContractViolation(RuntimeError):
    """An event breaks the CDC contract (ADR-0005): retrying cannot fix it."""


def _write_quarantined(
    rows: DataFrame,
    target: str,
    key_columns: tuple[str, ...],
    dedup: Callable[[DataFrame], DataFrame],
    batch_id: int,
) -> None:
    """Rows with quarantine_reason are logged, not written, still in raw_events for a reload
    (ADR-0008); the rest is deduplicated within the batch and appended."""
    rows = rows.persist()
    try:
        quarantined = rows.where(rows.quarantine_reason.isNotNull())
        sample = quarantined.select(*key_columns, "kafka_offset", "quarantine_reason")
        sample = sample.limit(QUARANTINE_SAMPLE).collect()
        if sample:
            log.warning(
                "batch=%s %s quarantined=%s sample=%s",
                batch_id,
                target,
                quarantined.count(),
                [(_key(r, key_columns), r.kafka_offset, r.quarantine_reason) for r in sample],
            )
        valid = dedup(rows.where(rows.quarantine_reason.isNull()))
        if not valid.isEmpty():
            valid.writeTo(table(target)).append()
    finally:
        rows.unpersist()


def _key(row, key_columns: tuple[str, ...]):
    """Key of a row for the log: a scalar for one column, a tuple for a composite key."""
    values = tuple(row[c] for c in key_columns)
    return values[0] if len(values) == 1 else values


def write_stg(raw: DataFrame, spec: StgSpec, batch_id: int) -> None:
    """Latest state per PK into stg_*, or one row per change into a journal (ADR-0009)."""
    _write_quarantined(
        stg_rows(raw, spec),
        spec.target_table,
        spec.pk,
        lambda df: latest_per_key(df, spec),
        batch_id,
    )


def write_status_history(raw: DataFrame, batch_id: int) -> None:
    """Order status transitions for the funnel (ADR-0009)."""
    _write_quarantined(
        status_history_rows(raw),
        STATUS_HISTORY_TABLE,
        ("order_id",),
        latest_status_rows,
        batch_id,
    )


def write_batch(batch: DataFrame, batch_id: int) -> None:
    """raw_events first, then stg_*. If a write fails the whole batch is replayed after the
    restart; both writes are idempotent by the ADR-0006 keys."""
    raw = to_raw_events(batch).persist()
    try:
        violations = raw_violations(raw).select("topic", "kafka_partition", "kafka_offset")
        sample = violations.limit(VIOLATION_SAMPLE).collect()
        if sample:
            positions = ", ".join(f"{r.topic}:{r.kafka_partition}:{r.kafka_offset}" for r in sample)
            raise ContractViolation(
                f"batch {batch_id}: events without payload, source.lsn, source.ts_ms or op "
                f"(first {len(sample)}): {positions}. See the runbook, section Spark."
            )
        if raw.isEmpty():
            return
        raw.writeTo(table("raw_events")).append()
        for spec in STG_SPECS:
            write_stg(raw, spec, batch_id)
        write_status_history(raw, batch_id)
    finally:
        raw.unpersist()


def build_session() -> SparkSession:
    builder = SparkSession.builder.appName("shopflow-streaming-to-clickhouse")
    for key, value in catalog_conf().items():
        builder = builder.config(key, value)
    # One insert per table per batch: never split a batch into several parts.
    builder = builder.config("spark.clickhouse.write.batchSize", str(MAX_OFFSETS_PER_TRIGGER))
    return builder.getOrCreate()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    heartbeat = Heartbeat()
    spark.streams.addListener(ProgressListener(heartbeat))
    start_watchdog(heartbeat, WATCHDOG_TIMEOUT_S)

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", FAIL_ON_DATA_LOSS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    query = (
        source.writeStream.queryName("cdc_to_clickhouse")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    log.info(
        "started: topics=%s trigger=%s checkpoint=%s failOnDataLoss=%s",
        TOPICS,
        TRIGGER,
        CHECKPOINT,
        FAIL_ON_DATA_LOSS,
    )
    try:
        query.awaitTermination()
    except Exception:
        # The query failed: exit non-zero so Docker restarts it from the checkpoint.
        log.exception("query failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
