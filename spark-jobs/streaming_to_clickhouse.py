"""Streaming job: Debezium topics in Kafka -> ClickHouse on Pi5 (ADR-0008).

One query, one checkpoint. Each micro-batch is written with keys from ADR-0006, so a replayed
batch (same batchId, same offset range after a restart) collapses in ClickHouse.

Run by spark-jobs/entrypoint.sh (waits for ClickHouse /ping first):
    /opt/spark/bin/spark-submit /opt/shopflow/streaming_to_clickhouse.py
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from shopflow_stream.liveness import Heartbeat, ProgressListener, start_watchdog
from shopflow_stream.sink import catalog_conf, table
from shopflow_stream.transforms import raw_violations, to_raw_events

log = logging.getLogger("shopflow.stream")

TABLES = ("customers", "products", "orders", "order_items", "inventory")
# An explicit list: heartbeat topics and new topics are never picked up silently (ADR-0008).
TOPICS = ",".join(f"cdc.public.{t}" for t in TABLES)

CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "/checkpoints/streaming_to_clickhouse")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000"))
# Longer than one socket timeout (120 s) plus client retries, shorter than NFR-3 (5 min) x 2.
WATCHDOG_TIMEOUT_S = float(os.environ.get("WATCHDOG_TIMEOUT_S", "600"))
VIOLATION_SAMPLE = 5


class ContractViolation(RuntimeError):
    """An event breaks the CDC contract (ADR-0005): retrying cannot fix it."""


def write_batch(batch: DataFrame, batch_id: int) -> None:
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
        .option("failOnDataLoss", "true")
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
    log.info("started: topics=%s trigger=%s checkpoint=%s", TOPICS, TRIGGER, CHECKPOINT)
    try:
        query.awaitTermination()
    except Exception:
        # The query failed: exit non-zero so Docker restarts it from the checkpoint.
        log.exception("query failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
