"""Debezium envelope -> ClickHouse rows (ADR-0005 contract, ADR-0006 keys, ADR-0008 policies).

Pure DataFrame functions, no I/O: the streaming job wires them into foreachBatch, tests run
them on a local SparkSession.

- raw_events: every event as is. Only source.lsn, source.ts_ms and op are parsed, without
  table schemas. An event without them breaks the CDC contract: raw_violations() finds it
  and the job fails fast.
- stg_*: the latest state of a Postgres row. Rows that do not fit the ClickHouse types are
  quarantined (quarantine_reason is set): they stay in raw_events and are not written.
  The connector does not validate values (NULL in UInt64 becomes 0, -1 wraps), so every
  check happens here, before the write.

Spark 4 runs in ANSI mode: a plain CAST of a bad string fails the whole batch, so values
are converted with try_cast / try_to_timestamp and a failure becomes a quarantine reason.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TOPIC_PREFIX = "cdc.public."
STG_OPS = ("c", "u", "d", "r")

RAW_COLUMNS = (
    "topic",
    "kafka_partition",
    "kafka_offset",
    "source_lsn",
    "event_key",
    "event_time",
    "op",
    "payload",
    "ingested_at",
)

# Only what raw_events needs; independent of table schemas (ADR-0008).
_SOURCE_ENVELOPE = StructType(
    [
        StructField("op", StringType()),
        StructField(
            "source",
            StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())]),
        ),
    ]
)


@dataclass(frozen=True)
class Field:
    """A non-key column of a staging table.

    kind: "uint64" (BIGINT >= 0), "int32", "string", "timestamp" (ISO string from Debezium),
    "decimal" (Decimal(10,2) sent as a string, decimal.handling.mode=string).
    """

    name: str
    kind: str


@dataclass(frozen=True)
class StgSpec:
    source_table: str
    target_table: str
    pk: str
    fields: tuple[Field, ...]

    @property
    def topic(self) -> str:
        return TOPIC_PREFIX + self.source_table

    @property
    def columns(self) -> list[str]:
        return [self.pk, *(f.name for f in self.fields), "version", "is_deleted"]


ORDERS = StgSpec(
    source_table="orders",
    target_table="stg_orders",
    pk="order_id",
    fields=(
        Field("customer_id", "uint64"),
        Field("status", "string"),
        Field("created_at", "timestamp"),
        Field("updated_at", "timestamp"),
    ),
)

ORDER_ITEMS = StgSpec(
    source_table="order_items",
    target_table="stg_order_items",
    pk="order_item_id",
    fields=(
        Field("order_id", "uint64"),
        Field("product_id", "uint64"),
        Field("quantity", "int32"),
        Field("price_at_order", "decimal"),
    ),
)

STG_SPECS = (ORDERS, ORDER_ITEMS)

# How a kind is read from JSON; conversion and defaults are below.
_JSON_TYPE = {
    "uint64": LongType(),
    "int32": IntegerType(),
    "string": StringType(),
    "timestamp": StringType(),
    "decimal": StringType(),
}


def _default(kind: str) -> Column:
    """Value for columns of a deleted row whose `before` lacks them. Built on call: Column
    objects need an active SparkContext."""
    if kind == "uint64":
        return F.lit(0).cast(LongType())
    if kind == "int32":
        return F.lit(0).cast(IntegerType())
    if kind == "string":
        return F.lit("")
    if kind == "timestamp":
        return F.timestamp_seconds(F.lit(0))
    return F.lit(0).cast(DecimalType(10, 2))


def _convert(kind: str, value: Column) -> Column:
    if kind == "timestamp":
        return F.try_to_timestamp(value)
    if kind == "decimal":
        return value.try_cast(DecimalType(10, 2))
    return value


def to_raw_events(kafka_df: DataFrame, ingested_at: Column | None = None) -> DataFrame:
    """Kafka source rows -> raw_events rows. ingested_at defaults to the batch time in Spark:
    the same clock as Postgres (laptop), so Pi5 clock skew stays out of the NFR-3 metric."""
    payload = F.col("value").cast(StringType())
    envelope = F.from_json(payload, _SOURCE_ENVELOPE)
    return kafka_df.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        envelope["source"]["lsn"].alias("source_lsn"),
        F.col("key").cast(StringType()).alias("event_key"),
        F.timestamp_millis(envelope["source"]["ts_ms"]).alias("event_time"),
        envelope["op"].alias("op"),
        payload.alias("payload"),
        (F.current_timestamp() if ingested_at is None else ingested_at).alias("ingested_at"),
    )


def raw_violations(raw_df: DataFrame) -> DataFrame:
    """raw_events rows that break the CDC contract (ADR-0005): the job must fail fast."""
    return raw_df.where(
        F.col("payload").isNull()
        | F.col("source_lsn").isNull()
        | (F.col("source_lsn") < 0)
        | F.col("event_time").isNull()
        | F.col("op").isNull()
    )


def _row_schema(spec: StgSpec) -> StructType:
    return StructType(
        [StructField(spec.pk, LongType())]
        + [StructField(f.name, _JSON_TYPE[f.kind]) for f in spec.fields]
    )


def _envelope_schema(spec: StgSpec) -> StructType:
    row = _row_schema(spec)
    return StructType(
        [
            StructField("before", row),
            StructField("after", row),
            StructField("op", StringType()),
            StructField("source", StructType([StructField("lsn", LongType())])),
        ]
    )


def stg_rows(raw_df: DataFrame, spec: StgSpec) -> DataFrame:
    """raw_events rows of one topic -> staging rows, one per event, not deduplicated.

    Columns: the target table columns, kafka_offset (tie-break for dedup) and
    quarantine_reason (NULL for rows that may be written).
    - c, u, r: values from `after`; r is a snapshot row and behaves as c (ADR-0006).
    - d: is_deleted = 1, PK from the message key, other columns from `before` with defaults:
      they do not matter for a deleted row, and a delete must not depend on REPLICA IDENTITY.
    - other ops (for example t) are not staging events and are dropped.
    """
    envelope = F.from_json(F.col("payload"), _envelope_schema(spec))
    key = F.from_json(F.col("event_key"), StructType([StructField(spec.pk, LongType())]))
    events = raw_df.where((F.col("topic") == spec.topic) & F.col("op").isin(*STG_OPS)).select(
        F.col("kafka_offset"),
        F.col("op"),
        key[spec.pk].alias("_pk"),
        envelope["source"]["lsn"].alias("_lsn"),
        F.when(F.col("op") == "d", envelope["before"]).otherwise(envelope["after"]).alias("_row"),
    )

    deleted = F.col("op") == "d"
    values, reasons = [], []
    for f in spec.fields:
        converted = _convert(f.kind, F.col("_row")[f.name])
        with_default = F.coalesce(converted, _default(f.kind))
        values.append(F.when(deleted, with_default).otherwise(converted).alias(f.name))
        bad = converted.isNull()
        if f.kind == "uint64":
            bad = bad | (converted < 0)
        reasons.append(F.when(~deleted & bad, F.lit(f"bad {f.name}")))

    reasons = [
        F.when(F.col("_pk").isNull() | (F.col("_pk") < 0), F.lit(f"bad {spec.pk}")),
        F.when(F.col("_lsn").isNull() | (F.col("_lsn") < 0), F.lit("bad version")),
        F.when(~deleted & F.col("_row").isNull(), F.lit("no after")),
        *reasons,
    ]
    return events.select(
        F.col("_pk").alias(spec.pk),
        *values,
        F.col("_lsn").alias("version"),
        deleted.cast(ByteType()).alias("is_deleted"),
        F.col("kafka_offset"),
        # First reason wins; concat_ws would hide NULLs, coalesce keeps the check order.
        F.coalesce(*reasons).alias("quarantine_reason"),
    )


def latest_per_key(stg_df: DataFrame, spec: StgSpec) -> DataFrame:
    """One row per PK: the highest (version, kafka_offset). Debezium replays after a crash
    carry the same LSN, the offset makes the choice deterministic. Correctness across
    batches comes from ReplacingMergeTree(version, is_deleted), this only shrinks the insert."""
    value_columns = [c for c in spec.columns if c != spec.pk]
    latest = F.max_by(
        F.struct(*value_columns), F.struct(F.col("version"), F.col("kafka_offset"))
    )
    return (
        stg_df.groupBy(spec.pk)
        .agg(latest.alias("_latest"))
        .select(spec.pk, *(F.col("_latest")[c].alias(c) for c in value_columns))
    )
