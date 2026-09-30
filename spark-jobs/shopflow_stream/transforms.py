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
- stg_*_versions: SCD2 journal, one row per change keyed by (PK, valid_from = source.ts_us);
  ClickHouse rebuilds dim_* from it (ADR-0009). Same parsing and quarantine as stg_*.
- stg_order_status_history: one row per status change of an order, for the funnel (ADR-0009).

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
    """A column of a staging table.

    kind: "uint64" (BIGINT >= 0), "uint32" (INT >= 0), "int32", "string", "timestamp" (ISO
    string from Debezium), "decimal" (Decimal(10,2) sent as a string,
    decimal.handling.mode=string).
    nullable: the Postgres column may be NULL; the ClickHouse column is not, so NULL becomes
    the kind's default instead of a quarantine.
    """

    name: str
    kind: str
    nullable: bool = False


@dataclass(frozen=True)
class StgSpec:
    """One staging table fed by one topic.

    keys: the primary key columns, read from the message key (always present, also for d).
    versioned: False = latest state per key (stg_*, ReplacingMergeTree(version, is_deleted));
    True = SCD2 journal (stg_*_versions): one row per change, keyed by (keys, valid_from).
    """

    source_table: str
    target_table: str
    keys: tuple[Field, ...]
    fields: tuple[Field, ...]
    versioned: bool = False

    @property
    def topic(self) -> str:
        return TOPIC_PREFIX + self.source_table

    @property
    def pk(self) -> tuple[str, ...]:
        return tuple(k.name for k in self.keys)

    @property
    def dedup_key(self) -> tuple[str, ...]:
        """What one output row stands for within a batch (ADR-0006, ADR-0009)."""
        return (*self.pk, "valid_from") if self.versioned else self.pk

    @property
    def columns(self) -> list[str]:
        extra = ["is_snapshot"] if self.versioned else []
        head = ["valid_from"] if self.versioned else []
        return [
            *self.pk,
            *head,
            *(f.name for f in self.fields),
            *extra,
            "is_deleted",
            "event_time",
            "version",
        ]


ORDERS = StgSpec(
    source_table="orders",
    target_table="stg_orders",
    keys=(Field("order_id", "uint64"),),
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
    keys=(Field("order_item_id", "uint64"),),
    fields=(
        Field("order_id", "uint64"),
        Field("product_id", "uint64"),
        Field("quantity", "int32"),
        Field("price_at_order", "decimal"),
    ),
)

INVENTORY = StgSpec(
    source_table="inventory",
    target_table="stg_inventory",
    keys=(Field("product_id", "uint64"), Field("warehouse_id", "uint32")),
    fields=(Field("quantity", "int32"), Field("updated_at", "timestamp")),
)

# SCD2 attributes only (ADR-0009): other columns (email, updated_at) do not open a version,
# the refreshable MV drops journal rows whose attributes did not change.
CUSTOMER_VERSIONS = StgSpec(
    source_table="customers",
    target_table="stg_customer_versions",
    keys=(Field("customer_id", "uint64"),),
    fields=(
        Field("name", "string"),
        Field("address", "string", nullable=True),
        Field("segment", "string", nullable=True),
    ),
    versioned=True,
)

PRODUCT_VERSIONS = StgSpec(
    source_table="products",
    target_table="stg_product_versions",
    keys=(Field("product_id", "uint64"),),
    fields=(
        Field("name", "string"),
        Field("category", "string"),
        Field("price", "decimal"),
    ),
    versioned=True,
)

STG_SPECS = (ORDERS, ORDER_ITEMS, INVENTORY, CUSTOMER_VERSIONS, PRODUCT_VERSIONS)

STATUS_HISTORY_TABLE = "stg_order_status_history"
STATUS_HISTORY_COLUMNS = ("order_id", "status", "changed_at", "is_snapshot", "version")

# How a kind is read from JSON; conversion and defaults are below.
_JSON_TYPE = {
    "uint64": LongType(),
    "uint32": LongType(),
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
    if kind in ("uint32", "int32"):
        return F.lit(0).cast(IntegerType())
    if kind == "string":
        return F.lit("")
    if kind == "timestamp":
        return F.timestamp_seconds(F.lit(0))
    return F.lit(0).cast(DecimalType(10, 2))


def _convert(kind: str, value: Column) -> Column:
    if kind == "uint32":
        # INT >= 0 fits Int32 in Spark; values outside UInt32 are caught by _out_of_range.
        return value.try_cast(IntegerType())
    if kind == "timestamp":
        return F.try_to_timestamp(value)
    if kind == "decimal":
        return value.try_cast(DecimalType(10, 2))
    return value


def _out_of_range(kind: str, value: Column) -> Column:
    if kind in ("uint64", "uint32"):
        return value < 0
    return F.lit(False)


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
        [StructField(k.name, LongType()) for k in spec.keys]
        + [StructField(f.name, _JSON_TYPE[f.kind]) for f in spec.fields]
    )


def _envelope_schema(row: StructType) -> StructType:
    return StructType(
        [
            StructField("before", row),
            StructField("after", row),
            StructField("op", StringType()),
            StructField(
                "source",
                StructType([StructField("lsn", LongType()), StructField("ts_us", LongType())]),
            ),
        ]
    )


def stg_rows(raw_df: DataFrame, spec: StgSpec) -> DataFrame:
    """raw_events rows of one topic -> staging rows, one per event, not deduplicated.

    Columns: the target table columns, kafka_offset (tie-break for dedup) and
    quarantine_reason (NULL for rows that may be written).
    - c, u, r: values from `after`; r is a snapshot row and behaves as c (ADR-0006).
    - d: is_deleted = 1, PK from the message key, other columns from `before` with defaults:
      they do not matter for a deleted row, and a delete must not depend on REPLICA IDENTITY.
      In a journal the delete row closes the previous version (ADR-0009).
    - other ops (for example t) are not staging events and are dropped.
    - versioned: valid_from = source.ts_us, is_snapshot = (op = r).
    """
    envelope = F.from_json(F.col("payload"), _envelope_schema(_row_schema(spec)))
    key = F.from_json(
        F.col("event_key"), StructType([StructField(k.name, LongType()) for k in spec.keys])
    )
    events = raw_df.where((F.col("topic") == spec.topic) & F.col("op").isin(*STG_OPS)).select(
        F.col("kafka_offset"),
        F.col("op"),
        F.col("event_time"),
        *(key[k.name].alias(f"_key_{k.name}") for k in spec.keys),
        envelope["source"]["lsn"].alias("_lsn"),
        envelope["source"]["ts_us"].alias("_ts_us"),
        F.when(F.col("op") == "d", envelope["before"]).otherwise(envelope["after"]).alias("_row"),
    )

    deleted = F.col("op") == "d"
    keys, key_reasons = [], []
    for k in spec.keys:
        converted = _convert(k.kind, F.col(f"_key_{k.name}"))
        keys.append(converted.alias(k.name))
        key_reasons.append(
            F.when(converted.isNull() | _out_of_range(k.kind, converted), F.lit(f"bad {k.name}"))
        )

    values, reasons = [], []
    for f in spec.fields:
        converted = _convert(f.kind, F.col("_row")[f.name])
        with_default = F.coalesce(converted, _default(f.kind))
        if f.nullable:
            values.append(with_default.alias(f.name))
            bad = F.col("_row")[f.name].isNotNull() & converted.isNull()
        else:
            values.append(F.when(deleted, with_default).otherwise(converted).alias(f.name))
            bad = converted.isNull()
        bad = bad | F.coalesce(_out_of_range(f.kind, converted), F.lit(False))
        reasons.append(F.when(~deleted & bad, F.lit(f"bad {f.name}")))

    head, tail = [], []
    version_reasons = []
    if spec.versioned:
        head.append(F.timestamp_micros(F.col("_ts_us")).alias("valid_from"))
        tail.append((F.col("op") == "r").cast(ByteType()).alias("is_snapshot"))
        version_reasons.append(F.when(F.col("_ts_us").isNull(), F.lit("bad valid_from")))

    reasons = [
        *key_reasons,
        F.when(F.col("_lsn").isNull() | (F.col("_lsn") < 0), F.lit("bad version")),
        *version_reasons,
        F.when(~deleted & F.col("_row").isNull(), F.lit("no after")),
        *reasons,
    ]
    return events.select(
        *keys,
        *head,
        *values,
        *tail,
        deleted.cast(ByteType()).alias("is_deleted"),
        F.col("event_time"),
        F.col("_lsn").alias("version"),
        F.col("kafka_offset"),
        # First reason wins; concat_ws would hide NULLs, coalesce keeps the check order.
        F.coalesce(*reasons).alias("quarantine_reason"),
    )


def latest_per_key(stg_df: DataFrame, spec: StgSpec) -> DataFrame:
    """One row per spec.dedup_key: the highest (version, kafka_offset).

    stg_*: the key is the PK, the latest state wins. Journals: the key is (PK, valid_from),
    so several UPDATEs of one transaction collapse to the highest LSN while the history of
    the key stays (grouping a journal by PK alone would squash it to one row, ADR-0009).
    Debezium replays carry the same LSN, the offset makes the choice deterministic.
    Correctness across batches comes from ReplacingMergeTree, this only shrinks the insert."""
    group = list(spec.dedup_key)
    value_columns = [c for c in spec.columns if c not in group]
    latest = F.max_by(
        F.struct(*value_columns), F.struct(F.col("version"), F.col("kafka_offset"))
    )
    return (
        stg_df.groupBy(*group)
        .agg(latest.alias("_latest"))
        .select(*group, *(F.col("_latest")[c].alias(c) for c in value_columns))
    )


_STATUS_ROW = StructType(
    [StructField("order_id", LongType()), StructField("status", StringType())]
)


def status_history_rows(raw_df: DataFrame) -> DataFrame:
    """orders events -> status transitions, one per event, not deduplicated (ADR-0009).

    A row for c and r (the status at insert or in the snapshot) and for u whose status
    differs from `before` (or `before` is missing). d and UPDATEs of other columns give none.
    changed_at = source.ts_ms (event_time). Columns: STATUS_HISTORY_COLUMNS, kafka_offset,
    quarantine_reason."""
    envelope = F.from_json(F.col("payload"), _envelope_schema(_STATUS_ROW))
    key = F.from_json(F.col("event_key"), StructType([StructField("order_id", LongType())]))
    before, after = envelope["before"]["status"], envelope["after"]["status"]
    changed = F.col("op").isin("c", "r") | (
        (F.col("op") == "u") & (envelope["before"].isNull() | ~before.eqNullSafe(after))
    )
    events = raw_df.where((F.col("topic") == ORDERS.topic) & changed).select(
        key["order_id"].alias("order_id"),
        after.alias("status"),
        F.col("event_time").alias("changed_at"),
        (F.col("op") == "r").cast(ByteType()).alias("is_snapshot"),
        envelope["source"]["lsn"].alias("version"),
        F.col("kafka_offset"),
    )
    reasons = [
        F.when(F.col("order_id").isNull() | (F.col("order_id") < 0), F.lit("bad order_id")),
        F.when(F.col("version").isNull() | (F.col("version") < 0), F.lit("bad version")),
        F.when(F.col("status").isNull(), F.lit("bad status")),
    ]
    return events.withColumn("quarantine_reason", F.coalesce(*reasons))


def latest_status_rows(history_df: DataFrame) -> DataFrame:
    """One row per (order_id, status, version): Debezium replays collapse, a repeated status
    or a later snapshot of the same status stays a separate row (the mart takes the first)."""
    group = ["order_id", "status", "version"]
    rest = [c for c in STATUS_HISTORY_COLUMNS if c not in group]
    latest = F.max_by(F.struct(*rest), F.col("kafka_offset"))
    return (
        history_df.groupBy(*group)
        .agg(latest.alias("_latest"))
        .select(*group, *(F.col("_latest")[c].alias(c) for c in rest))
    )
