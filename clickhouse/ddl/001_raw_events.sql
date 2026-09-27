-- Raw CDC events as delivered by Debezium, for debugging and reprocessing (NFR-5: 30 days).
-- Key = position in Kafka: a batch rewritten by Spark after a restart collapses into one row.
-- source_lsn guards against a recreated topic, where offsets start again from 0 (ADR-0006).
-- Debezium redeliveries are different Kafka messages: they stay here and collapse in stg_/dim_.
CREATE TABLE IF NOT EXISTS shopflow.raw_events
(
    topic           LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    source_lsn      UInt64,
    event_key       String,
    -- source.ts_ms: commit time in Postgres, so a redelivery lands in the same partition.
    event_time      DateTime64(3, 'UTC'),
    op              LowCardinality(String),
    payload         String,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (topic, kafka_partition, kafka_offset, source_lsn)
TTL toDateTime(event_time) + INTERVAL 30 DAY
-- Linter note: SETTINGS after TTL is valid ClickHouse, but the sqlfluff parser rejects it.
SETTINGS ttl_only_drop_parts = 1; -- noqa: PRS
