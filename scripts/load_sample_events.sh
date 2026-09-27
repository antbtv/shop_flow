#!/usr/bin/env bash
# One-off loader for Milestone 1.10: copy CDC events from Kafka (laptop) into shopflow.raw_events
# (ClickHouse on Pi5). Spark replaces it in Milestone 2.
# Usage: scripts/load_sample_events.sh [table ...]   (default: all five CDC tables)
#
# Kafka coordinates (partition, offset) come from the console consumer; LSN, commit time and op
# are extracted by ClickHouse from the Debezium envelope at insert time. Running the script twice
# writes the same rows again: raw_events (ReplacingMergeTree) must collapse them (ADR-0006).
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/lib/clickhouse-env.sh

tables=("$@")
((${#tables[@]})) || tables=(customers products orders order_items inventory)

read -r -d '' INSERT_QUERY <<'SQL' || true
INSERT INTO shopflow.raw_events
    (topic, kafka_partition, kafka_offset, source_lsn, event_key, event_time, op, payload)
SELECT
    topic,
    kafka_partition,
    kafka_offset,
    JSONExtractUInt(payload, 'source', 'lsn'),
    event_key,
    fromUnixTimestamp64Milli(JSONExtractInt(payload, 'source', 'ts_ms'), 'UTC'),
    JSONExtractString(payload, 'op'),
    payload
FROM input('topic String, kafka_partition UInt32, kafka_offset UInt64, event_key String, payload String')
FORMAT JSONEachRow
SQL

for t in "${tables[@]}"; do
    topic="cdc.public.$t"
    rows=$(mktemp)
    docker compose -f docker-compose.laptop.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic "$topic" --from-beginning --timeout-ms 10000 \
        --formatter-property print.partition=true --formatter-property print.offset=true \
        --formatter-property print.key=true 2>/dev/null |
        jq -cR --arg topic "$topic" 'split("\t") | {
            topic: $topic,
            kafka_partition: (.[0] | ltrimstr("Partition:") | tonumber),
            kafka_offset: (.[1] | ltrimstr("Offset:") | tonumber),
            event_key: .[2],
            payload: .[3]
        } | select(.payload != "null")' >"$rows"
    ch_curl --url-query "query=$INSERT_QUERY" --data-binary @"$rows" "$CLICKHOUSE_URL/" >/dev/null
    echo "$topic: loaded $(wc -l <"$rows") events"
    rm -f "$rows"
done
