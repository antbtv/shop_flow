#!/usr/bin/env bash
# Kafka -> ClickHouse check (ADR-0008). For each CDC topic, every Kafka position in
# [earliest, latest) must be in raw_events exactly once after dedup (uniqExact, no FINAL).
# stg_orders and stg_order_items must match Postgres: row count and cheap checksums.
# Also prints the ingest lag of the last 10 minutes.
# Usage: scripts/check_pipeline.sh   Exit 1 on any mismatch.
# Exact only when the job has caught up: stop the generator and wait one trigger (30 s).
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker-compose.laptop.yml)
TABLES=(customers products orders order_items inventory)
rc=0

offsets() { # offsets earliest|latest -> "topic offset" lines for partition 0
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9092 --time "$1" |
        awk -F: '$1 ~ /^cdc\.public\./ && $2 == 0 {print $1, $3}'
}
declare -A earliest latest
while read -r topic off; do earliest[$topic]=$off; done < <(offsets earliest)
while read -r topic off; do latest[$topic]=$off; done < <(offsets latest)

printf '%-24s %10s %10s %10s %10s  %s\n' topic earliest latest kafka raw verdict
for t in "${TABLES[@]}"; do
    topic=cdc.public.$t
    lo=${earliest[$topic]:?no offsets for $topic}
    hi=${latest[$topic]:?no offsets for $topic}
    raw=$(scripts/ch-query.sh "SELECT uniqExact(kafka_offset) FROM raw_events
        WHERE topic = '$topic' AND kafka_partition = 0 AND kafka_offset >= $lo AND kafka_offset < $hi")
    verdict=OK
    [[ $raw == $((hi - lo)) ]] || { verdict=MISMATCH; rc=1; }
    printf '%-24s %10s %10s %10s %10s  %s\n' "$topic" "$lo" "$hi" $((hi - lo)) "$raw" "$verdict"
done

psql() {
    "${COMPOSE[@]}" exec -T postgres sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -At -F ' ' -c \"$1\""
}
compare() { # compare name postgres_value clickhouse_value
    local verdict=OK
    [[ $2 == "$3" ]] || { verdict=MISMATCH; rc=1; }
    printf '%-40s %24s %24s  %s\n' "$1" "$2" "$3" "$verdict"
}
# Timestamps as epoch milliseconds: ClickHouse keeps DateTime64(3), Postgres microseconds.
printf '\n%-40s %24s %24s  %s\n' check postgres clickhouse verdict
compare "orders: rows, sum(order_id), max(updated_at)" \
    "$(psql "SELECT count(*), coalesce(sum(order_id), 0), coalesce(floor(extract(epoch FROM max(updated_at)) * 1000)::bigint, 0) FROM orders")" \
    "$(scripts/ch-query.sh "SELECT count(), sum(order_id), toUnixTimestamp64Milli(max(updated_at))
        FROM stg_orders FINAL WHERE is_deleted = 0 FORMAT CustomSeparated
        SETTINGS format_custom_field_delimiter = ' '")"
compare "orders: count by status" \
    "$(psql "SELECT string_agg(status || '=' || n, ',' ORDER BY status) FROM (SELECT status, count(*) n FROM orders GROUP BY status) s")" \
    "$(scripts/ch-query.sh "SELECT arrayStringConcat(groupArray(concat(status, '=', toString(n))), ',')
        FROM (SELECT status, count() AS n FROM stg_orders FINAL WHERE is_deleted = 0
              GROUP BY status ORDER BY status)")"
compare "order_items: rows, sum(qty), sum(qty*price)" \
    "$(psql "SELECT count(*), coalesce(sum(quantity), 0), coalesce(sum(quantity * price_at_order), 0.00) FROM order_items")" \
    "$(scripts/ch-query.sh "SELECT count(), sum(quantity), toDecimalString(sum(quantity * price_at_order), 2)
        FROM stg_order_items FINAL WHERE is_deleted = 0 FORMAT CustomSeparated
        SETTINGS format_custom_field_delimiter = ' '")"

echo
echo "ingest lag, last 10 min (ingested_at - event_time, seconds):"
scripts/ch-query.sh "SELECT count() AS events,
        round(quantile(0.5)(dateDiff('millisecond', event_time, ingested_at)) / 1000, 1) AS p50_s,
        round(quantile(0.95)(dateDiff('millisecond', event_time, ingested_at)) / 1000, 1) AS p95_s,
        round(max(dateDiff('millisecond', event_time, ingested_at)) / 1000, 1) AS max_s
    FROM raw_events
    WHERE event_time >= now64(3) - INTERVAL 10 MINUTE
    FORMAT PrettyCompactMonoBlock"
exit $rc
