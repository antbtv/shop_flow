#!/usr/bin/env bash
# Kafka -> ClickHouse check (ADR-0008). For each CDC topic, every Kafka position in
# [earliest, latest) must be in raw_events exactly once after dedup (uniqExact, no FINAL).
# stg_orders, stg_order_items and stg_inventory must match Postgres: row count and cheap
# checksums. The SCD2 journals must hold one row per (key, source.ts_us) of raw_events, and the
# status history the current status of every order changed since it started (ADR-0009): both
# only from the first event in the journal, older history comes from the backfill (3.8).
# SCD2 dimensions and fact_orders are checked last (see below).
# Also prints the ingest lag of the last 10 minutes. CLICKHOUSE_URL may point to another server.
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
compare "inventory: rows, sum(quantity)" \
    "$(psql "SELECT count(*), coalesce(sum(quantity), 0) FROM inventory")" \
    "$(scripts/ch-query.sh "SELECT count(), sum(quantity) FROM stg_inventory FINAL WHERE is_deleted = 0
        FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"

printf '\n%-40s %24s %24s  %s\n' check raw_events clickhouse verdict
for pair in customers:stg_customer_versions products:stg_product_versions; do
    topic=cdc.public.${pair%%:*} journal=${pair##*:}
    since=$(scripts/ch-query.sh "SELECT toString(min(event_time)) FROM $journal")
    if [[ $since == 1970-01-01* ]]; then
        printf '%-40s %24s %24s  %s\n' "$journal (key, ts_us)" - 0 EMPTY
        continue
    fi
    compare "$journal (key, ts_us) since ${since% *}" \
        "$(scripts/ch-query.sh "SELECT uniqExact(event_key, JSONExtract(payload, 'source', 'ts_us', 'Int64'))
            FROM raw_events WHERE topic = '$topic' AND op IN ('c', 'u', 'd', 'r')
              AND event_time >= '$since'")" \
        "$(scripts/ch-query.sh "SELECT count() FROM $journal FINAL WHERE event_time >= '$since'")"
done
since=$(scripts/ch-query.sh "SELECT toString(min(changed_at)) FROM stg_order_status_history")
if [[ $since == 1970-01-01* ]]; then
    printf '%-40s %24s %24s  %s\n' "status history" - 0 EMPTY
else
    compare "orders without current status in history" 0 \
        "$(scripts/ch-query.sh "SELECT count() FROM stg_orders FINAL
            WHERE is_deleted = 0 AND event_time >= '$since'
              AND (order_id, status) NOT IN (SELECT order_id, status FROM stg_order_status_history)")"
fi

# SCD2 dimensions (ADR-0009): current versions = Postgres (count + md5 of sorted attributes),
# one current version per key, no gaps or overlaps, valid_from non-decreasing in LSN order.
# Exact once the job caught up and dim_*_mv refreshed after it (every 2 min).
printf '\n%-40s %24s %24s  %s\n' check postgres clickhouse verdict
dim_check() { # dim_check dim id "pg_attr_sql" "ch_attr_sql"
    local dim=$1 id=$2 source=${1#dim_}
    compare "$dim: current = $source (count, md5)" \
        "$(psql "SELECT count(*), coalesce(md5(string_agg($id || '|' || $3, E'\\n' ORDER BY $id)), '') FROM $source")" \
        "$(scripts/ch-query.sh "SELECT count(), if(count() = 0, '', lower(hex(MD5(arrayStringConcat(
                arrayMap(x -> x.2, arraySort(groupArray(($id, concat(toString($id), '|', $4))))), '\n')))))
            FROM $dim WHERE is_current = 1 FORMAT CustomSeparated
            SETTINGS format_custom_field_delimiter = ' '")"
    compare "$dim: keys with >1 current, gaps, clock back" "0 0 0" \
        "$(scripts/ch-query.sh "SELECT
                (SELECT count() FROM (SELECT $id FROM $dim GROUP BY $id HAVING sum(is_current) > 1)),
                countIf(next_from IS NOT NULL AND (valid_to IS NULL OR valid_to != next_from)),
                countIf(valid_to < valid_from)
            FROM (SELECT $id, valid_from, valid_to,
                    leadInFrame(toNullable(valid_from)) OVER (PARTITION BY $id ORDER BY version
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_from
                  FROM $dim)
            FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"
}
dim_check dim_customers customer_id "name || '|' || coalesce(address, '') || '|' || coalesce(segment, '')" \
    "name, '|', address, '|', segment"
dim_check dim_products product_id "name || '|' || category || '|' || price::text" \
    "name, '|', category, '|', toDecimalString(price, 2)"

# fact_orders (ADR-0009): lines of existing orders = Postgres. For orders of the M3 generator
# (items since FACT_PRICE_SINCE) price_at_order must equal list_price of the SCD2 version valid
# at order creation, and every line must find a version (no ASOF miss): the end-to-end SCD2 check.
FACT_PRICE_SINCE=${FACT_PRICE_SINCE:-2026-09-29 18:15:00}
compare "fact_orders: rows, sum(amount)" \
    "$(psql "SELECT count(*), coalesce(sum(i.quantity * i.price_at_order), 0.00) FROM order_items i JOIN orders o USING (order_id)")" \
    "$(scripts/ch-query.sh "SELECT count(), toDecimalString(sum(amount), 2) FROM fact_orders
        FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"
compare "fact_orders since ${FACT_PRICE_SINCE% *}: price != list, miss" "0 0" \
    "$(scripts/ch-query.sh "SELECT countIf(price_at_order != list_price), countIf(category IS NULL)
        FROM fact_orders WHERE created_at >= '$FACT_PRICE_SINCE'
        FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"

# mart_revenue_daily (FR-4): per day items, revenue, revenue_net = Postgres (md5 over days).
# Categories are as of the order (SCD2) and Postgres keeps only the current one, so per
# (day, category) the mart is compared with the same aggregate over fact_orders instead.
# Exact once the mart refreshed after the job caught up (every 2 min).
compare "mart_revenue_daily: per day (md5)" \
    "$(psql "SELECT count(*), md5(coalesce(string_agg(d || '|' || i || '|' || r || '|' || n, E'\\n' ORDER BY d), ''))
        FROM (SELECT (o.created_at AT TIME ZONE 'UTC')::date AS d, sum(oi.quantity) AS i,
                     sum(oi.quantity * oi.price_at_order) AS r,
                     coalesce(sum(oi.quantity * oi.price_at_order) FILTER (WHERE o.status <> 'cancelled'), 0.00) AS n
              FROM order_items oi JOIN orders o USING (order_id) GROUP BY 1) t")" \
    "$(scripts/ch-query.sh "SELECT count(), lower(hex(MD5(arrayStringConcat(arrayMap(x -> x.2, arraySort(
                groupArray((d, concat(toString(d), '|', toString(i), '|', toDecimalString(r, 2), '|',
                                      toDecimalString(n, 2)))))), '\n'))))
        FROM (SELECT order_date AS d, sum(items) AS i, sum(revenue) AS r, sum(revenue_net) AS n
              FROM mart_revenue_daily GROUP BY d)
        FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"
compare "mart_revenue_daily vs fact_orders: missing, extra" "0 0" \
    "$(scripts/ch-query.sh "WITH
            fact AS (SELECT order_date, ifNull(category, 'unknown') AS category,
                         uniqExact(order_id) AS orders, sum(quantity) AS items, sum(amount) AS revenue,
                         sumIf(amount, status != 'cancelled') AS revenue_net
                     FROM fact_orders GROUP BY order_date, category),
            mart AS (SELECT order_date, category, orders, items, revenue, revenue_net
                     FROM mart_revenue_daily)
        SELECT (SELECT count() FROM (SELECT * FROM fact EXCEPT SELECT * FROM mart)),
               (SELECT count() FROM (SELECT * FROM mart EXCEPT SELECT * FROM fact))
        FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"
echo "mart_revenue_daily freshness: $(scripts/ch-query.sh "SELECT concat('refreshed_at ', toString(max(refreshed_at)),
    ', source_watermark ', toString(max(source_watermark)),
    ', lag ', toString(dateDiff('second', max(source_watermark), now64(3))), ' s') FROM mart_revenue_daily")"

# mart_funnel_daily (FR-5): per day created, shipped, delivered, cancelled = Postgres by current
# status (md5 over days): shipped and delivered cannot be cancelled and delivered/cancelled are
# final, so "reached" equals the current status there. paid is not comparable (a paid order may
# be cancelled): the history must hold a paid transition for every paid/shipped/delivered order
# and a delivered one for every delivered order.
compare "mart_funnel_daily: per day (md5)" \
    "$(psql "SELECT count(*), md5(coalesce(string_agg(d || '|' || c || '|' || s || '|' || dl || '|' || x, E'\\n' ORDER BY d), ''))
        FROM (SELECT (created_at AT TIME ZONE 'UTC')::date AS d, count(*) AS c,
                     count(*) FILTER (WHERE status IN ('shipped', 'delivered')) AS s,
                     count(*) FILTER (WHERE status = 'delivered') AS dl,
                     count(*) FILTER (WHERE status = 'cancelled') AS x
              FROM orders GROUP BY 1) t")" \
    "$(scripts/ch-query.sh "SELECT count(), lower(hex(MD5(arrayStringConcat(arrayMap(x -> x.2, arraySort(
                groupArray((order_date, concat(toString(order_date), '|', toString(created), '|',
                    toString(shipped), '|', toString(delivered), '|', toString(cancelled)))))), '\n'))))
        FROM mart_funnel_daily
        FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"
compare "status history: missing paid, delivered" "0 0" \
    "$(scripts/ch-query.sh "SELECT
            countIf(status IN ('paid', 'shipped', 'delivered') AND order_id NOT IN
                (SELECT order_id FROM stg_order_status_history WHERE status = 'paid')),
            countIf(status = 'delivered' AND order_id NOT IN
                (SELECT order_id FROM stg_order_status_history WHERE status = 'delivered'))
        FROM stg_orders FINAL WHERE is_deleted = 0
        FORMAT CustomSeparated SETTINGS format_custom_field_delimiter = ' '")"
echo "mart_funnel_daily freshness: $(scripts/ch-query.sh "SELECT concat('refreshed_at ', toString(max(refreshed_at)),
    ', source_watermark ', toString(max(source_watermark)),
    ', lag ', toString(dateDiff('second', max(source_watermark), now64(3))), ' s') FROM mart_funnel_daily")"

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
