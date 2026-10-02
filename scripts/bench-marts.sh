#!/usr/bin/env bash
# Milestone 4.16 (from M3): refreshable MVs and fact_orders at ~1M orders against their memory
# limits (512 MiB dimensions, 768 MiB marts, ADR-0009) on Pi5. Builds a separate database
# shopflow_bench from the same DDL, fills it with synthetic data via numbers(), refreshes every
# MV by hand and reads time and memory from system.query_log / system.view_refreshes.
# Production tables are not touched. Remove afterwards, by hand: DROP DATABASE shopflow_bench
# Usage: scripts/bench-marts.sh [orders]   (default 1000000; 3 lines per order)
set -euo pipefail
cd "$(dirname "$0")/.."
ORDERS=${1:-1000000}
DB=shopflow_bench
q() { scripts/ch-query.sh "$1"; }

ddl=$(mktemp -d)
trap 'rm -r "$ddl"' EXIT
for f in clickhouse/ddl/*.sql; do
    sed -e "s/shopflow\./$DB./g" -e "s/DATABASE IF NOT EXISTS shopflow;/DATABASE IF NOT EXISTS $DB;/" \
        "$f" > "$ddl/$(basename "$f")"
done
scripts/apply-ddl.sh "$ddl" >/dev/null
echo "schema: $(q "SELECT count() FROM system.tables WHERE database = '$DB'") objects in $DB"

# Synthetic history over 14 days from 2026-09-01; version (LSN) grows with time in every table.
BASE="toDateTime64('2026-09-01 00:00:00', 6, 'UTC')"
SPAN=1209600  # 14 days, s
t0=$(date +%s)
q "INSERT INTO $DB.stg_product_versions (product_id, valid_from, name, category, price,
       is_snapshot, is_deleted, event_time, version)
   SELECT n % 2000 + 1 AS product_id,
          $BASE + toIntervalMicrosecond(intDiv(n, 2000) * intDiv($SPAN, 10) * 1000000 + n % 2000) AS valid_from,
          concat('p', toString(product_id)), concat('cat', toString(product_id % 20)),
          toDecimal32(1 + (cityHash64(n) % 100000) / 100, 2), intDiv(n, 2000) = 0, 0,
          valid_from, n + 1
   FROM (SELECT number AS n FROM numbers(20000))"
q "INSERT INTO $DB.stg_customer_versions (customer_id, valid_from, name, address, segment,
       is_snapshot, is_deleted, event_time, version)
   SELECT n % 50000 + 1 AS customer_id,
          $BASE + toIntervalMicrosecond(intDiv(n, 50000) * intDiv($SPAN, 3) * 1000000 + n % 50000) AS valid_from,
          concat('c', toString(customer_id)), concat('street ', toString(n % 97)),
          ['retail', 'vip', 'b2b'][n % 3 + 1], intDiv(n, 50000) = 0, 0, valid_from, n + 1
   FROM (SELECT number AS n FROM numbers(150000))"
q "INSERT INTO $DB.stg_orders (order_id, customer_id, status, created_at, updated_at, version,
       is_deleted, event_time)
   SELECT n + 1, n % 50000 + 1, ['created', 'paid', 'shipped', 'delivered', 'cancelled'][n % 5 + 1],
          $BASE + toIntervalMicrosecond(intDiv(n * $SPAN * 1000000, $ORDERS)) AS created_at,
          created_at, 10000000 + n, 0, created_at
   FROM (SELECT number AS n FROM numbers($ORDERS))"
q "INSERT INTO $DB.stg_order_items (order_item_id, order_id, product_id, quantity, price_at_order,
       version, is_deleted, event_time)
   SELECT n + 1, intDiv(n, 3) + 1, cityHash64(n) % 2000 + 1, toInt32(n % 3 + 1),
          toDecimal32(1 + (cityHash64(n, 1) % 100000) / 100, 2), 20000000 + n, 0,
          $BASE + toIntervalMicrosecond(intDiv(intDiv(n, 3) * $SPAN * 1000000, $ORDERS))
   FROM (SELECT number AS n FROM numbers($ORDERS * 3))"
# History: created, then each later status up to the current one; cancelled orders were paid.
q "INSERT INTO $DB.stg_order_status_history (order_id, status, changed_at, is_snapshot, version)
   SELECT order_id, status, created_at + toIntervalMinute(step * 30), 0, 30000000 + order_id * 5 + step
   FROM (
       SELECT number + 1 AS order_id,
              $BASE + toIntervalMicrosecond(intDiv(number * $SPAN * 1000000, $ORDERS)) AS created_at,
              arrayJoin(arraySlice([('created', 0), ('paid', 1), ('shipped', 2), ('delivered', 3)],
                                   1, if(number % 5 = 4, 2, number % 5 + 1))) AS st,
              st.1 AS status, st.2 AS step
       FROM numbers($ORDERS)
       UNION ALL
       SELECT number + 1, $BASE + toIntervalMicrosecond(intDiv(number * $SPAN * 1000000, $ORDERS)),
              ('cancelled', 4), 'cancelled', 4
       FROM numbers($ORDERS) WHERE number % 5 = 4
   )"
q "INSERT INTO $DB.stg_inventory (product_id, warehouse_id, quantity, updated_at, event_time,
       version, is_deleted)
   SELECT n % 2000 + 1, toUInt32(intDiv(n, 2000) + 1), toInt32(n % 200), $BASE, $BASE, n + 1, 0
   FROM (SELECT number AS n FROM numbers(6000))"
echo "load: $(( $(date +%s) - t0 )) s"
q "SELECT table, sum(rows), formatReadableSize(sum(bytes_on_disk)) FROM system.parts
   WHERE database = '$DB' AND active AND table LIKE 'stg_%' GROUP BY table ORDER BY table
   FORMAT PrettyCompactMonoBlock"

start=$(q "SELECT now()")
for mv in dim_customers_mv dim_products_mv mart_revenue_daily_mv mart_funnel_daily_mv; do
    q "SYSTEM REFRESH VIEW $DB.$mv"
    q "SYSTEM WAIT VIEW $DB.$mv" || true
done
q "SELECT view, status, last_success_duration_ms, exception FROM system.view_refreshes
   WHERE database = '$DB' ORDER BY view FORMAT PrettyCompactMonoBlock"

for _ in 1 2 3; do
    q "SELECT count(), sum(amount), uniqExact(category), countIf(list_price IS NULL)
       FROM $DB.fact_orders SETTINGS log_comment = 'bench fact_orders full scan'" >/dev/null
done
q "SELECT order_date, category, sum(amount) FROM $DB.fact_orders
   WHERE order_date >= '2026-09-10' GROUP BY order_date, category FORMAT Null
   SETTINGS log_comment = 'bench fact_orders dashboard query'"
q "SYSTEM FLUSH LOGS"
echo "query_log since $start (refresh = the INSERT of each MV into its temporary table):"
q "SELECT if(log_comment != '', log_comment,
             arrayStringConcat(arrayFilter(t -> t LIKE '$DB.%', tables), ' ')) AS what,
          type, query_duration_ms, formatReadableSize(memory_usage) AS memory, read_rows
   FROM system.query_log
   WHERE event_time >= '$start' AND type != 'QueryStart' AND has(databases, '$DB')
     AND (log_comment LIKE 'bench%' OR query_kind = 'Insert')
   ORDER BY event_time_microseconds FORMAT PrettyCompactMonoBlock"
echo "dimension and mart sizes:"
q "SELECT 'dim_products', count(), countIf(is_current) FROM $DB.dim_products
   UNION ALL SELECT 'dim_customers', count(), countIf(is_current) FROM $DB.dim_customers
   UNION ALL SELECT 'mart_revenue_daily', count(), 0 FROM $DB.mart_revenue_daily
   UNION ALL SELECT 'mart_funnel_daily', count(), sum(created) FROM $DB.mart_funnel_daily
   FORMAT PrettyCompactMonoBlock"
