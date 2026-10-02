#!/usr/bin/env bash
# Create or update ClickHouse service users (ADR-0007, ADR-0008, ADR-0010). Idempotent: every run
# converges the users to the passwords, host subnets, profiles and grants below. Runs as $CLICKHOUSE_USER.
# spark_writer: INSERT (+ SELECT for the connector) only on the tables Spark writes, LAN subnet only.
# airflow_reader: SELECT for the checks, INSERT only into dq_check_results, Pi5 compose network only.
# Skipped while PI5_COMPOSE_SUBNET is empty (it is pinned when Airflow is deployed, 4.5).
# Passwords never leave this machine: only their sha256 goes to the server.
# Usage: scripts/create-ch-users.sh   (CLICKHOUSE_SPARK_PASSWORD, LAN_SUBNET, CLICKHOUSE_AIRFLOW_PASSWORD,
#        PI5_COMPOSE_SUBNET from env or .env)
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/lib/clickhouse-env.sh
for var in CLICKHOUSE_SPARK_PASSWORD LAN_SUBNET CLICKHOUSE_AIRFLOW_PASSWORD PI5_COMPOSE_SUBNET; do
    [[ -n ${!var:-} ]] || printf -v "$var" '%s' "$(env_value "$var")"
done
: "${CLICKHOUSE_SPARK_PASSWORD:?set CLICKHOUSE_SPARK_PASSWORD}" "${LAN_SUBNET:?set LAN_SUBNET}"
cidr='^[0-9]{1,3}(\.[0-9]{1,3}){3}/[0-9]{1,2}$'
[[ $LAN_SUBNET =~ $cidr ]] || { echo "LAN_SUBNET must be a CIDR" >&2; exit 1; }

# printf is a builtin: the password does not appear in ps.
hash=$(printf '%s' "$CLICKHOUSE_SPARK_PASSWORD" | sha256sum | cut -d' ' -f1)

statements=(
    # Inserts must not compete with merges for the 2 GB server budget (ADR-0004).
    "CREATE SETTINGS PROFILE IF NOT EXISTS spark_writer_profile SETTINGS max_memory_usage = 536870912"
    "ALTER SETTINGS PROFILE spark_writer_profile SETTINGS max_memory_usage = 536870912"
    "CREATE USER IF NOT EXISTS spark_writer IDENTIFIED WITH sha256_hash BY '$hash' HOST IP '$LAN_SUBNET'"
    "ALTER USER spark_writer IDENTIFIED WITH sha256_hash BY '$hash' HOST IP '$LAN_SUBNET' SETTINGS PROFILE 'spark_writer_profile'"
    "REVOKE ALL ON *.* FROM spark_writer"
    "GRANT INSERT ON shopflow.raw_events TO spark_writer"
    "GRANT INSERT ON shopflow.stg_orders TO spark_writer"
    "GRANT INSERT ON shopflow.stg_order_items TO spark_writer"
    # M3 journals and staging (ADR-0009). dim_*, fact_orders and marts are built by ClickHouse.
    "GRANT INSERT ON shopflow.stg_customer_versions TO spark_writer"
    "GRANT INSERT ON shopflow.stg_product_versions TO spark_writer"
    "GRANT INSERT ON shopflow.stg_order_status_history TO spark_writer"
    "GRANT INSERT ON shopflow.stg_inventory TO spark_writer"
    # The connector needs SELECT on every table it writes: loadTable queries all columns to read
    # the schema (spike 2.4; rechecked in 4.4: INSERT alone gives ACCESS_DENIED on append), and
    # the cluster topology when the catalog loads. system.tables/columns/databases are filtered
    # by grants. Spark reads data only from raw_events (backfill, 3.8).
    "GRANT SELECT ON shopflow.raw_events TO spark_writer"
    "GRANT SELECT ON shopflow.stg_orders TO spark_writer"
    "GRANT SELECT ON shopflow.stg_order_items TO spark_writer"
    "GRANT SELECT ON shopflow.stg_customer_versions TO spark_writer"
    "GRANT SELECT ON shopflow.stg_product_versions TO spark_writer"
    "GRANT SELECT ON shopflow.stg_order_status_history TO spark_writer"
    "GRANT SELECT ON shopflow.stg_inventory TO spark_writer"
    "GRANT SELECT ON system.clusters TO spark_writer"
    "GRANT SELECT ON system.macros TO spark_writer"
    # Reading (backfill from raw_events, 3.8): the connector plans one task per partition.
    "GRANT SELECT(partition, partition_id, rows, bytes_on_disk, database, table, active) ON system.parts TO spark_writer"
)

if [[ -z ${PI5_COMPOSE_SUBNET:-} ]]; then
    echo "skip airflow_reader: PI5_COMPOSE_SUBNET is not set (pinned in 4.5)"
else
    : "${CLICKHOUSE_AIRFLOW_PASSWORD:?set CLICKHOUSE_AIRFLOW_PASSWORD}"
    [[ $PI5_COMPOSE_SUBNET =~ $cidr ]] || { echo "PI5_COMPOSE_SUBNET must be a CIDR" >&2; exit 1; }
    airflow_hash=$(printf '%s' "$CLICKHOUSE_AIRFLOW_PASSWORD" | sha256sum | cut -d' ' -f1)
    statements+=(
        # No readonly: readonly = 2 forbids INSERT even with a grant. Grants limit what it touches,
        # constraints limit what a check may cost: Pi5 is CPU-bound, Spark inserts and refreshes
        # must keep their share (ADR-0004, ADR-0010).
        # A big GROUP BY spills to disk past 256 MiB instead of failing at 512 MiB (bench 4.17:
        # DQ checks over 3M lines); daily checks can afford the HDD.
        "CREATE SETTINGS PROFILE IF NOT EXISTS airflow_reader_profile SETTINGS max_memory_usage = 536870912 MAX 536870912, max_execution_time = 120 MAX 120, max_threads = 2 MAX 2, max_bytes_before_external_group_by = 268435456, max_bytes_before_external_sort = 268435456"
        "ALTER SETTINGS PROFILE airflow_reader_profile SETTINGS max_memory_usage = 536870912 MAX 536870912, max_execution_time = 120 MAX 120, max_threads = 2 MAX 2, max_bytes_before_external_group_by = 268435456, max_bytes_before_external_sort = 268435456"
        "CREATE USER IF NOT EXISTS airflow_reader IDENTIFIED WITH sha256_hash BY '$airflow_hash' HOST IP '$PI5_COMPOSE_SUBNET'"
        "ALTER USER airflow_reader IDENTIFIED WITH sha256_hash BY '$airflow_hash' HOST IP '$PI5_COMPOSE_SUBNET' SETTINGS PROFILE 'airflow_reader_profile'"
        "REVOKE ALL ON *.* FROM airflow_reader"
        # raw_events: lag check before reconciliation, (key, version) duplicates, retention (NFR-5).
        "GRANT SELECT ON shopflow.raw_events TO airflow_reader"
        "GRANT SELECT ON shopflow.stg_orders TO airflow_reader"
        "GRANT SELECT ON shopflow.stg_order_items TO airflow_reader"
        "GRANT SELECT ON shopflow.stg_inventory TO airflow_reader"
        "GRANT SELECT ON shopflow.stg_customer_versions TO airflow_reader"
        "GRANT SELECT ON shopflow.stg_product_versions TO airflow_reader"
        "GRANT SELECT ON shopflow.stg_order_status_history TO airflow_reader"
        "GRANT SELECT ON shopflow.dim_customers TO airflow_reader"
        "GRANT SELECT ON shopflow.dim_products TO airflow_reader"
        # A view runs with the caller's rights: its sources (stg_*, dim_products) are granted above.
        "GRANT SELECT ON shopflow.fact_orders TO airflow_reader"
        "GRANT SELECT ON shopflow.mart_revenue_daily TO airflow_reader"
        "GRANT SELECT ON shopflow.mart_funnel_daily TO airflow_reader"
        # The previous run's status (two days of source_unavailable) is read back from here.
        "GRANT SELECT, INSERT ON shopflow.dq_check_results TO airflow_reader"
        "GRANT SELECT(database, table, partition, rows, bytes_on_disk, active, min_time, max_time) ON system.parts TO airflow_reader"
    )
fi

for q in "${statements[@]}"; do
    # Print the first words only: the hashes stay out of terminal logs.
    label=$(cut -d' ' -f1-4 <<<"$q")
    # Query on stdin, not in argv: the hashes must not show up in ps.
    if out=$(ch_curl --data-binary @- "$CLICKHOUSE_URL/" <<<"$q" 2>&1); then
        echo "ok: $label"
    else
        out=${out//$hash/<hash>}
        echo "FAILED: $label: ${out//${airflow_hash:-$hash}/<hash>}" >&2
        exit 1
    fi
done
