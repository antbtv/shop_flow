#!/usr/bin/env bash
# Create or update ClickHouse service users (ADR-0007, ADR-0008). Idempotent: every run converges
# the user to the password, host subnet, profile and grants below. Runs as $CLICKHOUSE_USER.
# spark_writer: INSERT (+ SELECT for the connector) only on the tables Spark writes, LAN subnet only.
# The password never leaves this machine: only its sha256 goes to the server.
# Usage: scripts/create-ch-users.sh   (CLICKHOUSE_SPARK_PASSWORD, LAN_SUBNET from env or .env)
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/lib/clickhouse-env.sh
for var in CLICKHOUSE_SPARK_PASSWORD LAN_SUBNET; do
    [[ -n ${!var:-} ]] || printf -v "$var" '%s' "$(env_value "$var")"
done
: "${CLICKHOUSE_SPARK_PASSWORD:?set CLICKHOUSE_SPARK_PASSWORD}" "${LAN_SUBNET:?set LAN_SUBNET}"
[[ $LAN_SUBNET =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}/[0-9]{1,2}$ ]] || { echo "LAN_SUBNET must be a CIDR" >&2; exit 1; }

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
    # The connector reads the table schema with SELECT and the cluster topology when the
    # catalog loads (spike 2.4). system.tables/columns/databases are filtered by grants.
    "GRANT SELECT ON shopflow.raw_events TO spark_writer"
    "GRANT SELECT ON shopflow.stg_orders TO spark_writer"
    "GRANT SELECT ON shopflow.stg_order_items TO spark_writer"
    "GRANT SELECT ON shopflow.stg_customer_versions TO spark_writer"
    "GRANT SELECT ON shopflow.stg_product_versions TO spark_writer"
    "GRANT SELECT ON shopflow.stg_order_status_history TO spark_writer"
    "GRANT SELECT ON shopflow.stg_inventory TO spark_writer"
    "GRANT SELECT ON system.clusters TO spark_writer"
    "GRANT SELECT ON system.macros TO spark_writer"
)
for q in "${statements[@]}"; do
    # Print the first words only: the hash stays out of terminal logs.
    label=$(cut -d' ' -f1-4 <<<"$q")
    if out=$(ch_curl --data-binary "$q" "$CLICKHOUSE_URL/" 2>&1); then
        echo "ok: $label"
    else
        echo "FAILED: $label: ${out//$hash/<hash>}" >&2
        exit 1
    fi
done
