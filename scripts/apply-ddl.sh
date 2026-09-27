#!/usr/bin/env bash
# Apply clickhouse/ddl/*.sql in order over HTTP. Every file is idempotent (IF NOT EXISTS),
# so running the script again is safe. Usage: scripts/apply-ddl.sh [ddl_dir]
#
# Connection: CLICKHOUSE_URL (default http://$PI5_HOST:$CLICKHOUSE_HTTP_PORT), CLICKHOUSE_USER,
# CLICKHOUSE_PASSWORD. Missing variables are taken from .env (only these keys, never printed).
# The password goes in a header read from a file descriptor: not in the URL, not visible in ps.
set -euo pipefail

cd "$(dirname "$0")/.."
DDL_DIR=${1:-clickhouse/ddl}

env_value() {  # read one KEY=value from .env without sourcing it
    [[ -f .env ]] && sed -n "s/^$1=//p" .env | tail -n 1
}
for var in PI5_HOST CLICKHOUSE_HTTP_PORT CLICKHOUSE_USER CLICKHOUSE_PASSWORD; do
    [[ -n ${!var:-} ]] || printf -v "$var" '%s' "$(env_value "$var")"
done
CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://${PI5_HOST:?set PI5_HOST}:${CLICKHOUSE_HTTP_PORT:-8123}}
: "${CLICKHOUSE_USER:?set CLICKHOUSE_USER}" "${CLICKHOUSE_PASSWORD:?set CLICKHOUSE_PASSWORD}"

run_file() {
    curl -sS --fail-with-body --retry 3 --retry-connrefused \
        -H @<(printf 'X-ClickHouse-User: %s\nX-ClickHouse-Key: %s\n' "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD") \
        --data-binary @"$1" "$CLICKHOUSE_URL/"
}

shopt -s nullglob
files=("$DDL_DIR"/[0-9][0-9][0-9]_*.sql)
((${#files[@]})) || { echo "no DDL files in $DDL_DIR" >&2; exit 1; }
for f in "${files[@]}"; do
    if out=$(run_file "$f" 2>&1); then
        echo "applied $(basename "$f")"
    else
        echo "FAILED $(basename "$f"): $out" >&2
        exit 1
    fi
done
