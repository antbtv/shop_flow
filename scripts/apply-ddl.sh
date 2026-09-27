#!/usr/bin/env bash
# Apply clickhouse/ddl/*.sql in order over HTTP. Every file is idempotent (IF NOT EXISTS),
# so running the script again is safe. Usage: scripts/apply-ddl.sh [ddl_dir]
# Connection settings: scripts/lib/clickhouse-env.sh.
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/lib/clickhouse-env.sh
DDL_DIR=${1:-clickhouse/ddl}

shopt -s nullglob
files=("$DDL_DIR"/[0-9][0-9][0-9]_*.sql)
((${#files[@]})) || { echo "no DDL files in $DDL_DIR" >&2; exit 1; }
for f in "${files[@]}"; do
    if out=$(ch_curl --data-binary @"$f" "$CLICKHOUSE_URL/" 2>&1); then
        echo "applied $(basename "$f")"
    else
        echo "FAILED $(basename "$f"): $out" >&2
        exit 1
    fi
done
