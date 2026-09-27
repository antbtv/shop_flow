#!/usr/bin/env bash
# Run one ClickHouse query over HTTP: scripts/ch-query.sh 'SELECT 1'  or  ... < query.sql
# Default database shopflow, output TSV unless the query sets FORMAT. Connection: lib/clickhouse-env.sh.
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/lib/clickhouse-env.sh

if (($#)); then query=$1; else query=$(cat); fi
ch_curl --data-binary "$query" "$CLICKHOUSE_URL/?database=shopflow&default_format=TSV"
