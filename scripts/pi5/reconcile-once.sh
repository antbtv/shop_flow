#!/usr/bin/env bash
# One-off reconciliation Postgres (laptop) <-> ClickHouse (Pi5) with the DAG's logic (FR-8,
# ADR-0010), printed instead of stored. Runs ON Pi5 inside the Airflow scheduler; start it from
# the laptop:   ssh pi5 'bash -s' < scripts/pi5/reconcile-once.sh
# Prints counts and sample keys only, no secrets. Extra keys are not rechecked here.
set -euo pipefail
cd ~/shopflow
docker compose -f docker-compose.pi5.yml exec -T airflow-scheduler python - <<'PY' 2>&1 | grep -v -e Warning -e '^\s*$'
from shopflow_checks.reconciliation import TABLES, pipeline_lagging, postgres_cutoff, reconcile_table
from shopflow_common.connections import clickhouse_client, postgres_connect

pg_conn = postgres_connect()
client = clickhouse_client()


def pg(sql, params=None):
    with pg_conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def ch(sql):
    return [tuple(row) for row in client.query(sql).result_rows]


cutoff = postgres_cutoff(pg)
print("cutoff", cutoff.isoformat(), "lagging", pipeline_lagging(pg, ch, cutoff))
for spec in TABLES:
    r = reconcile_table(spec, pg, ch, cutoff)
    print(f"{spec.name:12} pg={r.pg_rows} ch={r.ch_rows} buckets_differ={r.buckets_differ}"
          f" in_flight={r.in_flight} violations={r.violations} {r.details() if r.violations else ''}")
PY
