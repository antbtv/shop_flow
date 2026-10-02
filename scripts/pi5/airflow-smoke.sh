#!/usr/bin/env bash
# Airflow smoke test on Pi5 (4.7, ADR-0010). Runs ON Pi5; start it from the laptop:
#   ssh pi5 'bash -s' < scripts/pi5/airflow-smoke.sh
# 1. DAG import errors and the healthcheck DAG run end to end.
# 2. Laptop unreachable (closed port) -> SourceUnavailable (via shopflow_common.connections).
# 3. Server rejects (pg_hba) -> plain OperationalError, never SourceUnavailable.
# 4. Connection passwords are masked in logs.
# Overrides apply to one exec only; no files or connections change. No secrets are printed.
set -uo pipefail
cd ~/shopflow
# The script itself arrives on stdin (bash -s): exec must not read it, so stdin is /dev/null
# unless a step passes its own input (exe_in).
exe() { docker compose -f docker-compose.pi5.yml exec -T "$@" </dev/null; }
exe_in() { docker compose -f docker-compose.pi5.yml exec -T "$@"; }
laptop=$(sed -n 's/^LAPTOP_HOST=//p' .env)
pg_conn() {  # port, login: a throwaway connection that differs only in what is being tested
    printf '{"conn_type": "postgres", "host": "%s", "port": %s, "schema": "shopflow", "login": "%s", "password": "x", "extra": {"connect_timeout": "10"}}' "$laptop" "$1" "$2"
}

echo "== import errors"
exe airflow-scheduler airflow dags list-import-errors 2>&1 | grep -v Warning
echo "== 1. dags test shopflow_healthcheck"
exe airflow-scheduler airflow dags test shopflow_healthcheck 2>&1 \
    | grep -E "Marking task|Done\. Returned value was|Error|Exception" | tail -6
# Steps 2-3 call the function the DAGs use and print the exception type and message as is.
probe_pg() {
    exe_in -e AIRFLOW_CONN_SHOPFLOW_POSTGRES="$(pg_conn "$1" "$2")" airflow-scheduler python - <<'PY' 2>&1 | grep '^result'
from shopflow_common.connections import postgres_connect
try:
    postgres_connect()
    print("result connected")
except Exception as exc:
    print("result", type(exc).__name__, "|", " ".join(str(exc).split())[:160])
PY
}
echo "== 2. laptop unreachable (port 5433)"
probe_pg 5433 recon_reader
echo "== 3. pg_hba rejects user shopflow"
probe_pg 5432 shopflow
echo "== 4. password masking"
exe_in airflow-scheduler python - <<'PY' 2>&1 | grep '^mask'
from airflow.sdk import BaseHook
from airflow.sdk.execution_time.secrets_masker import redact
for conn_id in ("shopflow_clickhouse", "shopflow_postgres"):
    pw = BaseHook.get_connection(conn_id).password
    print("mask", conn_id, redact(pw), redact("pw=" + pw))
PY
