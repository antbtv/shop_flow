#!/usr/bin/env bash
# Drive Airflow on Pi5 through its REST API (Airflow 3.1 has no CLI to set a task state).
# Runs ON Pi5; the admin password is read from ~/shopflow/.env and never printed. From the laptop:
#   ssh pi5 'bash -s -- trigger shopflow_reconciliation' < scripts/pi5/airflow-api.sh
#   ssh pi5 'bash -s -- states shopflow_reconciliation <run_id>' < scripts/pi5/airflow-api.sh
#   ssh pi5 'bash -s -- set-state shopflow_reconciliation <run_id> wait_for_postgres skipped' < ...
# trigger unpauses the DAG (it then also runs on its schedule), waits up to WAIT_S (default
# 1800 s) for the run to finish and prints the task states. SKIP_WAIT=1 (test of "laptop off")
# turns wait_for_postgres to skipped as soon as it is waiting. key=value arguments after the DAG id
# are the run conf:  ssh pi5 'bash -s -- trigger shopflow_data_quality simulate_violation=true' < ...
set -euo pipefail
cd ~/shopflow
env_value() { sed -n "s/^$1=//p" .env | tail -n 1; }
API="http://$(env_value PI5_HOST):8080"
TOKEN=$(python3 - "$(env_value AIRFLOW_ADMIN_USER)" "$API" <<PY
import json, sys, urllib.request
body = json.dumps({"username": sys.argv[1], "password": """$(env_value AIRFLOW_ADMIN_PASSWORD)"""}).encode()
req = urllib.request.Request(sys.argv[2] + "/auth/token", body, {"Content-Type": "application/json"})
print(json.load(urllib.request.urlopen(req))["access_token"])
PY
)
call() {  # method path [json]
    curl -sS --fail-with-body -X "$1" -H "Authorization: Bearer $TOKEN" \
        -H 'Content-Type: application/json' "$API/api/v2$2" ${3:+-d "$3"}
}
states() {
    call GET "/dags/$1/dagRuns/$2/taskInstances" | python3 -c '
import json, sys
for ti in json.load(sys.stdin)["task_instances"]:
    print("  %-20s %s  try=%s" % (ti["task_id"], ti["state"], ti["try_number"]))'
}

case "${1:-}" in
trigger)
    dag=$2
    call PATCH "/dags/$dag" '{"is_paused": false}' >/dev/null
    # key=value arguments after the DAG id become the run conf (true/false/numbers as JSON).
    body=$(python3 -c '
import json, sys
conf = {}
for arg in sys.argv[1:]:
    k, _, v = arg.partition("=")
    try:
        conf[k] = json.loads(v)
    except ValueError:
        conf[k] = v
print(json.dumps({"logical_date": None, "conf": conf}))' "${@:3}")
    run=$(call POST "/dags/$dag/dagRuns" "$body" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["dag_run_id"])')
    echo "run_id $run"
    if [[ ${SKIP_WAIT:-0} == 1 ]]; then
        # Test of "laptop off": once wait_for_postgres is waiting, end the wait as its 2 h
        # timeout would (soft_fail -> skipped) instead of sitting it out.
        for _ in $(seq 40); do
            state=$(call GET "/dags/$dag/dagRuns/$run/taskInstances/wait_for_postgres" \
                | python3 -c 'import json, sys; print(json.load(sys.stdin)["state"])')
            [[ $state == up_for_reschedule ]] && break
            sleep 3
        done
        echo "wait_for_postgres $state"
        if [[ $state == up_for_reschedule ]]; then
            call PATCH "/dags/$dag/dagRuns/$run/taskInstances/wait_for_postgres" \
                '{"new_state": "skipped"}' >/dev/null
            echo "wait_for_postgres -> skipped"
        fi
    fi
    for _ in $(seq $(( ${WAIT_S:-1800} / 15 ))); do
        state=$(call GET "/dags/$dag/dagRuns/$run" \
            | python3 -c 'import json, sys; print(json.load(sys.stdin)["state"])')
        [[ $state == success || $state == failed ]] && break
        sleep 15
    done
    echo "dag_run $state"
    states "$dag" "$run"
    ;;
states) states "$2" "$3" ;;
set-state)
    call PATCH "/dags/$2/dagRuns/$3/taskInstances/$4" "{\"new_state\": \"$5\"}" >/dev/null
    echo "set $4 -> $5"
    ;;
*) echo "usage: trigger <dag> | states <dag> <run> | set-state <dag> <run> <task> <state>" >&2; exit 2 ;;
esac
