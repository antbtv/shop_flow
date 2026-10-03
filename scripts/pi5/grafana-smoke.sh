#!/usr/bin/env bash
# Grafana smoke test on Pi5 (5.6, ADR-0011). Runs ON Pi5; start it from the laptop:
#   ssh pi5 'bash -s' < scripts/pi5/grafana-smoke.sh
# Service state, the listening address of port 3000, anonymous access, the data source health, a
# panel-style query as grafana_reader, the plugin version, memory and OOM/restarts.
# Read-only: nothing is created or changed. The admin password is read from ~/shopflow/.env and goes
# to curl on stdin (-K -), not in argv, and is never printed.
set -uo pipefail
cd "${SHOPFLOW_DIR:-$HOME/shopflow}"
env_file=${ENV_FILE:-.env}  # test hook; on Pi5 it is .env
host=$(sed -n 's/^PI5_HOST=//p' "$env_file")
pw=$(sed -n 's/^GRAFANA_ADMIN_PASSWORD=//p' "$env_file")
base="http://$host:3000"
# The script itself arrives on stdin (bash -s): compose exec must not read it.
dc() { docker compose --env-file "$env_file" -f docker-compose.pi5.yml "$@" </dev/null; }
auth_curl() { printf 'user = "admin:%s"\n' "$pw" | curl -sS -m 30 -K - "$@"; }

echo "== service"
dc ps grafana --format 'table {{.Service}}\t{{.Status}}'
echo "== listening on :3000 (expected: only $host)"
ss -tln | awk '$4 ~ /:3000$/ {print $4}'
echo "== anonymous access (expected 401)"
curl -s -m 10 -o /dev/null -w '/api/search %{http_code}\n' "$base/api/search"
echo "== health"
curl -s -m 10 "$base/api/health" | tr -d '\n '
echo
echo "== data source health (expected: Data source is working)"
auth_curl "$base/api/datasources/uid/shopflow-clickhouse/health"
echo
echo "== panel-style query as grafana_reader (expected: status 200 and one value)"
auth_curl -H 'Content-Type: application/json' "$base/api/ds/query" -d '{"queries":[{"refId":"A","datasource":{"uid":"shopflow-clickhouse","type":"grafana-clickhouse-datasource"},"rawSql":"SELECT count() AS n FROM shopflow.mart_revenue_daily","format":1,"queryType":"table","editorType":"sql"}],"from":"now-6h","to":"now"}' \
    | python3 -c "
import json, sys
r = json.load(sys.stdin)['results']['A']
print('status', r.get('status'), 'error', r.get('error', '-'), 'values', r['frames'][0]['data']['values'] if 'frames' in r and r['frames'] else '-')"
echo "== dashboards folder"
auth_curl "$base/api/search?type=dash-folder" | python3 -c "import json,sys; print([f['title'] for f in json.load(sys.stdin)])"
echo "== plugin"
dc exec -T grafana grafana cli --pluginsDir /opt/grafana/plugins plugins ls 2>&1 | grep -v '^$'
echo "== memory (limit in MiB: grafana 384, clickhouse 2816)"
docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.MemPerc}}'
echo "== OOM kills and restarts of grafana (expected: false 0)"
docker inspect -f '{{.State.OOMKilled}} {{.RestartCount}}' "$(dc ps -q grafana)"
echo "== host memory"
free -m | sed -n '1,3p'
