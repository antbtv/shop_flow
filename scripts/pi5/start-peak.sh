#!/usr/bin/env bash
# Start peak of one Pi5 service, separating process memory from page cache (5.12).
# memory.peak mixes both, `docker stats` every 5 s misses short spikes. This recreates the
# service and samples its cgroup (memory.stat anon and file, memory.current) every ~0.3 s.
#   ssh pi5 'bash -s -- airflow-dag-processor 90' < scripts/pi5/start-peak.sh
#   ssh pi5 'DAG_PROCESSOR_PARSING_PROCESSES=2 bash -s -- airflow-dag-processor 90' < scripts/pi5/start-peak.sh
# Same method for every variant: always `up -d --force-recreate --no-deps` (not `restart`).
# Overrides (tests on a laptop stand): REPO_DIR, PROJECT, COMPOSE_EXTRA.
set -euo pipefail
export LC_ALL=C
service=${1:?service name, e.g. airflow-dag-processor}
seconds=${2:-90}
cd "${REPO_DIR:-$HOME/shopflow}"
PROJECT=${PROJECT:-shopflow}
# shellcheck disable=SC2086
C="docker compose -p $PROJECT -f docker-compose.pi5.yml ${COMPOSE_EXTRA:-}"

$C up -d --force-recreate --no-deps "$service" >/dev/null 2>&1
d=/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "$PROJECT-$service-1").scope
limit=$(( $(docker inspect -f '{{.HostConfig.Memory}}' "$PROJECT-$service-1") / 1048576 ))
end=$((SECONDS + seconds))
max_anon=0; max_file=0; max_cur=0; samples=0
while [ $SECONDS -lt $end ]; do
    read -r anon file < <(awk '$1=="anon"{a=$2} $1=="file"{f=$2} END{printf "%d %d\n", a/1048576, f/1048576}' "$d/memory.stat")
    cur=$(( $(cat "$d/memory.current") / 1048576 ))
    [ "$anon" -gt "$max_anon" ] && max_anon=$anon
    [ "$file" -gt "$max_file" ] && max_file=$file
    [ "$cur" -gt "$max_cur" ] && max_cur=$cur
    samples=$((samples + 1))
    sleep 0.3
done
echo "service $service (limit ${limit} MiB), $samples samples in ${seconds}s"
echo "max anon (processes): ${max_anon} MiB, max file (cache): ${max_file} MiB, max current: ${max_cur} MiB"
echo "memory.peak: $(( $(cat "$d/memory.peak") / 1048576 )) MiB; $(grep -E '^max ' "$d/memory.events"); $(grep -E '^oom_kill ' "$d/memory.events")"
echo "final: anon $(awk '$1=="anon"{printf "%d", $2/1048576}' "$d/memory.stat") MiB"
env_line=$($C exec -T "$service" printenv AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES </dev/null 2>/dev/null || true)
[ -n "$env_line" ] && echo "AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=$env_line"
exit 0
