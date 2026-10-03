#!/usr/bin/env bash
# Milestone 5.12, laptop side: the generator, "viewers" of the dashboard and the memory of the
# laptop containers. Start it, then right away the Pi5 side in another terminal:
#   scripts/loadtest-m5-laptop.sh
#   ssh pi5 'bash -s' < scripts/pi5/loadtest-m5.sh
# Viewers run scripts/check_dashboard.py every 30 s each (all panel queries through Grafana, as a
# browser with auto-refresh does), VIEWERS of them, staggered. Prints peaks of spark, kafka, connect,
# postgres against their limits, memory.events deltas, restarts, and what the viewers saw.
# The Grafana password is read from .env (key GRAFANA_ADMIN_PASSWORD) and never printed.
# Overrides: DURATION_SEC, RATE, SEED, VIEWERS, ENV_FILE, GRAFANA_URL, GRAFANA_PASSWORD,
# NO_GENERATOR=1 (tests), COMPOSE_PROJECT.
set -uo pipefail
export LC_ALL=C
cd "$(dirname "$0")/.."
DURATION_SEC=${DURATION_SEC:-900}
RATE=${RATE:-5}
SEED=${SEED:-25}
VIEWERS=${VIEWERS:-2}
PERIOD=30 # Grafana auto-refresh of the dashboard
SAMPLE_SEC=5
PROJECT=${COMPOSE_PROJECT:-shopflow}
SERVICES="spark kafka connect postgres"

env_value() { local file=${ENV_FILE:-.env}; [[ -f $file ]] && sed -n "s/^$1=//p" "$file" | tail -n 1 | tr -d '\r'; }
GRAFANA_PASSWORD=${GRAFANA_PASSWORD:-$(env_value GRAFANA_ADMIN_PASSWORD)}
GRAFANA_URL=${GRAFANA_URL:-http://$(env_value PI5_HOST):3000}
[[ -n $GRAFANA_PASSWORD ]] || { echo "GRAFANA_ADMIN_PASSWORD is not in the env file" >&2; exit 2; }
export GRAFANA_PASSWORD GRAFANA_URL
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

cg() { echo "/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "$PROJECT-$1-1").scope"; }
events() { awk -v k="$2" '$1 == k {print $2}' "$(cg "$1")/memory.events"; }
restarts() { docker inspect -f '{{.RestartCount}}' "$PROJECT-$1-1"; }

declare -A max0 oom0 rst0 peak
for s in $SERVICES; do max0[$s]=$(events "$s" max); oom0[$s]=$(events "$s" oom_kill); rst0[$s]=$(restarts "$s"); done
START=$(date -u +%T)
echo "$START start: ${DURATION_SEC}s, generator rate $RATE seed $SEED, $VIEWERS viewer(s) every ${PERIOD}s"

if [[ ${NO_GENERATOR:-0} != 1 ]]; then
    docker compose -f docker-compose.laptop.yml --profile generator run --rm generator \
        --duration "$DURATION_SEC" --rate "$RATE" --seed "$SEED" --no-seasonality \
        >"$WORK/generator.log" 2>&1 &
    gen_pid=$!
fi

end=$((SECONDS + DURATION_SEC))
viewer() {  # n: one browser tab with auto-refresh
    local n=$1 t0 t1
    sleep $(( (n - 1) * PERIOD / VIEWERS ))
    while [ $SECONDS -lt $end ]; do
        t0=$SECONDS
        python3 scripts/check_dashboard.py >"$WORK/view-$n.out" 2>&1; rc=$?
        t1=$SECONDS
        echo "$rc $((t1 - t0))" >>"$WORK/viewer-$n.log"
        grep '^FAIL' "$WORK/view-$n.out" >>"$WORK/fails-$n.log" || true
        [ $((PERIOD - (t1 - t0))) -gt 0 ] && sleep $((PERIOD - (t1 - t0)))
    done
}
for n in $(seq "$VIEWERS"); do viewer "$n" & done

min_avail=999999; max_swap=0
while [ $SECONDS -lt $end ]; do
    while read -r name mem; do
        mib=$(echo "$mem" | awk '{v=$1+0; if ($1 ~ /GiB/) v*=1024; else if ($1 ~ /KiB/) v/=1024; else if ($1 !~ /MiB/) v/=1048576; printf "%d", v}')
        [ "${peak[$name]:-0}" -lt "$mib" ] && peak[$name]=$mib
    done < <(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep "^$PROJECT-" | sed "s/^$PROJECT-//; s/-1 / /; s| /.*||")
    read -r avail swap < <(free -m | awk '/^Mem/{a=$7} /^Swap/{s=$3} END{print a, s}')
    [ "$avail" -lt "$min_avail" ] && min_avail=$avail
    [ "$swap" -gt "$max_swap" ] && max_swap=$swap
    sleep $SAMPLE_SEC
done
wait
[[ -n ${gen_pid:-} ]] && { wait "$gen_pid"; echo "generator exit code: $?"; }

echo "== $START .. $(date -u +%T) UTC. Peak memory of laptop containers, MiB (limit):"
for s in $SERVICES; do
    lim=$(docker inspect -f '{{.HostConfig.Memory}}' "$PROJECT-$s-1" | awk '{printf "%dm", $1/1048576}')
    printf '%-10s %5s  (%s)  max+%s oom_kill+%s restarts+%s OOMKilled=%s\n' "$s" "${peak[$s]:-?}" "$lim" \
        $(( $(events "$s" max) - max0[$s] )) $(( $(events "$s" oom_kill) - oom0[$s] )) \
        $(( $(restarts "$s") - rst0[$s] )) "$(docker inspect -f '{{.State.OOMKilled}}' "$PROJECT-$s-1")"
done
echo "laptop: min available ${min_avail} MiB, max swap used ${max_swap} MiB"
echo "== spark log: errors since start (expected 0)"
docker logs --since "${DURATION_SEC}s" "$PROJECT-spark-1" 2>&1 | grep -ciE 'error|exception|quarantin' || true

echo "== viewers (dashboard queries through Grafana):"
runs=0; bad=0; maxdur=0
for n in $(seq "$VIEWERS"); do
    [[ -f $WORK/viewer-$n.log ]] || continue
    while read -r rc dur; do
        runs=$((runs + 1)); [ "$rc" != 0 ] && bad=$((bad + 1)); [ "$dur" -gt "$maxdur" ] && maxdur=$dur
    done <"$WORK/viewer-$n.log"
done
echo "check_dashboard runs: $runs, with failed panels: $bad, longest run: ${maxdur}s (period ${PERIOD}s)"
if ls "$WORK"/fails-*.log >/dev/null 2>&1; then
    echo "failed panels (unique, first 10):"; sort -u "$WORK"/fails-*.log | cut -c1-200 | head -10
fi
