#!/usr/bin/env bash
# Milestone 4.15: memory of the Pi5 stack with all three check DAGs running next to the stream
# (NFR-7, ADR-0004). Runs ON Pi5; start it from the laptop while the generator runs there:
#   ssh pi5 'bash -s' < scripts/pi5/loadtest-m4.sh
# Triggers reconciliation, data quality and retention at the start and again at DURATION/2,
# samples `docker stats` every 5 s, then prints peaks, memory.events deltas, swap, OOM, DAG runs.
set -euo pipefail
export LC_ALL=C # ru_RU LC_NUMERIC makes awk read "1.402" as 1
cd ~/shopflow
C="docker compose -f docker-compose.pi5.yml"
DURATION_SEC=${DURATION_SEC:-900}
SAMPLE_SEC=5
DAGS="shopflow_reconciliation shopflow_data_quality shopflow_retention"
SERVICES="clickhouse airflow-scheduler airflow-api-server airflow-dag-processor airflow-db"

cg() { echo "/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "shopflow-$1-1").scope"; }
events() { awk -v k="$2" '$1 == k {print $2}' "$(cg "$1")/memory.events"; }
trigger_all() {
    for d in $DAGS; do $C exec -T airflow-scheduler airflow dags trigger "$d" </dev/null >/dev/null 2>&1; done
    echo "$(date +%T) triggered: $DAGS"
}

declare -A max0 oom0 peak
for s in $SERVICES; do max0[$s]=$(events "$s" max); oom0[$s]=$(events "$s" oom_kill); done
min_avail=999999; max_swap=0; max_load=0
trigger_all
half=$((SECONDS + DURATION_SEC / 2)); retriggered=0
end=$((SECONDS + DURATION_SEC))
while [ $SECONDS -lt $end ]; do
    while read -r name mem; do
        mib=$(echo "$mem" | awk '{v=$1+0; if ($1 ~ /GiB/) v*=1024; else if ($1 ~ /KiB/) v/=1024; else if ($1 !~ /MiB/) v/=1048576; printf "%d", v}')
        [ "${peak[$name]:-0}" -lt "$mib" ] && peak[$name]=$mib
    done < <(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep '^shopflow-' | sed 's/^shopflow-//; s/-1 / /; s| /.*||')
    read -r avail swap < <(free -m | awk '/^Mem/{a=$7} /^Swap/{s=$3} END{print a, s}')
    [ "$avail" -lt "$min_avail" ] && min_avail=$avail
    [ "$swap" -gt "$max_swap" ] && max_swap=$swap
    load=$(awk '{print $1}' /proc/loadavg); max_load=$(awk -v a="$load" -v b="$max_load" 'BEGIN{print (a > b) ? a : b}')
    if [ $retriggered = 0 ] && [ $SECONDS -ge $half ]; then trigger_all; retriggered=1; fi
    sleep $SAMPLE_SEC
done

echo "== peak memory, MiB (limit):"
for s in $SERVICES; do
    printf '%-22s %5s  (%s)\n' "$s" "${peak[$s]:-?}" "$(docker inspect -f '{{.HostConfig.Memory}}' "shopflow-$s-1" | awk '{printf "%dm", $1/1048576}')"
done
echo "== host: min available ${min_avail} MiB, max swap used ${max_swap} MiB, max load1 ${max_load}"
echo "== memory.events delta (max = hits of the limit, oom_kill), swap.current:"
for s in $SERVICES; do
    printf '%-22s max+%s oom_kill+%s swap_mib=%s\n' "$s" $(( $(events "$s" max) - max0[$s] )) \
        $(( $(events "$s" oom_kill) - oom0[$s] )) $(( $(cat "$(cg "$s")/memory.swap.current") / 1048576 ))
done
echo "== kernel oom:"; sudo -n dmesg 2>/dev/null | grep -iE 'oom|killed process' | tail -5 || echo "(dmesg needs sudo)"
vcgencmd get_throttled 2>/dev/null || true; vcgencmd measure_temp 2>/dev/null || true
echo "== dag runs (latest 3):"
for d in $DAGS; do
    echo "$d"; $C exec -T airflow-scheduler airflow dags list-runs "$d" -o plain </dev/null 2>/dev/null | sed -n '1,4p' | cut -c1-170
done
