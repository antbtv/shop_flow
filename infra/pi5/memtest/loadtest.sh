#!/usr/bin/env bash
# Milestone 0.9: ClickHouse heavy GROUP BY + memtest DAG at the same time, then a peak-memory summary.
# Run on Pi5 from ~/memtest after `docker compose -f docker-compose.memtest.yml up -d`.
set -euo pipefail
export LC_ALL=C # ru_RU LC_NUMERIC makes awk read "1.402" as 1
cd "$(dirname "$0")"
C="docker compose -f docker-compose.memtest.yml"
SAMPLE_SEC=5
DURATION_SEC=${DURATION_SEC:-300}

$C exec -T airflow-scheduler airflow dags unpause memtest >/dev/null
$C exec -T airflow-scheduler airflow dags trigger memtest >/dev/null
echo "dag triggered"

# stdin from /dev/null: a backgrounded `docker compose exec` otherwise gets stopped by SIGTTIN
$C exec -T clickhouse sh -c 'clickhouse-client --user shopflow --password "$CLICKHOUSE_PASSWORD" --time \
  --query "SELECT number % 20000000 AS k, count() FROM numbers(1000000000) GROUP BY k FORMAT Null"' \
  </dev/null >/tmp/ch_query.out 2>&1 &
CH_PID=$!

declare -A peak
min_avail=999999
end=$((SECONDS + DURATION_SEC))
while [ $SECONDS -lt $end ]; do
  while read -r name mem; do
    mib=$(echo "$mem" | awk '{v=$1+0; if ($1 ~ /GiB/) v*=1024; else if ($1 ~ /KiB/) v/=1024; else if ($1 !~ /MiB/) v/=1048576; printf "%d", v}')
    [ "${peak[$name]:-0}" -lt "$mib" ] && peak[$name]=$mib
  done < <(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | sed 's/memtest-//; s| /.*||')
  avail=$(free -m | awk '/Mem/{print $7}')
  [ "$avail" -lt "$min_avail" ] && min_avail=$avail
  sleep $SAMPLE_SEC
done

wait $CH_PID || true
echo "== clickhouse query (seconds or error):"; cat /tmp/ch_query.out
echo "== peak memory, MiB:"; for n in "${!peak[@]}"; do echo "$n ${peak[$n]}"; done | sort
echo "== min available, MiB: $min_avail"
for s in airflow-scheduler clickhouse; do
  id=$(docker inspect -f '{{.Id}}' "memtest-$s-1")
  cg=/sys/fs/cgroup/system.slice/docker-$id.scope
  echo "== $s: $(grep -E '^(max|oom_kill) ' $cg/memory.events | tr '\n' ' ') swap_mib=$(( $(cat $cg/memory.swap.current) / 1048576 ))"
done
echo "== kernel oom:"; sudo dmesg | grep -iE 'oom|killed process' | tail -5 || true
vcgencmd get_throttled; vcgencmd measure_temp
