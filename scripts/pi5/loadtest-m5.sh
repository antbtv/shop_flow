#!/usr/bin/env bash
# Milestone 5.12: memory and latency of the whole Pi5 stack (ClickHouse, Airflow, Grafana, the
# Telegram tunnel) with the generator running on the laptop and dashboards being watched
# (NFR-7, NFR-3, ADR-0004). Runs ON Pi5; start it from the laptop together with
# scripts/loadtest-m5-laptop.sh (generator, viewers, laptop memory):
#   ssh pi5 'bash -s' < scripts/pi5/loadtest-m5.sh
# Triggers the four DAGs at the start and at DURATION/2, samples `docker stats` and the host every
# 5 s, ClickHouse refresh state every 30 s, then prints peaks, memory.events deltas, swap, OOM,
# the RSS of amneziawg-go (a host process, outside the cgroups), delay of raw_events, refresh and
# grafana_reader query statistics from system.query_log, and DAG runs. No secret is printed.
# Overrides (tests on a laptop stand): REPO_DIR, PROJECT, COMPOSE_EXTRA (more compose arguments).
set -euo pipefail
export LC_ALL=C # ru_RU LC_NUMERIC makes awk read "1.402" as 1
cd "${REPO_DIR:-$HOME/shopflow}"
PROJECT=${PROJECT:-shopflow}
# COMPOSE_EXTRA is split into words on purpose (e.g. "-f override.yml --env-file stand.env").
# shellcheck disable=SC2086
C="docker compose -p $PROJECT -f docker-compose.pi5.yml ${COMPOSE_EXTRA:-}"
DURATION_SEC=${DURATION_SEC:-900}
SAMPLE_SEC=5
REFRESH_EVERY=6 # samples: 30 s
DAGS="shopflow_reconciliation shopflow_data_quality shopflow_retention shopflow_alert_channel"
SERVICES="clickhouse airflow-scheduler airflow-api-server airflow-dag-processor airflow-db grafana"

cg() { echo "/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "$PROJECT-$1-1").scope"; }
events() { awk -v k="$2" '$1 == k {print $2}' "$(cg "$1")/memory.events"; }
# The client takes user and password from the container's own environment: not in argv.
ch() { $C exec -T clickhouse clickhouse-client -q "$1" </dev/null; }
trigger_all() {
    for d in $DAGS; do
        $C exec -T airflow-scheduler airflow dags trigger "$d" </dev/null >/dev/null 2>&1 || echo "  could not trigger $d"
    done
    echo "$(date +%T) triggered: $DAGS"
}
awg_rss_mib() { ps -C amneziawg-go -o rss= 2>/dev/null | awk '{s+=$1} END{printf "%d", s/1024}'; }

declare -A max0 oom0 peak
for s in $SERVICES; do max0[$s]=$(events "$s" max); oom0[$s]=$(events "$s" oom_kill); done
START_UTC=$(date -u +'%Y-%m-%d %H:%M:%S')
min_avail=999999; max_swap=0; max_load=0; awg_peak=0
refresh_max=0; refresh_errors=0; n=0
echo "$(date +%T) start (UTC window starts $START_UTC), ${DURATION_SEC}s"
trigger_all
half=$((SECONDS + DURATION_SEC / 2)); retriggered=0
end=$((SECONDS + DURATION_SEC))
while [ $SECONDS -lt $end ]; do
    while read -r name mem; do
        mib=$(echo "$mem" | awk '{v=$1+0; if ($1 ~ /GiB/) v*=1024; else if ($1 ~ /KiB/) v/=1024; else if ($1 !~ /MiB/) v/=1048576; printf "%d", v}')
        [ "${peak[$name]:-0}" -lt "$mib" ] && peak[$name]=$mib
    done < <(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep "^$PROJECT-" | sed "s/^$PROJECT-//; s/-1 / /; s| /.*||")
    read -r avail swap < <(free -m | awk '/^Mem/{a=$7} /^Swap/{s=$3} END{print a, s}')
    [ "$avail" -lt "$min_avail" ] && min_avail=$avail
    [ "$swap" -gt "$max_swap" ] && max_swap=$swap
    load=$(awk '{print $1}' /proc/loadavg); max_load=$(awk -v a="$load" -v b="$max_load" 'BEGIN{print (a > b) ? a : b}')
    awg=$(awg_rss_mib || true); [ "${awg:-0}" -gt "$awg_peak" ] && awg_peak=$awg
    if [ $((n % REFRESH_EVERY)) = 0 ]; then
        read -r dur exc < <(ch "SELECT ifNull(max(last_success_duration_ms), 0), countIf(exception != '') FROM system.view_refreshes WHERE database = 'shopflow'" 2>/dev/null || echo "0 0")
        [ "${dur:-0}" -gt "$refresh_max" ] && refresh_max=$dur
        [ "${exc:-0}" -gt "$refresh_errors" ] && refresh_errors=$exc
    fi
    if [ $retriggered = 0 ] && [ $SECONDS -ge $half ]; then trigger_all; retriggered=1; fi
    n=$((n + 1))
    sleep $SAMPLE_SEC
done
END_UTC=$(date -u +'%Y-%m-%d %H:%M:%S')

echo "== peak memory, MiB (limit):"
for s in $SERVICES; do
    printf '%-22s %5s  (%s)\n' "$s" "${peak[$s]:-?}" "$(docker inspect -f '{{.HostConfig.Memory}}' "$PROJECT-$s-1" | awk '{printf "%dm", $1/1048576}')"
done
echo "amneziawg-go (host process, outside cgroups): peak RSS ${awg_peak} MiB"
echo "== host: min available ${min_avail} MiB, max swap used ${max_swap} MiB, max load1 ${max_load}"
echo "== memory.events delta (max = hits of the limit, oom_kill), swap.current:"
for s in $SERVICES; do
    printf '%-22s max+%s oom_kill+%s swap_mib=%s\n' "$s" $(( $(events "$s" max) - max0[$s] )) \
        $(( $(events "$s" oom_kill) - oom0[$s] )) $(( $(cat "$(cg "$s")/memory.swap.current") / 1048576 ))
done
echo "== kernel oom:"; sudo -n dmesg 2>/dev/null | grep -iE 'oom|killed process' | tail -5 || echo "(dmesg needs sudo)"
vcgencmd get_throttled 2>/dev/null || true; vcgencmd measure_temp 2>/dev/null || true
echo "== tunnel: handshake age, s (needs sudo; empty = not readable)"
sudo -n awg show awg0 latest-handshakes 2>/dev/null | awk -v now="$(date +%s)" '{print now - $2}' || true

echo "== window UTC: $START_UTC .. $END_UTC"
echo "== ClickHouse refresh (sampled every 30 s): max duration ${refresh_max} ms, MV with exception: ${refresh_errors}"
SQL_DELAY="SELECT count(), round(quantile(0.5)(d), 1), round(quantile(0.95)(d), 1), round(quantile(0.99)(d), 1), round(max(d), 1) FROM (SELECT dateDiff('millisecond', event_time, ingested_at) / 1000 AS d FROM shopflow.raw_events WHERE ingested_at >= toDateTime64('$START_UTC', 3, 'UTC'))"
echo "== delay of raw_events (ingested_at - event_time), s: count p50 p95 p99 max"
ch "$SQL_DELAY" || echo "(query failed)"
# The refresh writes into a temporary table before EXCHANGE: that is how it shows in query_log (3.12).
SQL_REFRESH="SELECT count(), quantile(0.95)(query_duration_ms), max(query_duration_ms), max(memory_usage) FROM system.query_log WHERE type = 'QueryFinish' AND event_time >= toDateTime('$START_UTC', 'UTC') AND query LIKE 'INSERT INTO shopflow.\`.tmp.inner_id.%'"
echo "== refresh inserts (query_log): count p95_ms max_ms max_memory_bytes"
ch "$SQL_REFRESH" || echo "(query failed)"
SQL_GRAFANA="SELECT count(), max(query_duration_ms), max(memory_usage), countIf(exception_code != 0), max(read_rows) FROM system.query_log WHERE user = 'grafana_reader' AND type != 'QueryStart' AND event_time >= toDateTime('$START_UTC', 'UTC')"
echo "== grafana_reader queries (query_log): count max_ms max_memory_bytes errors max_read_rows"
ch "$SQL_GRAFANA" || echo "(query failed)"
echo "== alert channel probe rows in the window (telegram_reachable):"
ch "SELECT toString(status), count() FROM shopflow.dq_check_results FINAL WHERE check_name = 'telegram_reachable' AND checked_at >= toDateTime64('$START_UTC', 6, 'UTC') GROUP BY status" || true
echo "== dag runs (latest 3):"
for d in $DAGS; do
    echo "$d"; $C exec -T airflow-scheduler airflow dags list-runs "$d" -o plain </dev/null 2>/dev/null | sed -n '1,4p' | cut -c1-170
done
