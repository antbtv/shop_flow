#!/usr/bin/env bash
# Start the streaming job only when ClickHouse answers /ping (ADR-0008).
# Docker resets its restart delay once a container has run for 10 s and Spark takes longer to
# start, so without this wait an offline Pi5 means a JVM start and a failed batch every
# 20-40 s. A curl is cheap, so the cap is short: an outage adds at most 60 s of waiting
# after ClickHouse is back (scenario 2.9 B). /ping needs no credentials.
set -euo pipefail

url="http://${PI5_HOST:?set PI5_HOST}:${CLICKHOUSE_HTTP_PORT:-8123}/ping"
delay=5
until curl -fsS --connect-timeout 5 --max-time 10 "$url" >/dev/null 2>&1; do
    echo "$(date -u +%FT%TZ) waiting for ClickHouse /ping, next try in ${delay}s"
    sleep "$delay"
    delay=$((delay * 2 > 60 ? 60 : delay * 2))
done
echo "$(date -u +%FT%TZ) ClickHouse is up, starting the streaming job"
exec /opt/spark/bin/spark-submit /opt/shopflow/streaming_to_clickhouse.py "$@"
