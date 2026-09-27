#!/usr/bin/env bash
# Create or update the Debezium Postgres connector (idempotent PUT), then print its state.
# Secrets are not here: the config references ${env:...} resolved inside the Connect container (ADR-0005).
set -euo pipefail

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
NAME=${CONNECTOR_NAME:-shopflow-pg}
CONFIG=$(dirname "$0")/../debezium/postgres-connector.json

curl -fsS -X PUT -H 'Content-Type: application/json' \
    --data @"$CONFIG" "$CONNECT_URL/connectors/$NAME/config" >/dev/null

state=unknown
for _ in $(seq 1 30); do
    # Right after PUT the status endpoint can return 404 until the connector is created.
    state=$(curl -sS "$CONNECT_URL/connectors/$NAME/status" |
        jq -r '[.connector.state, (.tasks[]?.state)] | join(" ")' 2>/dev/null) || state=unknown
    if [[ $state == "RUNNING RUNNING" ]]; then
        echo "$NAME: $state"
        exit 0
    fi
    sleep 2
done
echo "$NAME: not running: $state" >&2
curl -fsS "$CONNECT_URL/connectors/$NAME/status" | jq -r '.tasks[].trace // empty' | head -20 >&2
exit 1
