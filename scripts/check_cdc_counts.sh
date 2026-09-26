#!/usr/bin/env bash
# Compare the state replayed from cdc.public.<table> with count(*) in Postgres.
# Usage: scripts/check_cdc_counts.sh [table ...]   (default: orders customers)
# Exit 1 if any table differs. Stop the generator first, or counts race with new events.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker-compose.laptop.yml)
tables=("${@:-orders customers}")
read -r -a tables <<<"${tables[*]}"
rc=0

for t in "${tables[@]}"; do
    state=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic "cdc.public.$t" --from-beginning \
        --timeout-ms 10000 --formatter-property print.key=true --formatter-property 'key.separator=|' \
        2>/dev/null | python3 scripts/cdc_state.py)
    pg=$("${COMPOSE[@]}" exec -T postgres sh -c \
        "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -At -c 'SELECT count(*) FROM $t'")
    live=$(jq -r .live_keys <<<"$state")
    verdict=OK
    [[ $live == "$pg" ]] || { verdict=MISMATCH; rc=1; }
    echo "$t: postgres=$pg kafka_live=$live ops=$(jq -c .ops <<<"$state") duplicates=$(jq -r .duplicates <<<"$state") $verdict"
done
exit $rc
