#!/usr/bin/env bash
# Telegram alerts check on Pi5 (5.11, ADR-0011). Runs ON Pi5; start it from the laptop:
#   ssh pi5 'bash -s' < scripts/pi5/alerts-smoke.sh
# Checks where the Telegram settings are visible and that the token is in no log. The token is
# read from ~/shopflow/.env and handed to grep through a pipe or a file descriptor: never in argv,
# never printed. Sending a message is not done here: trigger a DAG run (scripts/pi5/airflow-api.sh).
set -uo pipefail
cd ~/shopflow
COMPOSE="docker compose -f docker-compose.pi5.yml"
token=$(sed -n 's/^TELEGRAM_BOT_TOKEN=//p' .env | tail -n 1 | tr -d '\r')
chat=$(sed -n 's/^TELEGRAM_CHAT_ID=//p' .env | tail -n 1 | tr -d '\r')
# The secret part after the colon is what must not appear anywhere (the numeric bot id is public).
secret=${token#*:}

echo "== .env on Pi5 (expected: mode 600, both values set)"
stat -c '%a %U' .env
echo "TELEGRAM_BOT_TOKEN set: $([[ -n $token ]] && echo yes || echo NO)"
echo "TELEGRAM_CHAT_ID set:   $([[ -n $chat ]] && echo yes || echo NO)"
[[ -n $secret ]] || { echo "token is empty, stop"; exit 1; }

echo "== TELEGRAM_BOT_TOKEN in the environment of each container (expected: only airflow-scheduler)"
for svc in $($COMPOSE ps --services); do
    id=$($COMPOSE ps -q "$svc")
    [[ -n $id ]] || continue
    n=$(docker inspect "$id" --format '{{range .Config.Env}}{{println .}}{{end}}' \
        | grep -c '^TELEGRAM_BOT_TOKEN=.\+')
    printf '  %-24s %s\n' "$svc" "$n"
done

echo "== token in container logs (expected: 0 everywhere)"
for svc in $($COMPOSE ps --services); do
    n=$($COMPOSE logs --no-color "$svc" 2>&1 | grep -c -F -f <(printf '%s\n' "$secret"))
    printf '  %-24s %s\n' "$svc" "$n"
done

echo "== files with the token in the Airflow logs volume (expected: 0)"
$COMPOSE exec -T airflow-scheduler sh -c 'grep -rlF -f /dev/stdin /opt/airflow/logs | wc -l' <<<"$secret"

echo "== Telegram lines of recent task logs (no secrets expected, only names and codes)"
$COMPOSE exec -T airflow-scheduler sh -c \
    'grep -rhE "Telegram|alert failed|ShopFlow check failed" /opt/airflow/logs/dag_id=* 2>/dev/null | tail -n 6 | cut -c1-260' </dev/null

