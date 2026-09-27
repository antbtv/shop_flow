# Sourced by ClickHouse scripts. Sets CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD and ch_curl.
# Missing variables are read from .env (only these keys, without sourcing it, never printed).
# The password goes in a header read from a file descriptor: not in the URL, not visible in ps.

env_value() {
    [[ -f .env ]] && sed -n "s/^$1=//p" .env | tail -n 1
}
for var in PI5_HOST CLICKHOUSE_HTTP_PORT CLICKHOUSE_USER CLICKHOUSE_PASSWORD; do
    [[ -n ${!var:-} ]] || printf -v "$var" '%s' "$(env_value "$var")"
done
CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://${PI5_HOST:?set PI5_HOST}:${CLICKHOUSE_HTTP_PORT:-8123}}
: "${CLICKHOUSE_USER:?set CLICKHOUSE_USER}" "${CLICKHOUSE_PASSWORD:?set CLICKHOUSE_PASSWORD}"

# ch_curl [curl args...]: authenticated request to $CLICKHOUSE_URL, fails with the server message.
ch_curl() {
    curl -sS --fail-with-body --retry 3 --retry-connrefused \
        -H @<(printf 'X-ClickHouse-User: %s\nX-ClickHouse-Key: %s\n' "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD") \
        "$@"
}
