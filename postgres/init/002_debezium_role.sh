#!/usr/bin/env bash
# Debezium role: replication + read-only access to the CDC tables (ADR-0005).
# The password comes from the environment, so this is a shell script rather than plain SQL.
set -euo pipefail

psql -v ON_ERROR_STOP=1 -v pw="$DEBEZIUM_PASSWORD" \
    --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD :'pw';
GRANT CONNECT ON DATABASE :"DBNAME" TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON customers, products, orders, order_items, inventory TO debezium;
SQL
