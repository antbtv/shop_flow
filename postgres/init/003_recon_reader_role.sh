#!/usr/bin/env bash
# Read-only role for the reconciliation DAG on Pi5 (FR-8, ADR-0010). Idempotent: runs on a new
# volume as an init script and on the existing one via scripts/create-pg-reader.sh.
# The password is read with \getenv, so it never appears in psql's argv.
set -euo pipefail

: "${RECON_READER_PASSWORD:?set RECON_READER_PASSWORD}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv pw RECON_READER_PASSWORD
SELECT NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'recon_reader') AS create_role \gset
\if :create_role
CREATE ROLE recon_reader LOGIN;
\endif
ALTER ROLE recon_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    CONNECTION LIMIT 2 PASSWORD :'pw';
ALTER ROLE recon_reader SET default_transaction_read_only = on;
-- Tables grow without retention; revisit after the load test (4.15).
ALTER ROLE recon_reader SET statement_timeout = '300s';
ALTER ROLE recon_reader SET idle_in_transaction_session_timeout = '60s';
-- Sessions left behind by a sleeping laptop must not eat the connection limit.
ALTER ROLE recon_reader SET idle_session_timeout = '5min';
GRANT CONNECT ON DATABASE :"DBNAME" TO recon_reader;
GRANT USAGE ON SCHEMA public TO recon_reader;
GRANT SELECT ON customers, products, orders, order_items, inventory TO recon_reader;
SQL
