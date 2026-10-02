#!/usr/bin/env bash
# Renders pg_hba.conf from the template (ADR-0010), then hands over to the image entrypoint.
# Postgres reads it via -c hba_file=/etc/postgresql/pg_hba.conf (docker-compose.laptop.yml).
set -euo pipefail

ip='[0-9]{1,3}(\.[0-9]{1,3}){3}'
[[ ${PI5_HOST:?set PI5_HOST} =~ ^$ip$ ]] || { echo "PI5_HOST must be an IPv4 address" >&2; exit 1; }
[[ ${LAPTOP_COMPOSE_SUBNET:?set LAPTOP_COMPOSE_SUBNET} =~ ^$ip/[0-9]{1,2}$ ]] \
    || { echo "LAPTOP_COMPOSE_SUBNET must be a CIDR" >&2; exit 1; }
[[ ${POSTGRES_DB:?set POSTGRES_DB} =~ ^[a-z_][a-z0-9_]*$ ]] || { echo "POSTGRES_DB must be a plain name" >&2; exit 1; }

mkdir -p /etc/postgresql
sed -e "s|@PI5_HOST@|$PI5_HOST|" -e "s|@LAPTOP_COMPOSE_SUBNET@|$LAPTOP_COMPOSE_SUBNET|" \
    -e "s|@POSTGRES_DB@|$POSTGRES_DB|" /shopflow/pg_hba.conf.template > /etc/postgresql/pg_hba.conf
chown postgres:postgres /etc/postgresql/pg_hba.conf
chmod 640 /etc/postgresql/pg_hba.conf

exec docker-entrypoint.sh "$@"
