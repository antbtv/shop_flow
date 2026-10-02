#!/usr/bin/env bash
# Create or update the read-only reconciliation role on the running laptop Postgres (ADR-0010).
# Init scripts run only on an empty volume, so the existing one needs this. Idempotent.
# Usage: scripts/create-pg-reader.sh   (RECON_READER_PASSWORD comes from .env via compose)
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose -f docker-compose.laptop.yml exec -T postgres \
    bash /docker-entrypoint-initdb.d/003_recon_reader_role.sh
