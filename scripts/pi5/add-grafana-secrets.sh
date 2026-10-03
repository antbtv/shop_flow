#!/usr/bin/env bash
# 5.6 (ADR-0011): add the Grafana secrets to the laptop .env and to ~/shopflow/.env on Pi5.
# Idempotent: a variable that is already set is left alone, nothing is printed but variable names.
#   CLICKHOUSE_GRAFANA_PASSWORD  laptop (scripts/create-ch-users.sh) and Pi5 (grafana service): the same value
#   GRAFANA_ADMIN_PASSWORD, GRAFANA_SECRET_KEY   Pi5 only
# All values are hex (openssl rand -hex 24). The shared password goes to Pi5 on stdin, not in argv.
# If Pi5 already holds a different CLICKHOUSE_GRAFANA_PASSWORD the script stops: run create-ch-users.sh
# and the grafana service with one value only.
# Usage: scripts/pi5/add-grafana-secrets.sh        (PI5_SSH=pi5, PI5_DIR=shopflow)
# Test hooks: ENV_FILE (laptop file), PI5_REMOTE_CMD (default "ssh $PI5_SSH bash -s").
set -euo pipefail

cd "$(dirname "$0")/../.."
env_file=${ENV_FILE:-.env}
read -ra remote <<<"${PI5_REMOTE_CMD:-ssh ${PI5_SSH:-pi5} bash -s}"
remote_dir=${PI5_DIR:-shopflow}

[[ -f $env_file ]] || { echo "$env_file not found" >&2; exit 1; }
password=$(sed -n 's/^CLICKHOUSE_GRAFANA_PASSWORD=//p' "$env_file" | tail -n 1)
if [[ -z $password ]]; then
    password=$(openssl rand -hex 24)
    # Keep the file mode; make sure the last line ends with a newline before appending.
    [[ -z $(tail -c1 "$env_file") ]] || echo >>"$env_file"
    sed -i '/^CLICKHOUSE_GRAFANA_PASSWORD=$/d' "$env_file"
    printf 'CLICKHOUSE_GRAFANA_PASSWORD=%s\n' "$password" >>"$env_file"
    echo "laptop: added CLICKHOUSE_GRAFANA_PASSWORD"
else
    echo "laptop: CLICKHOUSE_GRAFANA_PASSWORD already set"
fi

# The script reaches Pi5 on stdin; the value is expanded here, so it never appears in a command line.
"${remote[@]}" <<EOF
set -euo pipefail
cd ~/$remote_dir
[[ -f .env ]] || { echo "pi5: ~/$remote_dir/.env not found" >&2; exit 1; }
chmod 600 .env
value_of() { sed -n "s/^\$1=//p" .env | tail -n 1; }
add() {  # name value: append unless the variable already has a value
    if [[ -n \$(value_of "\$1") ]]; then echo "pi5: \$1 already set"; return; fi
    [[ -z \$(tail -c1 .env) ]] || echo >>.env
    sed -i "/^\$1=\$/d" .env
    printf '%s=%s\n' "\$1" "\$2" >>.env
    echo "pi5: added \$1"
}
current=\$(value_of CLICKHOUSE_GRAFANA_PASSWORD)
if [[ -n \$current && \$current != '$password' ]]; then
    echo "pi5: CLICKHOUSE_GRAFANA_PASSWORD differs from the laptop value, not changed" >&2
    exit 1
fi
add CLICKHOUSE_GRAFANA_PASSWORD '$password'
add GRAFANA_ADMIN_PASSWORD "\$(openssl rand -hex 24)"
add GRAFANA_SECRET_KEY "\$(openssl rand -hex 24)"
stat -c 'pi5: .env mode %a' .env
EOF
