"""Which Pi5 services see which secrets (ADR-0010, ADR-0011): Telegram only in the scheduler.

Resolves docker-compose.pi5.yml with a throwaway env file; no real .env is read.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker-compose.pi5.yml"
VARS = {
    "PI5_HOST": "127.0.0.1", "PI5_COMPOSE_SUBNET": "172.31.250.0/24",
    "CLICKHOUSE_USER": "u", "CLICKHOUSE_PASSWORD": "p", "AIRFLOW_ADMIN_USER": "a",
    "AIRFLOW_ADMIN_PASSWORD": "a", "AIRFLOW_DB_PASSWORD": "d", "AIRFLOW_FERNET_KEY": "k",
    "AIRFLOW_JWT_SECRET": "j", "CLICKHOUSE_AIRFLOW_PASSWORD": "c", "LAPTOP_HOST": "127.0.0.2",
    "RECON_READER_PASSWORD": "r", "CLICKHOUSE_GRAFANA_PASSWORD": "g",
    "GRAFANA_ADMIN_PASSWORD": "ga", "GRAFANA_SECRET_KEY": "gk",
}
TOKEN = "123456:TOKEN-VALUE-FOR-TEST"


def resolved(tmp_path: Path, extra: dict[str, str]) -> dict:
    if not shutil.which("docker"):
        pytest.skip("docker is not available")
    env_file = tmp_path / "env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in {**VARS, **extra}.items()))
    out = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "--env-file", str(env_file), "config",
         "--format", "json"],
        capture_output=True, text=True, check=True, cwd=ROOT,
    ).stdout
    return json.loads(out)["services"]


def test_telegram_settings_reach_only_the_scheduler(tmp_path):
    services = resolved(tmp_path, {"TELEGRAM_BOT_TOKEN": TOKEN, "TELEGRAM_CHAT_ID": "-100500"})
    env = services["airflow-scheduler"]["environment"]
    assert env["TELEGRAM_BOT_TOKEN"] == TOKEN
    assert env["TELEGRAM_CHAT_ID"] == "-100500"
    assert env["TELEGRAM_API_URL"] == "https://api.telegram.org"
    for name, service in services.items():
        if name == "airflow-scheduler":
            continue
        assert not any(k.startswith("TELEGRAM") for k in service.get("environment", {})), name
        assert TOKEN not in json.dumps(service), name


def test_without_telegram_variables_the_stack_still_resolves(tmp_path):
    env = resolved(tmp_path, {})["airflow-scheduler"]["environment"]
    assert env["TELEGRAM_BOT_TOKEN"] == "" and env["TELEGRAM_CHAT_ID"] == ""
