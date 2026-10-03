"""scripts/pi5/add-grafana-secrets.sh (5.6): both env files on temporary paths, "Pi5" is a local
bash with HOME pointed at a temp dir (PI5_REMOTE_CMD), so no real .env and no ssh are involved.
"""

import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/pi5/add-grafana-secrets.sh"
NAMES = ("CLICKHOUSE_GRAFANA_PASSWORD", "GRAFANA_ADMIN_PASSWORD", "GRAFANA_SECRET_KEY")


def read(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, _, value = line.partition("=")
            out[key] = value
    return out


@pytest.fixture
def env(tmp_path):
    laptop = tmp_path / "laptop-vars"
    laptop.write_text("POSTGRES_PASSWORD=abc\nCLICKHOUSE_GRAFANA_PASSWORD=")  # no final newline
    pi5 = tmp_path / "home" / "shopflow" / ".env"
    pi5.parent.mkdir(parents=True)
    pi5.write_text("CLICKHOUSE_PASSWORD=keep\nGRAFANA_SECRET_KEY=\n")
    pi5.chmod(0o644)

    def run():
        return subprocess.run(
            [str(SCRIPT)], cwd=ROOT, capture_output=True, text=True,
            env={**os.environ, "ENV_FILE": str(laptop), "PI5_REMOTE_CMD": "bash -s",
                 "HOME": str(tmp_path / "home")},
        )
    return run, laptop, pi5


def test_adds_the_three_secrets_as_hex_with_one_shared_password(env):
    run, laptop, pi5 = env
    done = run()
    assert done.returncode == 0, done.stderr
    lap, pi = read(laptop), read(pi5)
    assert lap["POSTGRES_PASSWORD"] == "abc" and pi["CLICKHOUSE_PASSWORD"] == "keep"
    for name in NAMES:
        assert re.fullmatch(r"[0-9a-f]{48}", pi[name]), name
    assert lap["CLICKHOUSE_GRAFANA_PASSWORD"] == pi["CLICKHOUSE_GRAFANA_PASSWORD"]
    assert len({pi[n] for n in NAMES}) == 3
    assert stat.S_IMODE(pi5.stat().st_mode) == 0o600


def test_output_has_names_but_no_values(env):
    run, laptop, pi5 = env
    done = run()
    values = list(read(laptop).values()) + list(read(pi5).values())
    for value in values:
        assert value not in done.stdout + done.stderr
    assert "added GRAFANA_ADMIN_PASSWORD" in done.stdout


def test_second_run_changes_nothing(env):
    run, laptop, pi5 = env
    run()
    before = (laptop.read_text(), pi5.read_text())
    done = run()
    assert done.returncode == 0, done.stderr
    assert (laptop.read_text(), pi5.read_text()) == before
    assert done.stdout.count("already set") == 4
    assert "added" not in done.stdout


def test_a_different_password_on_pi5_stops_the_script(env):
    run, laptop, pi5 = env
    laptop.write_text("CLICKHOUSE_GRAFANA_PASSWORD=" + "a" * 48 + "\n")
    pi5.write_text("CLICKHOUSE_GRAFANA_PASSWORD=" + "b" * 48 + "\n")
    done = run()
    assert done.returncode != 0
    assert "differs" in done.stderr
    assert read(pi5)["CLICKHOUSE_GRAFANA_PASSWORD"] == "b" * 48
    assert "GRAFANA_ADMIN_PASSWORD" not in read(pi5)


def test_missing_pi5_env_file_is_an_error(env, tmp_path):
    run, laptop, pi5 = env
    pi5.unlink()
    done = run()
    assert done.returncode != 0 and "not found" in done.stderr
