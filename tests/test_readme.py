"""README.md (5.13): it points only at things that exist and holds no secret or private address.

The checker is scripts/check_readme.py; here it runs on the real README and on deliberately broken
copies, so that a checker that finds nothing is not mistaken for a README that is clean.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from scripts import check_readme

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"


def check(tmp_path, extra: str) -> list[str]:
    copy = tmp_path / "README.md"
    copy.write_text(README.read_text(encoding="utf-8") + "\n" + extra + "\n", encoding="utf-8")
    return check_readme.problems(copy, base=ROOT)  # links resolve as in the real README


def test_the_real_readme_is_clean():
    assert check_readme.problems(README) == []


def test_laptop_compose_resolves_with_the_example_env_as_the_readme_says():
    if not shutil.which("docker"):
        pytest.skip("docker is not available")
    done = subprocess.run(
        ["docker", "compose", "--env-file", ".env.example", "-f", "docker-compose.laptop.yml",
         "config", "-q"], cwd=ROOT, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


def test_the_readme_has_the_parts_that_the_acceptance_asks_for():
    text = README.read_text(encoding="utf-8")
    assert "```mermaid" in text and "flowchart" in text
    for needle in ("## Запуск", "## Тесты", "## Ключевые решения", "docker-compose.laptop.yml",
                   "docker-compose.pi5.yml", "ADR-0012"):
        assert needle in text, needle


@pytest.mark.parametrize(("extra", "expected"), [
    ("[broken](docs/no-such-file.md)", "link target missing"),
    ("```bash\nscripts/no-such-script.sh\n```", "path missing: scripts/no-such-script.sh"),
    ("see `infra/pi5/no-such-dir/` here", "path missing"),
    ("ADR-9999 says so", "ADR-9999 has no file"),
    ("host 10.20.30.40 is the server", "private address"),
    ("host 172.20.1.5 is the server", "private address"),
    ("key 0123456789abcdef0123456789abcdef0123456789abcdef", "long hex"),
    ("bot 123456789:AAFabcdefghijklmnopqrstuvwxyz0123456", "Telegram bot token"),
    ("```bash\nPOSTGRES_PASSWORD=hunter2\n```", "secret-looking name"),
    ("```bash\nTELEGRAM_BOT_TOKEN=abc\n```", "secret-looking name"),
])
def test_a_broken_readme_is_caught(tmp_path, extra, expected):
    found = check(tmp_path, extra)
    assert len(found) == 1 and expected in found[0], found  # exactly the planted problem


@pytest.mark.parametrize("extra", [
    "the example address 192.168.0.151 from .env.example is fine",
    "```bash\nPOSTGRES_PASSWORD=<your value>\nCLICKHOUSE_PASSWORD=$PASSWORD\n```",
    "see [the ADR](docs/adr/0004-pi5-memory-budget.md#x) and `scripts/apply-ddl.sh`",
    "public address 8.8.8.8 is not private",
])
def test_legitimate_text_is_not_flagged(tmp_path, extra):
    assert check(tmp_path, extra) == []
