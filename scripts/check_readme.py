#!/usr/bin/env python3
"""Checks README.md against the repository (5.13): nothing it points to is missing, and it holds
no secret or private address except the examples of .env.example.

    scripts/check_readme.py [README.md]

Checks: relative links `[text](path)`; paths in inline code and in fenced commands (`scripts/...`,
`docs/...`, compose files and so on); every ADR-NNNN has a file in docs/adr; no private IPv4
outside .env.example, no long hex strings, no Telegram-token shape, no `KEY=value` of a secret.
Exit code 1 and one line per problem. Standard library only.
"""

from __future__ import annotations

import ipaddress
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOP_DIRS = ("scripts", "docs", "infra", "clickhouse", "airflow", "grafana", "dashboards", "tests",
            "postgres", "debezium", "generator", "spark-jobs")
ROOT_FILES = re.compile(r"^(docker-compose\.[a-z0-9]+\.yml|PRD-[\w-]+\.md|\.env\.example|"
                        r"requirements-dev\.txt|pyproject\.toml)$")
SECRET_KEYS = ("PASSWORD", "SECRET", "TOKEN", "PRIVATEKEY", "API_KEY")


def allowed_networks() -> list[ipaddress.IPv4Network]:
    """Addresses that .env.example itself shows as examples."""
    text = (ROOT / ".env.example").read_text()
    found = re.findall(r"\b\d{1,3}(?:\.\d{1,3}){3}(?:/\d{1,2})?\b", text)
    return [ipaddress.ip_network(item, strict=False) for item in found]


def strip_fences(text: str) -> tuple[str, list[str]]:
    """(text without fenced blocks, the fenced blocks)."""
    blocks = re.findall(r"```.*?\n(.*?)```", text, flags=re.S)
    return re.sub(r"```.*?```", "", text, flags=re.S), blocks


def looks_like_repo_path(token: str) -> bool:
    token = token.strip().rstrip(",.;:)")
    if not token or any(ch in token for ch in " $<>*{}|=:'\"") or token.startswith(("http", "~")):
        return False
    return token.split("/")[0] in TOP_DIRS or bool(ROOT_FILES.match(token))


def path_exists(token: str) -> bool:
    return (ROOT / token.strip().rstrip(",.;:)").rstrip("/")).exists()


def problems(readme: Path, base: Path | None = None) -> list[str]:
    """base: the directory that relative links are resolved from (default: the README's own)."""
    base = base or readme.parent
    text = readme.read_text(encoding="utf-8")
    prose, blocks = strip_fences(text)
    found: list[str] = []

    for target in re.findall(r"\]\(([^)\s]+)\)", text):
        if target.startswith(("http://", "https://", "#", "mailto:")):
            continue
        if not (base / target.split("#")[0]).exists():
            found.append(f"link target missing: {target}")

    tokens = re.findall(r"`([^`\n]+)`", prose)
    for block in blocks:
        tokens += re.findall(r"[\w./-]+", block)
    for token in dict.fromkeys(tokens):
        if looks_like_repo_path(token) and not path_exists(token):
            found.append(f"path missing: {token}")

    adrs = {p.name[:4] for p in (ROOT / "docs" / "adr").glob("*.md")}
    for number in dict.fromkeys(re.findall(r"ADR-(\d{4})", text)):
        if number not in adrs:
            found.append(f"ADR-{number} has no file in docs/adr")

    allowed = allowed_networks()
    for item in dict.fromkeys(re.findall(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", text)):
        try:
            ip = ipaddress.ip_address(item)
        except ValueError:
            continue
        if ip.is_private and not any(ip in net for net in allowed):
            found.append(f"private address that .env.example does not show: {item}")
    if re.search(r"\b[0-9a-f]{32,}\b", text, flags=re.I):
        found.append("a long hex string (a key or a hash?)")
    if re.search(r"\b\d{6,}:[A-Za-z0-9_-]{30,}\b", text):
        found.append("a string shaped like a Telegram bot token")
    for line in text.splitlines():
        match = re.match(r"\s*([A-Z0-9_]+)=(\S+)", line)
        if not match:
            continue
        name, value = match.groups()
        if any(k in name for k in SECRET_KEYS) and not value.startswith(("<", "$", "changeme")):
            found.append(f"a value assigned to a secret-looking name: {name}")
    return found


def main() -> int:
    readme = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "README.md"
    found = problems(readme)
    for line in found:
        print(line)
    print(f"{readme.name}: {len(found)} problem(s)")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
