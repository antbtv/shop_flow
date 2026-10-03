"""Throwaway ClickHouse for SQL regression tests (4.8): same image and server config as Pi5.

One container per test session on a free local port; all of clickhouse/ddl is applied with
scripts/apply-ddl.sh, so the migration script is exercised too. Skipped without Docker or the
image. SHOPFLOW_DDL_DIR points the session at another DDL folder (a deliberately broken copy
shows that the tests catch the bug). No dependency beyond the standard library.
"""

import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "clickhouse/clickhouse-server:25.8.33.6"
USER, PASSWORD = "admin", "test-only"
CONFIG = ROOT / "clickhouse/config.d/shopflow.xml"
PROFILE = ROOT / "clickhouse/users.d/shopflow-profile.xml"


class ClickHouse:
    def __init__(self, url: str, container: str, network: str, subnet: str):
        self.url = url
        # For clients in other containers (Spark): host = container name on this network.
        self.container = container
        self.network = network
        self.subnet = subnet

    def query(self, sql: str) -> str:
        """Run one statement; returns TSV text. Raises with the server message on error."""
        req = urllib.request.Request(
            f"{self.url}/?database=shopflow&default_format=TSV",
            data=sql.encode(),
            headers={"X-ClickHouse-User": USER, "X-ClickHouse-Key": PASSWORD},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as exc:
            raise AssertionError(f"ClickHouse error: {exc.read().decode()[:500]}") from None

    def rows(self, sql: str) -> list[tuple[str, ...]]:
        return [tuple(line.split("\t")) for line in self.query(sql).splitlines()]

    def refresh(self, view: str) -> None:
        """Run a refreshable MV now and wait for it (its schedule is every 2 minutes)."""
        self.query(f"SYSTEM REFRESH VIEW shopflow.{view}")
        self.query(f"SYSTEM WAIT VIEW shopflow.{view}")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _docker_ready() -> bool:
    if not shutil.which("docker"):
        return False
    probe = subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True)
    return probe.returncode == 0


@pytest.fixture(scope="session")
def ch():
    if not _docker_ready():
        pytest.skip(f"docker or image {IMAGE} not available")
    name = f"shopflow-sqltest-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    # Own network: a Spark container can reach the server by name (test_backfill.py).
    subprocess.run(["docker", "network", "create", name], check=True, capture_output=True)
    subnet = subprocess.run(
        ["docker", "network", "inspect", name, "-f", "{{range .IPAM.Config}}{{.Subnet}}{{end}}"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(
        ["docker", "run", "-d", "--name", name, "--network", name,
         "-p", f"127.0.0.1:{port}:8123",
         "-e", f"CLICKHOUSE_USER={USER}", "-e", f"CLICKHOUSE_PASSWORD={PASSWORD}",
         "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
         "-v", f"{CONFIG}:/etc/clickhouse-server/config.d/shopflow.xml:ro",
         "-v", f"{PROFILE}:/etc/clickhouse-server/users.d/shopflow-profile.xml:ro",
         IMAGE],
        check=True, capture_output=True,
    )
    try:
        url = f"http://127.0.0.1:{port}"
        for _ in range(60):
            try:
                with urllib.request.urlopen(f"{url}/ping", timeout=2) as resp:
                    if resp.read().startswith(b"Ok"):
                        break
            except OSError:
                pass
            time.sleep(1)
        else:
            pytest.fail("ClickHouse did not start in 60 s")
        env = {**os.environ, "CLICKHOUSE_URL": url,
               "CLICKHOUSE_USER": USER, "CLICKHOUSE_PASSWORD": PASSWORD}
        ddl_dir = os.environ.get("SHOPFLOW_DDL_DIR", "clickhouse/ddl")
        applied = subprocess.run([str(ROOT / "scripts/apply-ddl.sh"), ddl_dir],
                                 cwd=ROOT, env=env, capture_output=True, text=True)
        if applied.returncode:
            pytest.fail(f"apply-ddl.sh failed:\n{applied.stdout[-1500:]}\n{applied.stderr[-1500:]}")
        yield ClickHouse(url, name, name, subnet)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "network", "rm", name], capture_output=True)


PG_IMAGE = "postgres:17-alpine"


@pytest.fixture(scope="session")
def pg():
    """Throwaway Postgres with the OLTP schema (postgres/init/001_schema.sql), psycopg2 conn."""
    psycopg2 = pytest.importorskip("psycopg2")
    if not shutil.which("docker"):
        pytest.skip("docker not available")
    name = f"shopflow-pgtest-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    subprocess.run(
        ["docker", "run", "-d", "--name", name, "-p", f"127.0.0.1:{port}:5432",
         "-e", "POSTGRES_DB=shopflow", "-e", "POSTGRES_USER=shopflow",
         "-e", f"POSTGRES_PASSWORD={PASSWORD}", PG_IMAGE],
        check=True, capture_output=True,
    )
    try:
        for _ in range(60):
            ready = subprocess.run(
                ["docker", "exec", name, "pg_isready", "-U", "shopflow", "-h", "127.0.0.1"],
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail("Postgres did not start in 60 s")
        schema = (ROOT / "postgres/init/001_schema.sql").read_bytes()
        subprocess.run(["docker", "exec", "-i", name, "psql", "-v", "ON_ERROR_STOP=1",
                        "-U", "shopflow", "-d", "shopflow"],
                       input=schema, check=True, capture_output=True)
        conn = psycopg2.connect(host="127.0.0.1", port=port, dbname="shopflow",
                                user="shopflow", password=PASSWORD)
        conn.autocommit = True
        yield conn
        conn.close()
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
