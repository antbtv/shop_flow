"""Panel 32 of the dashboard (age of the last successful Telegram probe, ADR-0012) on the
throwaway ClickHouse: rows written the way the probe DAG writes them, the panel's own SQL."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import pytest
from shopflow_checks.channel import CHECK
from shopflow_common.results import COLUMNS, TABLE

clickhouse_connect = pytest.importorskip("clickhouse_connect")
DASHBOARD = Path(__file__).resolve().parents[2] / "dashboards" / "shopflow.json"


def panel_sql() -> str:
    panels = json.loads(DASHBOARD.read_text())["panels"]
    (panel,) = [p for p in panels if p["id"] == 32]
    return panel["targets"][0]["rawSql"]


@pytest.fixture
def run(ch):
    ch.query("TRUNCATE TABLE shopflow.dq_check_results")
    url = urlparse(ch.url)
    client = clickhouse_connect.get_client(host=url.hostname, port=url.port, username="admin",
                                           password="test-only", database="shopflow")

    def write(status, hours_ago, dag_id="shopflow_alert_channel", check=CHECK):
        """A summary row as the probe writes it (write_results columns), checked_at moved back."""
        checked_at = datetime.now(UTC) - timedelta(hours=hours_ago)
        client.insert(TABLE, [[dag_id, f"r-{status}-{hours_ago}", None, check, "", status, None,
                               None, 0, None, json.dumps({"host": "h"}), checked_at]],
                      column_names=list(COLUMNS))

    def age() -> int:
        return int(ch.rows(panel_sql() + " FORMAT TSV")[0][0])

    return write, age


def test_no_probe_yet_is_the_99999_marker(run):
    _, age = run
    assert age() == 99999


def test_age_of_the_last_ok_probe_not_of_the_last_probe(run):
    write, age = run
    write("ok", 5)
    write("error", 1)  # the newest probe failed: the age of the last good one keeps growing
    assert age() in (5, 6)


def test_a_fresh_ok_probe_resets_the_age(run):
    write, age = run
    write("ok", 9)
    write("error", 3)
    write("ok", 0)
    assert age() == 0


def test_only_errors_is_the_99999_marker(run):
    write, age = run
    write("error", 2)
    assert age() == 99999


def test_other_checks_do_not_count(run):
    write, age = run
    write("ok", 0, dag_id="shopflow_data_quality", check="data_quality")
    assert age() == 99999
