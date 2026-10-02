"""dq_check_results writer and reader (airflow/dags/shopflow_common/results.py, ADR-0010)
on the throwaway ClickHouse, with clickhouse-connect as in the Airflow image."""

from datetime import UTC, datetime
from urllib.parse import urlparse

import pytest
from shopflow_checks.outcome import Row
from shopflow_common.results import previous_status, write_results

clickhouse_connect = pytest.importorskip("clickhouse_connect")


@pytest.fixture
def client(ch):
    ch.query("TRUNCATE TABLE shopflow.dq_check_results")
    url = urlparse(ch.url)
    return clickhouse_connect.get_client(host=url.hostname, port=url.port, username="admin",
                                         password="test-only", database="shopflow")


def write(client, run_id, status, logical_date=None, violations=0):
    write_results(client, dag_id="d", run_id=run_id, logical_date=logical_date,
                  check_name="reconciliation",
                  rows=[Row("", status, violations=violations, details={"note": "тест"}),
                        Row("orders", status, violations, 10, 10 - violations)],
                  cutoff=datetime(2026, 10, 2, 16, tzinfo=UTC))


def test_rows_round_trip_with_null_logical_date_and_json_details(client):
    write(client, "manual__1", "violation", violations=2)
    rows = client.query(
        "SELECT run_id, logical_date, table_name, status, pg_value, ch_value, violations,"
        " details FROM shopflow.dq_check_results FINAL ORDER BY table_name").result_rows
    assert [r[:7] for r in rows] == [
        ("manual__1", None, "", "violation", None, None, 2),
        ("manual__1", None, "orders", "violation", 10, 8, 2),
    ]
    assert rows[0][7] == '{"note": "тест"}'


def test_retried_task_rewrites_its_rows(client):
    write(client, "r1", "error")
    write(client, "r1", "ok")
    assert client.query("SELECT table_name, status FROM shopflow.dq_check_results FINAL"
                        " ORDER BY table_name").result_rows == [("", "ok"), ("orders", "ok")]


def test_previous_status_is_the_latest_other_scheduled_run(client):
    assert previous_status(client, dag_id="d", check_name="reconciliation",
                           run_id="scheduled__3") is None
    write(client, "scheduled__1", "ok", logical_date=datetime(2026, 10, 1, 17, tzinfo=UTC))
    write(client, "scheduled__2", "source_unavailable",
          logical_date=datetime(2026, 10, 2, 17, tzinfo=UTC))
    assert previous_status(client, dag_id="d", check_name="reconciliation",
                           run_id="scheduled__3") == "source_unavailable"
    # The current run never counts as its own previous one.
    assert previous_status(client, dag_id="d", check_name="reconciliation",
                           run_id="scheduled__2") == "ok"


def test_manual_runs_neither_start_nor_break_the_series(client):
    write(client, "scheduled__1", "source_unavailable")
    write(client, "manual__test", "ok")  # a recovery check by hand
    assert previous_status(client, dag_id="d", check_name="reconciliation",
                           run_id="scheduled__2") == "source_unavailable"
    write(client, "manual__skip_wait", "source_unavailable")  # a test of the "laptop off" path
    write(client, "scheduled__2", "ok")
    assert previous_status(client, dag_id="d", check_name="reconciliation",
                           run_id="scheduled__3") == "ok"
