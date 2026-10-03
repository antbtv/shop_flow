"""callbacks.lookup_run on the throwaway ClickHouse: the query behind the Telegram text
(dq_check_results FINAL by dag and run, ADR-0011), clickhouse-connect as in the Airflow image."""

from datetime import UTC, datetime
from urllib.parse import urlparse

import pytest
from shopflow_checks.outcome import Row
from shopflow_common import callbacks
from shopflow_common.results import write_results

clickhouse_connect = pytest.importorskip("clickhouse_connect")


@pytest.fixture
def client(ch, monkeypatch):
    ch.query("TRUNCATE TABLE shopflow.dq_check_results")
    url = urlparse(ch.url)
    made = clickhouse_connect.get_client(host=url.hostname, port=url.port, username="admin",
                                         password="test-only", database="shopflow")
    monkeypatch.setattr("shopflow_common.connections.clickhouse_client", lambda **kw: made)
    return made


def write(client, run_id, rows, dag_id="shopflow_reconciliation"):
    write_results(client, dag_id=dag_id, run_id=run_id, logical_date=None,
                  check_name=None, rows=rows, cutoff=datetime(2026, 10, 3, tzinfo=UTC))


def test_summary_and_problem_rows_of_one_run_worst_first(client):
    write(client, "r1", [
        ("reconciliation", Row("", "violation", details={})),
        ("reconciliation", Row("orders", "ok")),
        ("reconciliation", Row("customers", "error", details={"different": [5, 6, 7, 8]})),
        ("reconciliation", Row("inventory", "violation", violations=2,
                               details={"missing_in_ch": [1, 2]})),
        ("table_size", Row("raw_events", "ok", ch_value=10)),
    ])
    summary, problems = callbacks.lookup_run("shopflow_reconciliation", "r1")
    assert summary == "violation"
    # error (5) before violation (2); ok rows and table_size are not problems
    assert [(p["table"], p["status"], p["violations"], p["keys"]) for p in problems] == [
        ("customers", "error", 0, ["5", "6", "7"]),
        ("inventory", "violation", 2, ["1", "2"]),
    ]


def test_other_runs_and_dags_do_not_leak_into_the_alert(client):
    write(client, "r1", [("reconciliation", Row("", "ok"))])
    write(client, "r2", [("reconciliation", Row("", "violation")),
                         ("reconciliation", Row("orders", "violation", violations=1))])
    write(client, "r1", [("data_quality", Row("", "error")),
                         ("data_quality", Row("x", "error"))], dag_id="shopflow_data_quality")
    assert callbacks.lookup_run("shopflow_reconciliation", "r1") == ("ok", [])


def test_a_rewritten_row_counts_once(client):
    write(client, "r1", [("reconciliation", Row("", "error")),
                         ("reconciliation", Row("orders", "error"))])
    write(client, "r1", [("reconciliation", Row("", "violation")),
                         ("reconciliation", Row("orders", "violation", violations=1))])
    summary, problems = callbacks.lookup_run("shopflow_reconciliation", "r1")
    assert summary == "violation"
    assert [(p["table"], p["status"]) for p in problems] == [("orders", "violation")]


def test_an_unknown_run_has_no_summary_and_no_problems(client):
    assert callbacks.lookup_run("shopflow_reconciliation", "never") == (None, [])
