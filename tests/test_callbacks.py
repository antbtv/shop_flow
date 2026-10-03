"""shopflow_common/callbacks.py (FR-11, ADR-0011): the alert text and the "never raise" rules.

Airflow is not needed: the context is a namespace with the fields the callbacks read, ClickHouse
and Telegram are replaced by fakes. The real dq_check_results query is tested on a throwaway
ClickHouse in tests/sql/test_alert_lookup.py.
"""

import json
from types import SimpleNamespace

import pytest
from shopflow_common import callbacks, telegram


def context(dag_id="shopflow_reconciliation", task_id="report",
            run_id="scheduled__2026-10-03T17:00:00+00:00", exception=None):
    ti = SimpleNamespace(dag_id=dag_id, task_id=task_id, run_id=run_id, try_number=1)
    return {"ti": ti, "exception": exception}


PROBLEM = {"check": "reconciliation", "table": "inventory", "status": "violation",
           "violations": 4, "keys": ["1001", "1002", "1003"]}


@pytest.fixture
def sent(monkeypatch):
    messages = []
    monkeypatch.setattr(telegram, "send_message", lambda text, **kw: messages.append(text) or True)
    return messages


@pytest.fixture(autouse=True)
def ui(monkeypatch):
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", "http://192.168.0.151:8080")


# --- sample_of --------------------------------------------------------------------------------

def test_sample_keys_come_from_json_or_dict_and_are_capped_at_three():
    assert callbacks.sample_of(json.dumps({"sample_keys": [1, 2, 3, 4, 5]})) == ["1", "2", "3"]
    assert callbacks.sample_of({"missing_in_ch": ["a"], "different": ["b"]}) == ["a"]
    assert callbacks.sample_of({"extra_in_ch": [7]}) == ["7"]


@pytest.mark.parametrize("details", [None, "", "not json", "[1, 2]", {}, {"note": "x"},
                                     {"sample_keys": []}, {"sample_keys": "abc"}])
def test_no_keys_is_an_empty_list(details):
    assert callbacks.sample_of(details) == []


# --- format_alert -----------------------------------------------------------------------------

def render(**over):
    args = dict(dag_id="shopflow_reconciliation", task_id="report", run_id="scheduled__x+00:00",
                summary="violation", problems=[PROBLEM], error=None)
    return callbacks.format_alert(**{**args, **over})


def test_text_names_dag_task_run_status_problem_and_link():
    text = render()
    assert "shopflow_reconciliation — violation" in text
    assert "задача report, запуск scheduled__x+00:00" in text
    assert "• reconciliation / inventory: violation, нарушений 4, например 1001, 1002, 1003" in text
    assert text.splitlines()[-1] == (
        "http://192.168.0.151:8080/dags/shopflow_reconciliation/runs/scheduled__x%2B00%3A00")


def test_link_is_left_out_without_a_base_url(monkeypatch):
    monkeypatch.delenv("AIRFLOW__API__BASE_URL")
    assert "http" not in render()


def test_without_rows_the_exception_text_is_the_fallback_and_is_cut():
    text = render(summary=None, problems=[], error="E" * 2000)
    assert "— failed" in text
    limit = callbacks.MAX_ERROR_CHARS
    assert "E" * limit in text and "E" * (limit + 1) not in text


def test_the_exception_is_not_repeated_when_the_rows_say_what_failed():
    assert "boom" not in render(error="boom")


def test_many_problems_are_summarised():
    problems = [{**PROBLEM, "table": f"t{i}"} for i in range(12)]
    text = render(problems=problems)
    assert text.count("• ") == callbacks.MAX_PROBLEM_LINES
    assert "… и ещё 4" in text


def test_a_check_without_a_table_or_keys_reads_cleanly():
    text = render(problems=[{"check": "dup_orders", "table": "", "status": "error",
                             "violations": 0, "keys": []}])
    assert "• dup_orders: error" in text and "нарушений" not in text


# --- the callbacks ----------------------------------------------------------------------------

def test_alert_failure_sends_one_message_with_the_run_details(monkeypatch, sent):
    monkeypatch.setattr(callbacks, "lookup_run", lambda dag, run: ("violation", [PROBLEM]))
    callbacks.alert_failure(context(exception=Exception("x")))
    assert len(sent) == 1
    assert "shopflow_reconciliation — violation" in sent[0] and "inventory" in sent[0]


def test_alert_failure_still_alerts_when_clickhouse_is_down(monkeypatch, sent):
    def down(dag, run):
        raise ConnectionError("clickhouse is down")

    monkeypatch.setattr(callbacks, "lookup_run", down)
    callbacks.alert_failure(context(exception=Exception("reconciliation error: boom")))
    assert len(sent) == 1
    assert "reconciliation error: boom" in sent[0] and "— failed" in sent[0]


def test_alert_failure_never_raises_whatever_breaks(monkeypatch):
    def broken_send(text, **kw):
        raise RuntimeError("telegram client crashed")

    monkeypatch.setattr(callbacks, "lookup_run", lambda dag, run: (None, []))
    monkeypatch.setattr(telegram, "send_message", broken_send)
    callbacks.alert_failure(context())  # must not raise
    callbacks.alert_failure({})  # not even for a context without a task instance


def test_log_failure_logs_and_does_not_send(sent, caplog):
    callbacks.log_failure(context(exception=Exception("boom")))
    assert sent == []
    assert "ShopFlow check failed" in caplog.text and "boom" in caplog.text


def test_alert_failure_also_writes_the_log_line(monkeypatch, sent, caplog):
    monkeypatch.setattr(callbacks, "lookup_run", lambda dag, run: (None, []))
    callbacks.alert_failure(context(exception=Exception("boom")))
    assert "ShopFlow check failed" in caplog.text


# --- lookup_run with a fake client ------------------------------------------------------------

class FakeClient:
    def __init__(self, rows):
        self.rows, self.closed, self.calls = rows, False, []

    def query(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        return SimpleNamespace(result_rows=self.rows)

    def close(self):
        self.closed = True


def test_lookup_returns_the_summary_and_only_the_problem_rows(monkeypatch):
    rows = [
        ("reconciliation", "", "violation", 0, "{}"),
        ("reconciliation", "inventory", "violation", 1,
         json.dumps({"missing_in_ch": [1, 2, 3, 4]})),
        ("reconciliation", "orders", "ok", 0, "{}"),
    ]
    client = FakeClient(rows)
    kwargs = {}
    monkeypatch.setattr("shopflow_common.connections.clickhouse_client",
                        lambda **kw: kwargs.update(kw) or client)
    summary, problems = callbacks.lookup_run("shopflow_reconciliation", "run1")
    assert summary == "violation"
    assert problems == [{"check": "reconciliation", "table": "inventory", "status": "violation",
                         "violations": 1, "keys": ["1", "2", "3"]}]
    assert client.closed
    assert kwargs == {"send_receive_timeout": callbacks.LOOKUP_TIMEOUT_S}
    assert client.calls[0][1] == {"dag_id": "shopflow_reconciliation", "run_id": "run1"}
    assert "FINAL" in client.calls[0][0]


def test_lookup_closes_the_client_when_the_query_fails(monkeypatch):
    class Failing(FakeClient):
        def query(self, sql, parameters=None):
            raise RuntimeError("query failed")

    client = Failing([])
    monkeypatch.setattr("shopflow_common.connections.clickhouse_client", lambda **kw: client)
    with pytest.raises(RuntimeError):
        callbacks.lookup_run("d", "r")
    assert client.closed
