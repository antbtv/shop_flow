"""When a reconciliation run alerts (FR-11, ADR-0011): every status of outcome.py, and the rule
"any failed upstream task of report is an error".

Pure decisions, no Airflow: report raises (and so calls alert_failure) exactly when should_fail
says so. Which callback is wired to which task is checked inside the Airflow image in test_dags.py.
"""

import pytest
from shopflow_checks.outcome import failed_upstream, reconciliation_outcome, should_fail


def table(name, missing=0):
    return {"table": name, "pg_rows": 10, "ch_rows": 10 - missing, "buckets_differ": 0,
            "in_flight": 0, "missing_total": missing, "different_total": 0, "extra_total": 0,
            "details": {}}


RUN = {"cutoff": "2026-10-03T16:00:00+00:00", "lagging": False, "tables": [table("orders")]}
OK_STATES = {"wait_for_postgres": "success", "reconcile": "success", "recheck": "success"}


def outcome_for(status):
    """A real Outcome of the given status, built the way report builds it."""
    if status == "ok":
        return reconciliation_outcome("success", RUN, OK_STATES)
    if status == "violation":
        run = {**RUN, "tables": [table("orders", missing=2)]}
        return reconciliation_outcome("success", run, OK_STATES)
    if status == "lagging":
        return reconciliation_outcome("success", {**RUN, "lagging": True}, OK_STATES)
    if status == "source_unavailable":
        skipped = {"wait_for_postgres": "skipped", "reconcile": "skipped", "recheck": "skipped"}
        return reconciliation_outcome("skipped", None, skipped)
    if status == "error":
        return reconciliation_outcome("success", None, OK_STATES)
    raise AssertionError(status)


# Status of this run, status of the previous scheduled run -> does a human get a message.
ALERT_TABLE = [
    ("ok", None, False),
    ("ok", "violation", False),
    ("violation", None, True),
    ("violation", "ok", True),
    ("lagging", "ok", True),
    ("error", "ok", True),
    ("error", None, True),
    ("source_unavailable", None, False),         # the first quiet night
    ("source_unavailable", "ok", False),
    ("source_unavailable", "violation", False),  # a failed run is not "unreachable twice"
    ("source_unavailable", "source_unavailable", True),
]


@pytest.mark.parametrize(("status", "previous", "alerts"), ALERT_TABLE)
def test_alert_decision_per_status(status, previous, alerts):
    outcome = outcome_for(status)
    assert outcome.status == status
    assert should_fail(outcome, previous) is alerts


def test_every_status_of_the_module_is_covered():
    seen = {status for status, _, _ in ALERT_TABLE}
    assert seen == {"ok", "violation", "lagging", "error", "source_unavailable"}


# --- any failed upstream task is an error ------------------------------------------------------

@pytest.mark.parametrize("failed_task", ["wait_for_postgres", "reconcile", "recheck"])
@pytest.mark.parametrize("state", ["failed", "upstream_failed"])
def test_any_failed_upstream_task_is_an_error_and_alerts(failed_task, state):
    upstream = {**OK_STATES, failed_task: state}
    outcome = reconciliation_outcome("success", RUN, upstream)
    assert outcome.status == "error"
    assert f"{failed_task} {state}" in outcome.summary.details["reason"]
    assert should_fail(outcome, None)
    assert should_fail(outcome, "source_unavailable")


def test_failed_recheck_does_not_hide_behind_the_xcom_of_reconcile():
    # recheck failed, so report pulls the XCom of reconcile: a clean-looking run.
    upstream = {**OK_STATES, "recheck": "failed"}
    assert reconciliation_outcome("success", RUN, OK_STATES).status == "ok"
    assert reconciliation_outcome("success", RUN, upstream).status == "error"


def test_failed_task_wins_over_a_laptop_that_was_off():
    # reconcile raised (not SourceUnavailable) after the sensor passed: not "laptop off".
    upstream = {**OK_STATES, "reconcile": "failed"}
    outcome = reconciliation_outcome("success", {"source_unavailable": True}, upstream)
    assert outcome.status == "error"


def test_skipped_tasks_are_not_failures():
    # The sensor gave up (soft_fail): reconcile and recheck are skipped, report says
    # source_unavailable, not error.
    skipped = {"wait_for_postgres": "skipped", "reconcile": "skipped", "recheck": "skipped"}
    assert failed_upstream(skipped) == {}
    assert reconciliation_outcome("skipped", None, skipped).status == "source_unavailable"


@pytest.mark.parametrize("states", [None, {}, {"reconcile": None},
                                    {"reconcile": "success", "recheck": "removed"}])
def test_missing_states_are_not_failures(states):
    assert failed_upstream(states) == {}


def test_state_may_be_an_enum_with_a_value():
    class State:  # TaskInstanceState is a str enum; anything with .value must work too
        value = "upstream_failed"

    assert failed_upstream({"recheck": State()}) == {"recheck": "upstream_failed"}
