"""What a reconciliation run means (airflow/dags/shopflow_checks/outcome.py, ADR-0010)."""

from shopflow_checks.outcome import reconciliation_outcome, should_fail


def table(name, missing=0, different=0, extra=0, in_flight=0):
    return {"table": name, "pg_rows": 10, "ch_rows": 10 - missing, "buckets_differ": 1,
            "in_flight": in_flight, "missing_total": missing, "different_total": different,
            "extra_total": extra, "details": {"missing_in_ch": ["7"] if missing else []}}


CLEAN = {"cutoff": "2026-10-02T16:00:00+00:00", "lagging": False,
         "tables": [table("orders", in_flight=3), table("customers")]}


def test_clean_run_is_ok_and_in_flight_rows_do_not_count():
    outcome = reconciliation_outcome("success", CLEAN)
    assert outcome.status == "ok"
    assert [(r.table_name, r.status, r.violations) for r in outcome.rows] == [
        ("", "ok", 0), ("orders", "ok", 0), ("customers", "ok", 0)]
    assert outcome.rows[1].details["in_flight"] == 3
    assert not should_fail(outcome, None)


def test_any_violation_fails_the_run():
    run = {**CLEAN, "tables": [table("orders", missing=1), table("customers", different=2)]}
    outcome = reconciliation_outcome("success", run)
    assert outcome.status == "violation"
    assert outcome.summary.violations == 3
    assert outcome.rows[1].details["missing_in_ch"] == ["7"]
    assert should_fail(outcome, "ok")


def test_lagging_stream_is_its_own_status_and_fails():
    outcome = reconciliation_outcome("success", {"cutoff": "x", "lagging": True})
    assert outcome.status == "lagging"
    assert should_fail(outcome, None)


def test_laptop_off_once_is_tolerated_twice_fails():
    outcome = reconciliation_outcome("skipped", None)
    assert outcome.status == "source_unavailable"
    assert not should_fail(outcome, "ok")
    assert not should_fail(outcome, None)
    assert should_fail(outcome, "source_unavailable")


def test_laptop_gone_between_sensor_and_reconcile_is_unavailable_too():
    outcome = reconciliation_outcome("success", {"source_unavailable": True})
    assert outcome.status == "source_unavailable"


def test_failed_sensor_is_an_error_not_unavailability():
    # pg_hba or a wrong password: the sensor fails instead of waiting (shopflow_common.errors).
    outcome = reconciliation_outcome("failed", None)
    assert outcome.status == "error"
    assert "wait_for_postgres failed" in outcome.summary.details["reason"]
    assert should_fail(outcome, "source_unavailable")


def test_reconcile_crash_is_an_error():
    outcome = reconciliation_outcome("success", None)
    assert outcome.status == "error"
    assert should_fail(outcome, None)
