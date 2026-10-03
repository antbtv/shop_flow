"""What a reconciliation run means (FR-8, ADR-0010): pure decisions, no Airflow or drivers.

Statuses of dq_check_results: ok, violation (data differ), lagging (the stream is behind, NFR-3),
source_unavailable (the laptop was off for the whole wait), error (anything else: a broken
configuration must not hide behind "laptop off"). The DAG fails on everything but ok and a
single source_unavailable; two runs in a row without the source fail too.
"""

from __future__ import annotations

from dataclasses import dataclass, field

CHECK = "reconciliation"
# Keys kept per kind in the XCom and rechecked later; counts stay exact.
MAX_KEYS = 1000


def serialize(result) -> dict:
    """TableResult -> JSON-safe dict for XCom, key lists capped."""
    return {
        "table": result.table,
        "pg_rows": result.pg_rows,
        "ch_rows": result.ch_rows,
        "buckets_differ": result.buckets_differ,
        "in_flight": result.in_flight,
        "missing_in_ch": result.missing_in_ch[:MAX_KEYS],
        "missing_total": result.missing_total,
        "different": result.different[:MAX_KEYS],
        "different_total": result.different_total,
        "extra_in_ch": result.extra_in_ch[:MAX_KEYS],
        "extra_total": result.extra_total,
        "details": result.details(),
    }


@dataclass
class Row:
    """One dq_check_results row without the run columns (dag_id, run_id, ...)."""

    table_name: str
    status: str
    violations: int = 0
    pg_value: int | None = None
    ch_value: int | None = None
    details: dict = field(default_factory=dict)


@dataclass
class Outcome:
    status: str
    rows: list[Row]

    @property
    def summary(self) -> Row:
        return self.rows[0]


FAILED_STATES = ("failed", "upstream_failed")


def failed_upstream(states: dict[str, object] | None) -> dict[str, str]:
    """Tasks of the run that failed (or were never run because an upstream failed)."""
    failed = {}
    for task_id, state in (states or {}).items():
        name = str(getattr(state, "value", state))
        if name in FAILED_STATES:
            failed[task_id] = name
    return failed


def reconciliation_outcome(sensor_state: str | None, run: dict | None,
                           upstream: dict[str, object] | None = None) -> Outcome:
    """sensor_state: final state of wait_for_postgres; run: XCom of reconcile/recheck or None;
    upstream: states of all upstream tasks of report (task_id -> state).

    Any failed upstream task is an error, whatever the XCom still holds: a failed recheck
    leaves the XCom of reconcile, which looks like a clean run.
    The first row is the run summary (table_name ''), then one row per table.
    """
    failed = failed_upstream(upstream)
    if failed:
        reason = "upstream task failed: " + ", ".join(f"{t} {s}" for t, s in failed.items())
        return Outcome("error", [Row("", "error", details={"reason": reason})])
    if sensor_state == "skipped" or (run or {}).get("source_unavailable"):
        return Outcome("source_unavailable", [Row("", "source_unavailable")])
    if run is None:
        reason = f"wait_for_postgres {sensor_state}" if sensor_state != "success" else \
            "reconcile did not finish"
        return Outcome("error", [Row("", "error", details={"reason": reason})])
    if run.get("lagging"):
        return Outcome("lagging", [Row("", "lagging", details={
            "reason": "Postgres changed in [T, now - 5 min], no raw_events since T (NFR-3)"})])

    rows = []
    for table in run["tables"]:
        violations = table["missing_total"] + table["different_total"] + table["extra_total"]
        rows.append(Row(
            table_name=table["table"],
            status="violation" if violations else "ok",
            violations=violations,
            pg_value=table["pg_rows"],
            ch_value=table["ch_rows"],
            details={k: table[k] for k in ("buckets_differ", "in_flight")}
            | {k: v for k, v in table["details"].items() if v},
        ))
    total = sum(r.violations for r in rows)
    status = "violation" if total else "ok"
    summary = Row("", status, violations=total, details={
        "tables": {r.table_name: r.status for r in rows},
        **({"recheck": run["recheck"]} if run.get("recheck") else {}),
    })
    return Outcome(status, [summary, *rows])


def should_fail(outcome: Outcome, previous_status: str | None) -> bool:
    """A human is needed: data differ, the stream lags, an error, or two days without source."""
    if outcome.status == "ok":
        return False
    if outcome.status == "source_unavailable":
        return previous_status == "source_unavailable"
    return True
