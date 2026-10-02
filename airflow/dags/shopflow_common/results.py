"""dq_check_results (ADR-0010): where every check DAG writes its outcome.

One row per (dag_id, run_id, check_name, table_name); ReplacingMergeTree(checked_at), so a
retried task rewrites its rows. The client is passed in: the module is tested on a throwaway
ClickHouse without Airflow.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

TABLE = "shopflow.dq_check_results"
COLUMNS = (
    "dag_id", "run_id", "logical_date", "check_name", "table_name", "status",
    "pg_value", "ch_value", "violations", "cutoff", "details", "checked_at",
)


def write_results(client, *, dag_id: str, run_id: str, logical_date: datetime | None,
                  check_name: str | None, rows, cutoff: datetime | None = None) -> None:
    """rows: objects with table_name, status, violations, pg_value, ch_value, details; or
    (check_name, row) pairs when check_name is None (one DAG run, several checks)."""
    checked_at = datetime.now(UTC)
    pairs = [(check_name, r) for r in rows] if check_name is not None else rows
    data = [
        [dag_id, run_id, logical_date, name, r.table_name, r.status,
         r.pg_value, r.ch_value, r.violations, cutoff,
         json.dumps(r.details, ensure_ascii=False, default=str), checked_at]
        for name, r in pairs
    ]
    client.insert(TABLE, data, column_names=list(COLUMNS))


def previous_status(client, *, dag_id: str, check_name: str, run_id: str) -> str | None:
    """Summary status of the latest earlier run of this check (table_name '')."""
    rows = client.query(
        f"SELECT status FROM {TABLE} FINAL"
        " WHERE dag_id = {dag_id:String} AND check_name = {check:String}"
        " AND table_name = '' AND run_id != {run_id:String}"
        " ORDER BY checked_at DESC LIMIT 1",
        parameters={"dag_id": dag_id, "check": check_name, "run_id": run_id},
    ).result_rows
    return rows[0][0] if rows else None
