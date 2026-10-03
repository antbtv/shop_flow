"""Task failure callbacks (ADR-0010, ADR-0011, FR-11).

A failed task means "a human is needed": mismatch, lagging, error, two days unreachable.
log_failure only writes the task log; alert_failure also sends a Telegram message. Neither
raises: a broken alert must not change the state of the task or hide the original failure.

Callbacks of a task run in the process of that task (the scheduler container) only when the task
itself raised. A task marked failed from outside (heartbeat timeout, kill, "mark failed" in the UI)
is handled by the dag-processor, which has no secrets, so no alert goes out then; the dashboard
shows the age of the last scheduled run instead (ADR-0011).
"""

from __future__ import annotations

import json
import logging
import os
from urllib.parse import quote

log = logging.getLogger(__name__)

MAX_SAMPLE_KEYS = 3
MAX_PROBLEM_LINES = 8
MAX_ERROR_CHARS = 500
# Keys under which a check keeps the offending primary keys (shopflow_checks.dq / reconciliation).
SAMPLE_FIELDS = ("sample_keys", "missing_in_ch", "different", "extra_in_ch")
# Short, because a callback must not hang a worker: the client default is for 2 minute queries.
LOOKUP_TIMEOUT_S = 15


def log_failure(context) -> None:
    try:
        ti = context["ti"]
        log.error(
            "ShopFlow check failed: dag=%s task=%s run=%s try=%s error=%s",
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            ti.try_number,
            context.get("exception"),
        )
    except Exception as exc:  # noqa: BLE001 - a callback must never raise (see the module doc)
        log.error("ShopFlow check failed, context unreadable: %s", type(exc).__name__)


def sample_of(details: dict | str | None) -> list[str]:
    """Up to three example keys out of a dq_check_results details document."""
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except ValueError:
            return []
    if not isinstance(details, dict):
        return []
    for field in SAMPLE_FIELDS:
        values = details.get(field)
        if isinstance(values, list) and values:
            return [str(v) for v in values[:MAX_SAMPLE_KEYS]]
    return []


def lookup_run(dag_id: str, run_id: str) -> tuple[str | None, list[dict]]:
    """Summary status and the non-ok rows of one run from dq_check_results (FINAL)."""
    from shopflow_common.connections import clickhouse_client

    client = clickhouse_client(send_receive_timeout=LOOKUP_TIMEOUT_S)
    try:
        rows = client.query(
            "SELECT check_name, table_name, toString(status), violations, details"
            " FROM shopflow.dq_check_results FINAL"
            " WHERE dag_id = {dag_id:String} AND run_id = {run_id:String}"
            " AND check_name != 'table_size'"
            " ORDER BY status DESC, check_name, table_name",
            parameters={"dag_id": dag_id, "run_id": run_id},
        ).result_rows
    finally:
        client.close()
    summary = next((status for _, table, status, _, _ in rows if table == ""), None)
    problems = [
        {"check": check, "table": table, "status": status, "violations": int(violations),
         "keys": sample_of(details)}
        for check, table, status, violations, details in rows
        if table != "" and status != "ok"
    ]
    return summary, problems


def run_url(dag_id: str, run_id: str) -> str | None:
    base = os.environ.get("AIRFLOW__API__BASE_URL", "").rstrip("/")
    return f"{base}/dags/{quote(dag_id, safe='')}/runs/{quote(run_id, safe='')}" if base else None


def format_alert(*, dag_id: str, task_id: str, run_id: str, summary: str | None,
                 problems: list[dict], error: str | None) -> str:
    """Plain text: which DAG and run, the status, what failed, a link. No secrets: only names,
    counts and primary keys."""
    status = summary or "failed"
    lines = [f"ShopFlow: {dag_id} — {status}", f"задача {task_id}, запуск {run_id}"]
    for item in problems[:MAX_PROBLEM_LINES]:
        where = item["check"] + (f" / {item['table']}" if item["table"] else "")
        line = f"• {where}: {item['status']}"
        if item["violations"]:
            line += f", нарушений {item['violations']}"
        if item["keys"]:
            line += f", например {', '.join(item['keys'])}"
        lines.append(line)
    if len(problems) > MAX_PROBLEM_LINES:
        lines.append(f"… и ещё {len(problems) - MAX_PROBLEM_LINES}")
    if not problems and error:
        # ClickHouse had nothing for this run (or was unreachable): the exception is all we know.
        lines.append(error[:MAX_ERROR_CHARS])
    url = run_url(dag_id, run_id)
    if url:
        lines.append(url)
    return "\n".join(lines)


def alert_failure(context) -> None:
    log_failure(context)
    try:
        from shopflow_common.telegram import send_message

        ti = context["ti"]
        summary, problems = None, []
        try:
            summary, problems = lookup_run(ti.dag_id, ti.run_id)
        except Exception as exc:  # noqa: BLE001 - ClickHouse down: alert with what we have
            log.warning("alert: no details from dq_check_results (%s)", type(exc).__name__)
        text = format_alert(
            dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, summary=summary,
            problems=problems, error=str(context.get("exception") or "") or None,
        )
        send_message(text)
    except Exception as exc:  # noqa: BLE001 - an alert must never break the callback
        log.error("alert failed: %s", type(exc).__name__)


# The default only logs: a DAG that wants a message opts in with alert_failure (on the DAG's
# default_args or on one task), so a new DAG never alerts, or alerts twice, by accident (ADR-0011).
DEFAULT_ARGS = {
    "owner": "shopflow",
    "retries": 0,
    "on_failure_callback": log_failure,
}
