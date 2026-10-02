"""Daily retention check and cleanup on Pi5 (NFR-5, ADR-0010).

- raw_events: no events past 30 days + grace (TTL with ttl_only_drop_parts does the dropping);
- table sizes from system.parts into dq_check_results (disk trend for the dashboard);
- Airflow task logs older than 30 days removed from the logs volume.
The metadata DB is cleaned by a host timer, not here: Airflow 3 blocks it for task code.
ClickHouse only: runs with the laptop off (NFR-6).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable
from shopflow_common.callbacks import DEFAULT_ARGS

LOGS_ROOT = "/opt/airflow/logs"


@dag(
    dag_id="shopflow_retention",
    # 04:00 Moscow: quiet hours, after the night's TTL merges.
    schedule=CronTriggerTimetable("0 4 * * *", timezone="Europe/Moscow"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["shopflow", "nfr-5"],
)
def shopflow_retention():
    @task(execution_timeout=timedelta(minutes=20))
    def retention() -> dict:
        from pathlib import Path

        from airflow.exceptions import AirflowFailException
        from shopflow_checks.dq import CheckResult, summarize
        from shopflow_checks.retention import clean_logs, raw_ttl_check, table_sizes
        from shopflow_common.connections import clickhouse_client
        from shopflow_common.results import write_results

        context = get_current_context()
        ti = context["ti"]
        client = clickhouse_client()
        try:
            ch = lambda sql: client.query(sql).result_rows  # noqa: E731

            checks = [("raw_events_ttl", raw_ttl_check(ch))]
            try:
                logs = clean_logs(Path(LOGS_ROOT))
                checks.append(("airflow_logs", CheckResult("airflow_logs", "ok", details=logs)))
            except OSError as exc:
                checks.append(("airflow_logs", CheckResult(
                    "airflow_logs", "error", details={"error": f"{type(exc).__name__}: {exc}"})))
            sizes = [("table_size", r) for r in table_sizes(ch)]
            summary = summarize(checks)
            write_results(client, dag_id=ti.dag_id, run_id=ti.run_id,
                          logical_date=context.get("logical_date"), check_name=None,
                          rows=[("retention", summary), *checks, *sizes])
        finally:
            client.close()
        if summary.status != "ok":
            failing = {name: r.details for name, r in checks if r.status != "ok"}
            raise AirflowFailException(f"retention {summary.status}: {failing}")
        return {"status": summary.status, "tables": len(sizes), **checks[1][1].details}

    retention()


shopflow_retention()
