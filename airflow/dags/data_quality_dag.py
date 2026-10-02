"""Daily FR-9 data quality checks on ClickHouse (ADR-0010).

One task runs every check in airflow/dags/sql/dq/, writes one dq_check_results row per check
plus a summary (check_name data_quality, table_name ''), and fails when any check finds a
violation or errors. Postgres is not touched: the DAG runs with the laptop off (NFR-6).
Trigger with conf {"simulate_violation": true} to test the failure path without bad data.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable
from shopflow_common.callbacks import DEFAULT_ARGS


@dag(
    dag_id="shopflow_data_quality",
    # 20:30 Moscow, half an hour after the reconciliation; does not need the laptop.
    schedule=CronTriggerTimetable("30 20 * * *", timezone="Europe/Moscow"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"simulate_violation": Param(False, type="boolean")},
    tags=["shopflow", "fr-9"],
)
def shopflow_data_quality():
    @task(execution_timeout=timedelta(minutes=20))
    def run_checks() -> dict:
        from airflow.exceptions import AirflowFailException
        from shopflow_checks.dq import SIMULATED, SUMMARY_CHECK, load_checks, run_all, summarize
        from shopflow_common.connections import clickhouse_client
        from shopflow_common.results import write_results

        context = get_current_context()
        ti = context["ti"]
        checks = load_checks()
        if context["params"].get("simulate_violation"):
            checks.append(SIMULATED)

        client = clickhouse_client()
        results = run_all(checks, lambda sql: client.query(sql).result_rows)
        summary = summarize(results)
        write_results(client, dag_id=ti.dag_id, run_id=ti.run_id,
                      logical_date=context.get("logical_date"), check_name=None,
                      rows=[(SUMMARY_CHECK, summary), *results])
        if summary.status != "ok":
            failing = {name: r.details for name, r in results if r.status != "ok"}
            raise AirflowFailException(f"data quality {summary.status}: {failing}")
        return {"status": summary.status, "checks": len(results)}

    run_checks()


shopflow_data_quality()
