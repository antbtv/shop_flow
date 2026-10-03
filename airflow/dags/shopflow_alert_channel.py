"""Probe of the alert channel Pi5 -> Telegram Bot API (ADR-0012, FR-11).

Every 3 hours: a GET without token, the result goes to dq_check_results (check telegram_reachable)
so that the dashboard shows a dead tunnel. The task fails on error to make the run red in the UI,
but the callback is log_failure only: this DAG must not try to alert through the broken channel.
ClickHouse only: runs with the laptop off (NFR-6).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable
from shopflow_common.callbacks import DEFAULT_ARGS


@dag(
    dag_id="shopflow_alert_channel",
    # :40 every third hour Moscow: away from 20:00 reconciliation, 20:30 DQ and 04:00 retention.
    schedule=CronTriggerTimetable("40 */3 * * *", timezone="Europe/Moscow"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["shopflow", "fr-11"],
)
def shopflow_alert_channel():
    @task(execution_timeout=timedelta(minutes=2))
    def probe_channel() -> str:
        from airflow.exceptions import AirflowFailException
        from shopflow_checks.channel import CHECK, api_url, probe
        from shopflow_common.connections import clickhouse_client
        from shopflow_common.results import write_results

        context = get_current_context()
        ti = context["ti"]
        row = probe(api_url())
        client = clickhouse_client()
        try:
            write_results(client, dag_id=ti.dag_id, run_id=ti.run_id,
                          logical_date=context.get("logical_date"), check_name=CHECK, rows=[row])
        finally:
            client.close()
        if row.status != "ok":
            raise AirflowFailException(f"alert channel unreachable: {row.details}")
        return row.status

    probe_channel()


shopflow_alert_channel()
