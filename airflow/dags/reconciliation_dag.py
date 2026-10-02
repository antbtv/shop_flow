"""Daily reconciliation Postgres (laptop) <-> ClickHouse (Pi5), FR-8, ADR-0010.

wait_for_postgres  the laptop may be off: wait up to 2 h in reschedule mode (no slot held),
                   then skip. A server that answers with an error fails instead.
reconcile          cutoff T, lag check, buckets and key arbitration for the five tables.
recheck            keys extra in ClickHouse (deleted in Postgres) again after 10 min: their
                   tombstone may have been in flight.
report             always runs: writes dq_check_results, fails the run when a human is needed.
"""

from datetime import UTC, datetime, timedelta

import pendulum
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import PokeReturnValue, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable
from shopflow_common.callbacks import DEFAULT_ARGS
from shopflow_common.connections import postgres_available

RECHECK_AFTER = timedelta(minutes=10)


def _clients():
    from shopflow_common.connections import clickhouse_client, postgres_connect

    pg_conn = postgres_connect()
    client = clickhouse_client()

    def pg(sql, params=None):
        with pg_conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def ch(sql):
        return [tuple(row) for row in client.query(sql).result_rows]

    return pg_conn, pg, ch


@dag(
    dag_id="shopflow_reconciliation",
    # 20:00 Moscow: the laptop is usually on then (ADR-0010). A run missed while Pi5 was down
    # is not caught up; trigger it by hand.
    schedule=CronTriggerTimetable("0 20 * * *", timezone="Europe/Moscow"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["shopflow", "fr-8"],
)
def shopflow_reconciliation():
    wait_for_postgres = PythonSensor(
        task_id="wait_for_postgres",
        python_callable=postgres_available,
        mode="reschedule",
        poke_interval=20 * 60,
        timeout=2 * 60 * 60,
        soft_fail=True,
    )

    @task(execution_timeout=timedelta(minutes=30))
    def reconcile() -> dict:
        from shopflow_checks.outcome import serialize
        from shopflow_checks.reconciliation import (
            TABLES,
            pipeline_lagging,
            postgres_cutoff,
            reconcile_table,
        )
        from shopflow_common.errors import SourceUnavailable

        try:
            pg_conn, pg, ch = _clients()
        except SourceUnavailable:
            return {"source_unavailable": True}  # went away between the sensor and here
        try:
            cutoff = postgres_cutoff(pg)
            run = {"cutoff": cutoff.isoformat(), "lagging": pipeline_lagging(pg, ch, cutoff)}
            if not run["lagging"]:
                run["tables"] = [serialize(reconcile_table(s, pg, ch, cutoff)) for s in TABLES]
            run["finished_at"] = datetime.now(UTC).isoformat()
            return run
        finally:
            pg_conn.close()

    @task.sensor(mode="reschedule", poke_interval=120, timeout=60 * 60)
    def recheck(run: dict) -> PokeReturnValue:
        if run.get("lagging") or run.get("source_unavailable") or not any(
            t["extra_in_ch"] for t in run.get("tables", [])
        ):
            return PokeReturnValue(is_done=True, xcom_value=run)
        if datetime.now(UTC) < datetime.fromisoformat(run["finished_at"]) + RECHECK_AFTER:
            return PokeReturnValue(is_done=False)

        from shopflow_checks.reconciliation import TABLES_BY_NAME, recheck_extra
        from shopflow_common.errors import SourceUnavailable

        try:
            pg_conn, pg, ch = _clients()
        except SourceUnavailable:
            # Cannot confirm the deletions now: keep them out of the count, say so.
            for table in run["tables"]:
                table["extra_in_ch"], table["extra_total"] = [], 0
            run["recheck"] = "skipped: Postgres unavailable"
            return PokeReturnValue(is_done=True, xcom_value=run)
        try:
            for table in run["tables"]:
                if table["extra_in_ch"]:
                    still = recheck_extra(TABLES_BY_NAME[table["table"]], table["extra_in_ch"],
                                          pg, ch)
                    table["extra_in_ch"], table["extra_total"] = still, len(still)
                    table["details"]["extra_in_ch"] = still[:20]
            run["recheck"] = "done"
        finally:
            pg_conn.close()
        return PokeReturnValue(is_done=True, xcom_value=run)

    @task(trigger_rule="all_done", execution_timeout=timedelta(minutes=5))
    def report() -> str:
        from airflow.exceptions import AirflowFailException
        from shopflow_checks.outcome import CHECK, reconciliation_outcome, should_fail
        from shopflow_common.connections import clickhouse_client
        from shopflow_common.results import previous_status, write_results

        context = get_current_context()
        ti = context["ti"]
        states = ti.get_task_states(dag_id=ti.dag_id, run_ids=[ti.run_id])
        sensor_state = (states.get(ti.run_id) or {}).get("wait_for_postgres")
        run = ti.xcom_pull(task_ids="recheck") or ti.xcom_pull(task_ids="reconcile")
        outcome = reconciliation_outcome(sensor_state, run)

        client = clickhouse_client()
        previous = previous_status(client, dag_id=ti.dag_id, check_name=CHECK, run_id=ti.run_id)
        cutoff = datetime.fromisoformat(run["cutoff"]) if run and run.get("cutoff") else None
        write_results(client, dag_id=ti.dag_id, run_id=ti.run_id,
                      logical_date=context.get("logical_date"), check_name=CHECK,
                      rows=outcome.rows, cutoff=cutoff)
        if should_fail(outcome, previous):
            raise AirflowFailException(
                f"reconciliation {outcome.status} (previous run: {previous}):"
                f" {outcome.summary.details}"
            )
        return outcome.status

    run = reconcile()
    checked = recheck(run)
    wait_for_postgres >> run
    checked >> report()


shopflow_reconciliation()
