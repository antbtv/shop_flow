"""Manual smoke DAG (4.7): can Airflow on Pi5 reach ClickHouse and the laptop Postgres?

check_postgres fails with SourceUnavailable when the laptop is off; any other Postgres error
fails as itself (ADR-0010).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from shopflow_common.callbacks import DEFAULT_ARGS


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["shopflow", "smoke"],
)
def shopflow_healthcheck():
    @task(execution_timeout=timedelta(minutes=2))
    def check_clickhouse() -> dict:
        from shopflow_common.connections import clickhouse_client

        client = clickhouse_client()
        user, version, orders = client.query(
            "SELECT currentUser(), version(), (SELECT count() FROM shopflow.stg_orders FINAL)"
        ).result_rows[0]
        return {"user": user, "version": version, "stg_orders": orders}

    @task(execution_timeout=timedelta(minutes=2))
    def check_postgres() -> dict:
        from shopflow_common.connections import postgres_connect

        pg = postgres_connect()
        with pg.cursor() as cur:
            cur.execute(
                "SELECT current_user, host(inet_client_addr()),"
                " current_setting('transaction_read_only'), (SELECT count(*) FROM orders)"
            )
            user, client_addr, read_only, orders = cur.fetchone()
        pg.close()
        return {"user": user, "client_addr": client_addr, "read_only": read_only, "orders": orders}

    check_clickhouse()
    check_postgres()


shopflow_healthcheck()
