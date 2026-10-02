"""Clients for ClickHouse on Pi5 and Postgres on the laptop (ADR-0010).

Connection settings come from AIRFLOW_CONN_SHOPFLOW_* in .env, never from the metadata DB.
Imports are inside the functions: DAG files stay cheap to parse for the dag-processor.
"""

from shopflow_common.errors import SourceUnavailable, is_source_unreachable

CLICKHOUSE_CONN_ID = "shopflow_clickhouse"
POSTGRES_CONN_ID = "shopflow_postgres"

# A check query is capped server-side at 120 s by the airflow_reader profile; the socket waits
# a bit longer so the server error, not a client timeout, is what the log shows.
CLICKHOUSE_CONNECT_TIMEOUT_S = 10
CLICKHOUSE_SEND_RECEIVE_TIMEOUT_S = 150


def clickhouse_client():
    """clickhouse_connect client as airflow_reader over the Pi5 compose network."""
    import clickhouse_connect
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema,
        connect_timeout=CLICKHOUSE_CONNECT_TIMEOUT_S,
        send_receive_timeout=CLICKHOUSE_SEND_RECEIVE_TIMEOUT_S,
    )


def postgres_connect():
    """psycopg2 connection to the laptop Postgres as recon_reader (read-only role).

    Raises SourceUnavailable when the server cannot be reached; any error the server itself
    returns is re-raised as is. Extras carry connect_timeout and keepalives.
    """
    import psycopg2
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    try:
        pg = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            application_name="shopflow-airflow",
            **conn.extra_dejson,
        )
    except psycopg2.OperationalError as exc:
        if is_source_unreachable(exc.pgcode, str(exc)):
            raise SourceUnavailable(f"Postgres {conn.host}:{conn.port} unreachable: {exc}") from exc
        raise
    # The role defaults to read-only; the session says so too, so a grant mistake cannot write.
    pg.set_session(readonly=True, autocommit=True)
    return pg


def postgres_available() -> bool:
    """Sensor probe: False while the laptop is unreachable, raises on any other error."""
    try:
        pg = postgres_connect()
    except SourceUnavailable:
        return False
    with pg, pg.cursor() as cur:
        cur.execute("SELECT 1")
    pg.close()
    return True
