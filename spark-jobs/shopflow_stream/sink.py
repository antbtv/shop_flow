"""ClickHouse catalog for the official Spark connector (DataSource V2 over HTTP, ADR-0008).

Credentials come from the container environment and are set in code, never on the
spark-submit command line. Spark UI redacts conf keys that contain "password".
"""

import os

CATALOG = "clickhouse"
DATABASE = "shopflow"

# client-v2 socket options, in milliseconds. Without them a Pi5 that silently drops off
# Wi-Fi hangs the batch forever instead of failing it (ADR-0008).
CONNECTION_TIMEOUT_MS = 10_000
SOCKET_TIMEOUT_MS = 120_000


def catalog_conf(env: dict[str, str] | None = None) -> dict[str, str]:
    """Spark conf entries that register the ClickHouse catalog for the writer user."""
    env = os.environ if env is None else env
    prefix = f"spark.sql.catalog.{CATALOG}"
    return {
        prefix: "com.clickhouse.spark.ClickHouseCatalog",
        f"{prefix}.host": env["PI5_HOST"],
        f"{prefix}.protocol": "http",
        f"{prefix}.http_port": env.get("CLICKHOUSE_HTTP_PORT", "8123"),
        f"{prefix}.user": env.get("CLICKHOUSE_SPARK_USER", "spark_writer"),
        f"{prefix}.password": env["CLICKHOUSE_SPARK_PASSWORD"],
        f"{prefix}.database": DATABASE,
        f"{prefix}.option.connection_timeout": str(CONNECTION_TIMEOUT_MS),
        f"{prefix}.option.socket_timeout": str(SOCKET_TIMEOUT_MS),
    }


def table(name: str) -> str:
    """Fully qualified connector table name, e.g. clickhouse.shopflow.raw_events."""
    return f"{CATALOG}.{DATABASE}.{name}"
