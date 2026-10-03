"""grafana_reader (M5, ADR-0011): grants, read-only profile and constraints as created by
scripts/create-ch-users.sh on a throwaway ClickHouse, checked by connecting as that user.
"""

import hashlib
import os
import subprocess
import urllib.error
import urllib.request

import pytest
from conftest import PASSWORD, ROOT, USER

GRAFANA_PASSWORD = "grafana-test-only"
MARTS = ("mart_revenue_daily", "mart_funnel_daily", "mart_cohort_retention",
         "mart_top_products_daily", "mart_inventory_current", "mart_pipeline_health")


def create_users(ch, **extra):
    env = {**os.environ, "CLICKHOUSE_URL": ch.url, "CLICKHOUSE_USER": USER,
           "CLICKHOUSE_PASSWORD": PASSWORD, "CLICKHOUSE_SPARK_PASSWORD": "spark-test-only",
           "LAN_SUBNET": ch.subnet, "PI5_COMPOSE_SUBNET": ch.subnet,
           "CLICKHOUSE_AIRFLOW_PASSWORD": "airflow-test-only", **extra}
    return subprocess.run([str(ROOT / "scripts/create-ch-users.sh")], cwd=ROOT, env=env,
                          capture_output=True, text=True)


@pytest.fixture(scope="module")
def grafana(ch):
    done = create_users(ch, CLICKHOUSE_GRAFANA_PASSWORD=GRAFANA_PASSWORD)
    assert done.returncode == 0, done.stderr
    return ch


def as_grafana(ch, sql):
    """(http status, body) of one statement run as grafana_reader."""
    req = urllib.request.Request(
        f"{ch.url}/?database=shopflow&default_format=TSV", data=sql.encode(),
        headers={"X-ClickHouse-User": "grafana_reader", "X-ClickHouse-Key": GRAFANA_PASSWORD},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def test_grants_are_exactly_the_marts_and_dq_results(grafana):
    grants = {line.split("\t")[0] for line in
              grafana.query("SHOW GRANTS FOR grafana_reader").splitlines()}
    assert grants == {f"GRANT SELECT ON shopflow.{t} TO grafana_reader"
                      for t in (*MARTS, "dq_check_results")}


@pytest.mark.parametrize("table", (*MARTS, "dq_check_results"))
def test_marts_and_results_are_readable(grafana, table):
    status, body = as_grafana(grafana, f"SELECT count() FROM shopflow.{table}")
    assert status == 200, body


def test_dq_results_are_readable_with_final(grafana):
    status, body = as_grafana(grafana, "SELECT count() FROM shopflow.dq_check_results FINAL")
    assert status == 200, body


@pytest.mark.parametrize("table", ("stg_orders", "stg_order_items", "stg_inventory",
                                   "raw_events", "fact_orders", "dim_products",
                                   "dim_customers"))
def test_other_tables_are_denied(grafana, table):
    status, body = as_grafana(grafana, f"SELECT count() FROM shopflow.{table}")
    assert status != 200 and "ACCESS_DENIED" in body, body


def test_system_parts_is_denied(grafana):
    status, body = as_grafana(grafana, "SELECT count() FROM system.parts")
    assert status != 200 and "ACCESS_DENIED" in body, body


def test_no_writes_even_into_a_granted_mart(grafana):
    status, body = as_grafana(grafana, "INSERT INTO shopflow.mart_pipeline_health (kind, name,"
                              " refreshed_at) VALUES ('source', 'x', now())")
    assert status != 200 and ("ACCESS_DENIED" in body or "READONLY" in body), body
    status, body = as_grafana(grafana, "CREATE TABLE shopflow.t (a UInt8) ENGINE = Memory")
    assert status != 200, body


@pytest.mark.parametrize("setting", ("max_memory_usage = 1000000000", "max_execution_time = 60",
                                     "max_threads = 8", "max_rows_to_read = 100000000"))
def test_settings_above_the_profile_maximum_are_rejected(grafana, setting):
    status, body = as_grafana(grafana, f"SELECT 1 SETTINGS {setting}")
    assert status != 200 and "SETTING_CONSTRAINT_VIOLATION" in body, body


def test_settings_within_the_profile_are_accepted(grafana):
    # What the plugin sends: its query timeout may equal the profile maximum, not exceed it.
    status, body = as_grafana(grafana, "SELECT 1 SETTINGS max_execution_time = 30,"
                              " max_memory_usage = 134217728")
    assert status == 200, body


def test_the_session_cannot_lift_readonly(grafana):
    status, body = as_grafana(grafana, "SELECT 1 SETTINGS readonly = 0")
    assert status != 200 and ("SETTING_CONSTRAINT_VIOLATION" in body or "READONLY" in body), body


def test_the_script_is_idempotent_and_a_password_change_takes_effect(grafana):
    again = create_users(grafana, CLICKHOUSE_GRAFANA_PASSWORD=GRAFANA_PASSWORD)
    assert again.returncode == 0, again.stderr
    assert as_grafana(grafana, "SELECT 1")[0] == 200
    changed = create_users(grafana, CLICKHOUSE_GRAFANA_PASSWORD="changed-password")
    assert changed.returncode == 0, changed.stderr
    assert as_grafana(grafana, "SELECT 1")[0] != 200
    restored = create_users(grafana, CLICKHOUSE_GRAFANA_PASSWORD=GRAFANA_PASSWORD)
    assert restored.returncode == 0, restored.stderr
    assert as_grafana(grafana, "SELECT 1")[0] == 200


def test_without_a_password_the_user_is_skipped_and_the_rest_still_works(ch):
    done = create_users(ch, CLICKHOUSE_GRAFANA_PASSWORD="")
    assert done.returncode == 0, done.stderr
    assert "skip grafana_reader" in done.stdout
    assert "ok: GRANT INSERT ON shopflow.raw_events" in done.stdout


def test_the_password_and_its_hash_stay_out_of_the_output(grafana):
    done = create_users(grafana, CLICKHOUSE_GRAFANA_PASSWORD=GRAFANA_PASSWORD)
    assert GRAFANA_PASSWORD not in done.stdout + done.stderr
    assert hashlib.sha256(GRAFANA_PASSWORD.encode()).hexdigest() not in done.stdout + done.stderr
