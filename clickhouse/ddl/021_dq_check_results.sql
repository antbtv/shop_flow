-- Results of the Airflow checks (ADR-0010): reconciliation (FR-8), data quality (FR-9),
-- retention (NFR-5). One row per (DAG run, check, table); a retried task rewrites its row, so the
-- version is checked_at, not an LSN: this table is not a copy of a Postgres row (ADR-0006).
-- Read with FINAL. Written only by airflow_reader; the dashboard (M5) reads it. No TTL (NFR-5).
CREATE TABLE IF NOT EXISTS shopflow.dq_check_results
(
    dag_id       LowCardinality(String),
    run_id       String,
    -- NULL for a manual run in Airflow 3
    logical_date Nullable(DateTime64(6, 'UTC')),
    check_name   LowCardinality(String),
    -- '' for a check that is not about one table
    table_name   LowCardinality(String), -- noqa: RF04
    status       Enum8(
        'ok' = 1, 'violation' = 2, 'lagging' = 3, 'source_unavailable' = 4, 'error' = 5
    ),
    pg_value     Nullable(Int128),
    ch_value     Nullable(Int128),
    violations   UInt64,
    -- reconciliation cutoff T, by the Postgres clock
    cutoff       Nullable(DateTime64(6, 'UTC')),
    -- JSON: sample keys, hints (quarantine, backfill)
    details      String,
    checked_at   DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (dag_id, run_id, check_name, table_name);
