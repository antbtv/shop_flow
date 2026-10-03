-- Pipeline health for the dashboard (NFR-3, ADR-0011). Filled only by mart_pipeline_health_mv.
-- kind = 'source': newest event_time per staging table and raw_events (the dashboard user has no
-- grant on them); kind = 'view': state of every refreshable MV of the database from
-- system.view_refreshes (status, last success, error text of the last failed refresh, last_error).
-- The column that does not apply to a kind is NULL, not 0 or ''.
CREATE TABLE IF NOT EXISTS shopflow.mart_pipeline_health
(
    kind              LowCardinality(String),
    name              String,
    last_event_time   Nullable(DateTime64(3, 'UTC')),
    status            LowCardinality(Nullable(String)),
    last_success_time Nullable(DateTime64(3, 'UTC')),
    last_error        Nullable(String),
    refreshed_at      DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (kind, name);
