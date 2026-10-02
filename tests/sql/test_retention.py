"""Retention (NFR-5) on the throwaway ClickHouse: the TTL mechanism of raw_events and the
check that watches it. Live raw_events reach 30 days only on 2026-10-26, so the mechanism is
shown on a copy of the table with a 1-minute TTL."""

from shopflow_checks.retention import raw_ttl_check, table_sizes

TEST_TABLE = "shopflow.raw_ttl_test"


def rows(ch):
    return lambda sql: ch.rows(sql)


def test_raw_events_ddl_keeps_30_days_and_drops_whole_parts(ch):
    ((ddl,),) = ch.rows("SELECT create_table_query FROM system.tables"
                        " WHERE database = 'shopflow' AND name = 'raw_events'")
    assert "TTL toDateTime(event_time) + toIntervalDay(30)" in ddl
    assert "ttl_only_drop_parts = 1" in ddl
    assert "PARTITION BY toYYYYMMDD(event_time)" in ddl


def test_check_sees_expired_parts_until_ttl_drops_them(ch):
    ch.query(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    # The raw_events layout with a 1-minute TTL and TTL merges paused, so the check runs
    # against a part that is expired but still there.
    ch.query(
        f"CREATE TABLE {TEST_TABLE} (topic String, event_time DateTime64(3, 'UTC'))"
        " ENGINE = MergeTree PARTITION BY toYYYYMMDD(event_time) ORDER BY topic"
        " TTL toDateTime(event_time) + INTERVAL 1 MINUTE SETTINGS ttl_only_drop_parts = 1"
    )
    ch.query(f"SYSTEM STOP TTL MERGES {TEST_TABLE}")
    ch.query(f"INSERT INTO {TEST_TABLE} VALUES ('old', now64(3) - INTERVAL 2 DAY)")
    ch.query(f"INSERT INTO {TEST_TABLE} VALUES ('new', now64(3))")

    stale = raw_ttl_check(rows(ch), TEST_TABLE, "INTERVAL 1 HOUR")
    assert (stale.status, stale.violations) == ("violation", 1)
    assert "MATERIALIZE TTL" in stale.details["hint"]

    ch.query(f"SYSTEM START TTL MERGES {TEST_TABLE}")
    ch.query(f"ALTER TABLE {TEST_TABLE} MATERIALIZE TTL SETTINGS mutations_sync = 2")
    after = raw_ttl_check(rows(ch), TEST_TABLE, "INTERVAL 1 HOUR")
    assert (after.status, after.violations) == ("ok", 0)
    # The whole expired part went, the fresh one stays.
    assert ch.rows(f"SELECT topic FROM {TEST_TABLE}") == [("new",)]

    sizes = {r.table_name: r for r in table_sizes(rows(ch))}
    assert sizes["raw_ttl_test"].ch_value == 1
    assert sizes["raw_ttl_test"].details["bytes_on_disk"] > 0
    ch.query(f"DROP TABLE {TEST_TABLE}")


def test_live_layout_check_is_quiet_on_fresh_data(ch):
    assert raw_ttl_check(rows(ch)).status == "ok"
