"""Regression tests for mart_cohort_retention (FR-6, ADR-0011).

Rows go into stg_orders as Spark writes them, the mart is refreshed by hand. The period grid
is built up to the current month, so all dates are relative to now (UTC), not fixed.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

DDL = Path(__file__).resolve().parents[2] / "clickhouse" / "ddl"
CREATE_MV = DDL / "023_mart_cohort_retention_mv.sql"
MIGRATION = DDL / "030_mart_cohort_retention_mv_future_orders.sql"


def month(offset: int) -> str:
    """First day of the month `offset` months from the current UTC month, as YYYY-MM-01."""
    now = datetime.now(UTC)
    index = now.year * 12 + (now.month - 1) + offset
    return f"{index // 12}-{index % 12 + 1:02d}-01"


def at(offset: int, day: int = 10) -> str:
    return f"{month(offset)[:8]}{day:02d} 12:00:00.000000"


@pytest.fixture(autouse=True)
def clean(ch):
    for name in ("stg_orders", "mart_cohort_retention"):
        ch.query(f"TRUNCATE TABLE shopflow.{name}")


def orders(ch, *rows):
    """rows: (order_id, customer_id, status, created_at, version, is_deleted)."""
    values = ", ".join(
        f"({oid}, {cid}, '{status}', '{created}', '{created[:23]}', {ver}, {deleted},"
        f" '{created[:23]}')"
        for oid, cid, status, created, ver, deleted in rows
    )
    ch.query(
        "INSERT INTO shopflow.stg_orders (order_id, customer_id, status, created_at, updated_at,"
        f" version, is_deleted, event_time) VALUES {values}"
    )


def grid(ch):
    """{cohort_month: [(period, customers, returned, is_partial), ...]} in period order."""
    out: dict[str, list[tuple[int, int, int, int]]] = {}
    ch.refresh("mart_cohort_retention_mv")
    rows = ch.rows(
        "SELECT toString(cohort_month), period, customers, returned, is_partial"
        " FROM shopflow.mart_cohort_retention ORDER BY cohort_month, period"
    )
    for cohort, period, customers, returned, partial in rows:
        out.setdefault(cohort, []).append(
            (int(period), int(customers), int(returned), int(partial))
        )
    return out


def test_retention_by_cohort_with_full_grid(ch):
    # Customers 1 and 2 start two months ago, 1 comes back next month; customer 3 starts last
    # month. Cohort -2 has periods 0..2, cohort -1 has 0..1; the current month is partial.
    orders(
        ch,
        (1, 1, "paid", at(-2), 1, 0),
        (2, 1, "delivered", at(-1), 1, 0),
        (3, 2, "created", at(-2, 11), 1, 0),
        (4, 3, "paid", at(-1, 12), 1, 0),
    )
    got = grid(ch)
    assert got[month(-2)] == [(0, 2, 2, 0), (1, 2, 1, 0), (2, 2, 0, 1)]
    assert got[month(-1)] == [(0, 1, 1, 0), (1, 1, 0, 1)]
    assert set(got) == {month(-2), month(-1)}


def test_second_order_in_same_month_is_not_a_return(ch):
    orders(ch, (1, 1, "paid", at(-1, 5), 1, 0), (2, 1, "paid", at(-1, 20), 1, 0))
    got = grid(ch)
    assert got[month(-1)] == [(0, 1, 1, 0), (1, 1, 0, 1)]


def test_cancelled_orders_do_not_count(ch):
    # Customer 1: first order cancelled, so the cohort is the month of the second order.
    # Customer 2 has only a cancelled order: in no cohort. Customer 3: cancelled order in a later
    # month is not a return.
    orders(
        ch,
        (1, 1, "cancelled", at(-3), 1, 0),
        (2, 1, "paid", at(-1), 1, 0),
        (3, 2, "cancelled", at(-2), 1, 0),
        (4, 3, "paid", at(-2), 1, 0),
        (5, 3, "cancelled", at(-1), 1, 0),
    )
    got = grid(ch)
    assert set(got) == {month(-2), month(-1)}
    assert got[month(-2)] == [(0, 1, 1, 0), (1, 1, 0, 0), (2, 1, 0, 1)]
    assert got[month(-1)] == [(0, 1, 1, 0), (1, 1, 0, 1)]


def test_status_change_to_cancelled_moves_customer_between_cohorts(ch):
    # Same order in two parts (a block with both versions would collapse on insert): the later
    # version cancels it, only FINAL sees that.
    orders(ch, (1, 1, "paid", at(-2), 1, 0))
    orders(ch, (1, 1, "cancelled", at(-2), 2, 0))
    orders(ch, (2, 1, "paid", at(-1), 1, 0))
    got = grid(ch)
    assert set(got) == {month(-1)}


def test_deleted_order_does_not_count(ch):
    orders(ch, (1, 1, "paid", at(-2), 1, 0))
    orders(ch, (1, 1, "paid", at(-2), 2, 1))
    orders(ch, (2, 2, "paid", at(-1), 1, 0))
    got = grid(ch)
    assert set(got) == {month(-1)}


def test_customers_and_watermark_are_consistent(ch):
    orders(ch, (1, 1, "paid", at(-2), 1, 0), (2, 2, "paid", at(-2, 15), 1, 0))
    grid(ch)
    rows = ch.rows(
        "SELECT uniqExact(customers), min(refreshed_at) > now() - INTERVAL 5 MINUTE,"
        " toString(max(source_watermark)) FROM shopflow.mart_cohort_retention"
    )
    assert rows == [("1", "1", at(-2, 15)[:23])]


def test_no_orders_gives_empty_mart(ch):
    assert grid(ch) == {}


def test_an_order_dated_in_the_future_does_not_stop_the_refresh(ch):
    # A broken clock (generator, laptop) or a manual insert can leave an order two or more months
    # ahead: `range(toUInt32(negative))` used to wrap to 4 billion elements and fail every refresh
    # (found by the 5.14 review). Such an order is not a return and not a cohort start.
    orders(
        ch,
        (1, 1, "paid", at(-1), 1, 0),
        (2, 1, "paid", at(2), 1, 0),   # customer 1, two months ahead: not a return
        (3, 2, "paid", at(3), 1, 0),   # customer 2, only a future order: no cohort at all
    )
    got = grid(ch)
    assert got == {month(-1): [(0, 1, 1, 0), (1, 1, 0, 1)]}


def query_body(path: Path) -> str:
    text = path.read_text()
    return text[text.index("\nWITH\n"):].strip()


def test_the_migration_carries_the_same_query_as_the_create_statement():
    assert query_body(MIGRATION) == query_body(CREATE_MV)


def test_the_migration_repairs_an_mv_that_still_has_the_old_query(ch):
    """Pi5 had the old definition: CREATE ... IF NOT EXISTS does not change it, 030 does."""
    migration = MIGRATION.read_text()
    old = migration.replace(" AND created_at <= now('UTC')", "")
    assert old != migration
    ch.query(old)                                  # the MV as it was before the fix
    try:
        orders(ch, (1, 1, "paid", at(-1), 1, 0), (2, 2, "paid", at(3), 1, 0))
        with pytest.raises(AssertionError, match="REFRESH_FAILED"):
            grid(ch)
        ch.query(migration)                        # what apply-ddl.sh runs on Pi5
        assert grid(ch) == {month(-1): [(0, 1, 1, 0), (1, 1, 0, 1)]}
    finally:
        ch.query(migration)
    ch.query(migration)                            # idempotent: the same statement again
