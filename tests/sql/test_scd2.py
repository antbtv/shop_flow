"""SCD2 regression tests for dim_customers_mv and dim_products_mv (FR-3, ADR-0009).

Journal rows go into stg_*_versions as Spark writes them; the refreshable MV is run by hand
and dim_* is checked. Rules under test: order by version (LSN), not valid_from; unchanged rows
do not open a version; a delete closes the version and is not one; the first snapshot version
opens at 1970-01-01; replays and same-transaction updates collapse in the journal key.
"""

import pytest

EPOCH = "1970-01-01 00:00:00.000000"
T1 = "2026-10-01 10:00:00.000001"
T2 = "2026-10-01 11:00:00.000002"
T3 = "2026-10-01 12:00:00.000003"
T4 = "2026-10-01 13:00:00.000004"


@pytest.fixture(autouse=True)
def clean(ch):
    for table in ("stg_customer_versions", "stg_product_versions",
                  "dim_customers", "dim_products"):
        ch.query(f"TRUNCATE TABLE shopflow.{table}")


def customer(cid, valid_from, version, name="Ann", address="Moscow", segment="retail",
             snapshot=0, deleted=0):
    return (f"({cid}, '{valid_from}', '{name}', '{address}', '{segment}', {snapshot}, "
            f"{deleted}, '{valid_from[:23]}', {version})")


def load_customers(ch, *rows):
    ch.query(
        "INSERT INTO shopflow.stg_customer_versions (customer_id, valid_from, name, address,"
        " segment, is_snapshot, is_deleted, event_time, version) VALUES " + ", ".join(rows)
    )
    ch.refresh("dim_customers_mv")


def dim_customers(ch, cid=1):
    return ch.rows(
        "SELECT address, segment, toString(valid_from), ifNull(toString(valid_to), 'NULL'),"
        f" is_current FROM shopflow.dim_customers WHERE customer_id = {cid} ORDER BY version"
    )


def test_snapshot_opens_at_epoch_and_update_closes_it(ch):
    load_customers(
        ch,
        customer(1, T1, 100, snapshot=1),
        customer(1, T2, 200, address="Kazan"),
    )
    assert dim_customers(ch) == [
        ("Moscow", "retail", EPOCH, T2, "0"),
        ("Kazan", "retail", T2, "NULL", "1"),
    ]


def test_first_version_without_snapshot_opens_at_its_commit(ch):
    load_customers(ch, customer(1, T1, 100))
    assert dim_customers(ch) == [("Moscow", "retail", T1, "NULL", "1")]


def test_row_without_attribute_change_does_not_open_a_version(ch):
    # e.g. only email or updated_at changed, or the snapshot repeated the current row
    load_customers(
        ch,
        customer(1, T1, 100),
        customer(1, T2, 200),
        customer(1, T3, 300, segment="vip"),
    )
    assert dim_customers(ch) == [
        ("Moscow", "retail", T1, T3, "0"),
        ("Moscow", "vip", T3, "NULL", "1"),
    ]


def test_versions_follow_lsn_not_the_laptop_clock(ch):
    # The clock stepped back: the later change (higher LSN) has an earlier commit time.
    load_customers(
        ch,
        customer(1, T2, 100),
        customer(1, T1, 200, address="Kazan"),
    )
    rows = dim_customers(ch)
    assert [r[0] for r in rows] == ["Moscow", "Kazan"]
    assert rows[-1][4] == "1"


def test_delete_closes_the_version_and_is_not_one(ch):
    load_customers(
        ch,
        customer(1, T1, 100),
        customer(1, T2, 200, deleted=1),
    )
    assert dim_customers(ch) == [("Moscow", "retail", T1, T2, "0")]


def test_reinsert_after_delete_opens_a_new_version(ch):
    load_customers(
        ch,
        customer(1, T1, 100),
        customer(1, T2, 200, deleted=1),
        customer(1, T3, 300),
    )
    assert dim_customers(ch) == [
        ("Moscow", "retail", T1, T2, "0"),
        ("Moscow", "retail", T3, "NULL", "1"),
    ]


def test_replay_and_same_transaction_collapse_to_the_highest_lsn(ch):
    # Debezium replay: same (key, valid_from, version) twice. Two UPDATEs in one transaction:
    # same valid_from (commit time), the higher LSN wins in the journal key.
    load_customers(
        ch,
        customer(1, T1, 100),
        customer(1, T1, 100),
        customer(1, T2, 200, address="Kazan"),
        customer(1, T2, 210, address="Omsk"),
    )
    assert dim_customers(ch) == [
        ("Moscow", "retail", T1, T2, "0"),
        ("Omsk", "retail", T2, "NULL", "1"),
    ]


def test_invariants_over_many_keys(ch):
    rows = []
    for cid in range(1, 51):
        rows.append(customer(cid, T1, 100, snapshot=1))
        rows.append(customer(cid, T2, 200, address=f"Street {cid % 3}"))
        rows.append(customer(cid, T3, 300, segment=("vip" if cid % 2 else "retail")))
        if cid % 5 == 0:
            rows.append(customer(cid, T4, 400, deleted=1))
    load_customers(ch, *rows)
    # one current version per live key, none for deleted keys
    assert ch.rows(
        "SELECT countIf(c = 1), countIf(c > 1), countIf(c = 0) FROM ("
        " SELECT customer_id, sum(is_current) AS c FROM shopflow.dim_customers"
        " GROUP BY customer_id)"
    ) == [("40", "0", "10")]
    # no gaps or overlaps: every closed version ends where the next one starts
    assert ch.rows(
        "SELECT count() FROM ("
        " SELECT valid_to, leadInFrame(toNullable(valid_from)) OVER (PARTITION BY customer_id"
        "  ORDER BY version ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nxt"
        " FROM shopflow.dim_customers)"
        " WHERE nxt IS NOT NULL AND valid_to != nxt"
    ) == [("0",)]


def test_product_price_history(ch):
    ch.query(
        "INSERT INTO shopflow.stg_product_versions (product_id, valid_from, name, category,"
        " price, is_snapshot, is_deleted, event_time, version) VALUES"
        f" (7, '{T1}', 'Tea', 'food', 10.50, 1, 0, '{T1[:23]}', 100),"
        f" (7, '{T2}', 'Tea', 'food', 10.50, 0, 0, '{T2[:23]}', 200),"
        f" (7, '{T3}', 'Tea', 'food', 12.00, 0, 0, '{T3[:23]}', 300)"
    )
    ch.refresh("dim_products_mv")
    assert ch.rows(
        "SELECT toString(price), toString(valid_from), ifNull(toString(valid_to), 'NULL'),"
        " is_current FROM shopflow.dim_products WHERE product_id = 7 ORDER BY version"
    ) == [
        ("10.5", EPOCH, T3, "0"),
        ("12", T3, "NULL", "1"),
    ]
