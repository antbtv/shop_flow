"""Postgres <-> ClickHouse reconciliation (FR-8, ADR-0010).

The two databases share no order of events, so a plain count(*) sees rows in flight as
mismatches. The check:
1. cutoff T = Postgres now() - 15 min, rounded to a second, one clock for both sides;
2. per table, both databases aggregate live rows into 256 buckets (count, sum of a 64-bit
   fingerprint of the business columns); only the aggregates leave the databases;
3. buckets that differ are a coarse filter: their keys are fetched from ClickHouse, then from
   Postgres, and each key is judged on its own. A key changed in Postgres at or after T, or gone
   from it, is in flight and not counted; extra keys in ClickHouse are rechecked later (a
   tombstone may be on its way).

The cutoff applies on both sides where ClickHouse has the same column (orders, inventory:
updated_at; order_items: created_at of the order), so only real races reach the key step.
The SCD2 journals (customers, products) have no updated_at: ClickHouse takes the latest version
of every key, Postgres cuts by updated_at, the key step sorts it out.

No driver imports: callers pass pg(sql, params) and ch(sql) returning rows as tuples. Postgres
SQL uses psycopg2 placeholders; ClickHouse SQL gets only validated literals (ints, a timestamp).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

BUCKETS = 256
CUTOFF_LAG = timedelta(minutes=15)
SAMPLE = 20
# Key arbitration fetches this many differing buckets at a time and keeps at most KEEP_KEYS
# keys of each kind (counts stay exact): a run where most buckets differ must not pull a whole
# table into the scheduler container (1152m, shared with another task, ADR-0004).
ARBITRATION_BATCH = 16
KEEP_KEYS = 1000

Rows = list[tuple]
PgQuery = Callable[[str, dict], Rows]
ChQuery = Callable[[str], Rows]


@dataclass(frozen=True)
class Column:
    """A fingerprint column. kind: int, text, opt_text (NULL in Postgres, '' in ClickHouse),
    money (Decimal(10,2) as an integer of cents), ts_ms (integer milliseconds of the epoch: the
    precision both sides keep, DateTime64(3) in stg_*)."""

    name: str
    kind: str


@dataclass(frozen=True)
class TableSpec:
    name: str
    key: tuple[str, ...]
    columns: tuple[Column, ...]
    # Subqueries that yield the key, the columns and `cut` (the time compared with T).
    pg_source: str
    ch_source: str
    # False for the journals: ClickHouse has no comparable time, it gives every live key.
    ch_cut: bool = True


def _cols(spec: str) -> tuple[Column, ...]:
    return tuple(Column(*item.split(":")) for item in spec.split())


TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        name="customers",
        key=("customer_id",),
        columns=_cols("customer_id:int name:text address:opt_text segment:opt_text"),
        pg_source="SELECT customer_id, name, address, segment, updated_at AS cut FROM customers",
        ch_source=(
            "SELECT customer_id, argMax(name, version) AS name,"
            " argMax(address, version) AS address, argMax(segment, version) AS segment"
            " FROM shopflow.stg_customer_versions FINAL GROUP BY customer_id"
            " HAVING argMax(is_deleted, version) = 0"
        ),
        ch_cut=False,
    ),
    TableSpec(
        name="products",
        key=("product_id",),
        columns=_cols("product_id:int name:text category:text price:money"),
        pg_source="SELECT product_id, name, category, price, updated_at AS cut FROM products",
        ch_source=(
            "SELECT product_id, argMax(name, version) AS name,"
            " argMax(category, version) AS category, argMax(price, version) AS price"
            " FROM shopflow.stg_product_versions FINAL GROUP BY product_id"
            " HAVING argMax(is_deleted, version) = 0"
        ),
        ch_cut=False,
    ),
    TableSpec(
        name="orders",
        key=("order_id",),
        columns=_cols("order_id:int customer_id:int status:text created_at:ts_ms updated_at:ts_ms"),
        pg_source=(
            "SELECT order_id, customer_id, status, created_at, updated_at, updated_at AS cut"
            " FROM orders"
        ),
        ch_source=(
            "SELECT order_id, customer_id, status, created_at, updated_at, updated_at AS cut"
            " FROM shopflow.stg_orders FINAL WHERE is_deleted = 0"
        ),
    ),
    TableSpec(
        name="order_items",
        key=("order_item_id",),
        columns=_cols("order_item_id:int order_id:int product_id:int quantity:int"
                      " price_at_order:money"),
        # cut = creation of the order: items have no updated_at and are written with the order.
        pg_source=(
            "SELECT i.order_item_id, i.order_id, i.product_id, i.quantity, i.price_at_order,"
            " o.created_at AS cut FROM order_items AS i JOIN orders AS o USING (order_id)"
        ),
        # LEFT JOIN: an item whose order is missing in ClickHouse (quarantine, lag) still counts,
        # its cut is the 1970 default and it is compared, not hidden.
        ch_source=(
            "SELECT i.order_item_id AS order_item_id, i.order_id AS order_id,"
            " i.product_id AS product_id, i.quantity AS quantity,"
            " i.price_at_order AS price_at_order, o.created_at AS cut"
            " FROM shopflow.stg_order_items AS i FINAL"
            " LEFT JOIN (SELECT order_id, created_at FROM shopflow.stg_orders FINAL) AS o"
            " ON i.order_id = o.order_id WHERE i.is_deleted = 0"
        ),
    ),
    TableSpec(
        name="inventory",
        key=("product_id", "warehouse_id"),
        columns=_cols("product_id:int warehouse_id:int quantity:int updated_at:ts_ms"),
        pg_source=(
            "SELECT product_id, warehouse_id, quantity, updated_at, updated_at AS cut"
            " FROM inventory"
        ),
        ch_source=(
            "SELECT product_id, warehouse_id, quantity, updated_at, updated_at AS cut"
            " FROM shopflow.stg_inventory FINAL WHERE is_deleted = 0"
        ),
    ),
)
TABLES_BY_NAME = {spec.name: spec for spec in TABLES}


# --- SQL fragments, one per dialect ------------------------------------------------------------

_PG_CANON = {
    "int": "{c}::text",
    "text": "{c}",
    "opt_text": "coalesce({c}, '')",
    "money": "({c} * 100)::bigint::text",
    "ts_ms": "floor(extract(epoch FROM {c}) * 1000)::bigint::text",
}
_CH_CANON = {
    "int": "toString({c})",
    "text": "toString({c})",
    "opt_text": "toString({c})",
    "money": "toString(toInt64({c} * 100))",
    "ts_ms": "toString(toUnixTimestamp64Milli({c}))",
}


def _bucket(spec: TableSpec, pg: bool = False) -> str:
    """Same integer arithmetic in both databases. psycopg2 reads % as a placeholder, so the
    Postgres variant spells the modulo %%."""
    mod = "%%" if pg else "%"
    if spec.key == ("product_id", "warehouse_id"):
        # bigint in Postgres: product_id * 1000 leaves int4 range past ~2.1M products.
        product = "product_id::bigint" if pg else "toInt64(product_id)"
        return f"(({product} * 1000 + warehouse_id) {mod} {BUCKETS})"
    return f"({spec.key[0]} {mod} {BUCKETS})"


def pg_fingerprint(spec: TableSpec) -> str:
    """First 8 bytes of md5 of the canonical row, big-endian, as a signed bigint."""
    parts = ", ".join(_PG_CANON[c.kind].format(c=c.name) for c in spec.columns)
    return f"('x' || left(md5(concat_ws(E'\\x1f', {parts})), 16))::bit(64)::bigint"


def ch_fingerprint(spec: TableSpec) -> str:
    """The same 8 bytes: MD5 gives them in order, reinterpret reads little-endian, so reverse."""
    parts = ", ".join(_CH_CANON[c.kind].format(c=c.name) for c in spec.columns)
    md5 = f"MD5(concatWithSeparator('\\x1f', {parts}))"
    return f"reinterpretAsInt64(reverse(substring({md5}, 1, 8)))"


def _pg_key(spec: TableSpec) -> str:
    return " || ':' || ".join(f"{k}::text" for k in spec.key)


def _ch_key(spec: TableSpec) -> str:
    if len(spec.key) == 1:
        return f"toString({spec.key[0]})"
    return "concat(" + ", ':', ".join(f"toString({k})" for k in spec.key) + ")"


def _ch_ts(ts: datetime) -> str:
    """A validated UTC timestamp literal for ClickHouse."""
    return f"toDateTime64('{ts.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f}', 6, 'UTC')"


def _ch_where(spec: TableSpec, cutoff: datetime) -> str:
    return f"cut < {_ch_ts(cutoff)}" if spec.ch_cut else "1"


def _ints(values) -> str:
    return ", ".join(str(int(v)) for v in values)


# --- steps -------------------------------------------------------------------------------------

def postgres_cutoff(pg: PgQuery) -> datetime:
    """T by the Postgres clock (the laptop), whole seconds: same result for ms and µs columns."""
    ((cutoff,),) = pg("SELECT date_trunc('second', now()) - %(lag)s", {"lag": CUTOFF_LAG})
    return cutoff


def bucket_aggregates(spec: TableSpec, pg: PgQuery, ch: ChQuery, cutoff: datetime):
    pg_rows = pg(
        f"SELECT {_bucket(spec, pg=True)} AS b, count(*), sum({pg_fingerprint(spec)})::text"
        f" FROM ({spec.pg_source}) AS s WHERE cut < %(cutoff)s GROUP BY b",
        {"cutoff": cutoff},
    )
    ch_rows = ch(
        f"SELECT {_bucket(spec)} AS b, count(), toString(sum(toInt128({ch_fingerprint(spec)})))"
        f" FROM ({spec.ch_source}) WHERE {_ch_where(spec, cutoff)} GROUP BY b"
    )
    to_map = lambda rows: {int(b): (int(n), int(s)) for b, n, s in rows}  # noqa: E731
    return to_map(pg_rows), to_map(ch_rows)


@dataclass
class TableResult:
    table: str
    cutoff: datetime
    pg_rows: int = 0
    ch_rows: int = 0
    buckets_differ: int = 0
    in_flight: int = 0
    # Keys of each kind, at most KEEP_KEYS; *_total are the exact counts.
    missing_in_ch: list[str] = field(default_factory=list)
    different: list[str] = field(default_factory=list)
    # Candidates only: violations after recheck_extra().
    extra_in_ch: list[str] = field(default_factory=list)
    missing_total: int = 0
    different_total: int = 0
    extra_total: int = 0

    @property
    def violations(self) -> int:
        return self.missing_total + self.different_total + self.extra_total

    def add(self, kind: str, key: str) -> None:
        keys = getattr(self, kind)
        if len(keys) < KEEP_KEYS:
            keys.append(key)
        total = {"missing_in_ch": "missing_total", "different": "different_total",
                 "extra_in_ch": "extra_total"}[kind]
        setattr(self, total, getattr(self, total) + 1)

    def details(self) -> dict:
        details = {
            "buckets_differ": self.buckets_differ,
            "in_flight": self.in_flight,
            "missing_in_ch": self.missing_in_ch[:SAMPLE],
            "different": self.different[:SAMPLE],
            "extra_in_ch": self.extra_in_ch[:SAMPLE],
        }
        if self.missing_total:
            # Quarantine is legal absence (ADR-0008): the event is in raw_events.
            details["hint"] = ("missing keys may be quarantined: look them up in raw_events,"
                               " reload with spark-jobs/backfill_from_raw.py")
        return details


def reconcile_table(spec: TableSpec, pg: PgQuery, ch: ChQuery, cutoff: datetime) -> TableResult:
    pg_agg, ch_agg = bucket_aggregates(spec, pg, ch, cutoff)
    result = TableResult(
        table=spec.name,
        cutoff=cutoff,
        pg_rows=sum(n for n, _ in pg_agg.values()),
        ch_rows=sum(n for n, _ in ch_agg.values()),
    )
    differ = sorted(b for b in set(pg_agg) | set(ch_agg) if pg_agg.get(b) != ch_agg.get(b))
    result.buckets_differ = len(differ)
    if not differ:
        return result

    for start in range(0, len(differ), ARBITRATION_BATCH):
        _arbitrate(spec, pg, ch, cutoff, differ[start:start + ARBITRATION_BATCH], result)
    return result


def _arbitrate(spec: TableSpec, pg: PgQuery, ch: ChQuery, cutoff: datetime,
               buckets: list[int], result: TableResult) -> None:
    """Judge every key of these buckets; only counts and capped key lists are kept."""
    # ClickHouse first, Postgres second: a key that changes in between shows up as hot in
    # Postgres and is skipped, never judged against a newer ClickHouse row.
    ch_keys = {
        key: int(h)
        for key, h in ch(
            f"SELECT {_ch_key(spec)}, toString({ch_fingerprint(spec)}) FROM ({spec.ch_source})"
            f" WHERE {_ch_where(spec, cutoff)} AND {_bucket(spec)} IN ({_ints(buckets)})"
        )
    }
    pg_keys = {
        key: (int(h), bool(hot))
        for key, h, hot in pg(
            f"SELECT {_pg_key(spec)}, ({pg_fingerprint(spec)})::text, cut >= %(cutoff)s"
            f" FROM ({spec.pg_source}) AS s WHERE {_bucket(spec, pg=True)} = ANY(%(buckets)s)",
            {"cutoff": cutoff, "buckets": buckets},
        )
    }
    for key in sorted(set(ch_keys) | set(pg_keys), key=_key_order):
        in_pg, in_ch = key in pg_keys, key in ch_keys
        if in_pg and pg_keys[key][1]:
            result.in_flight += 1  # changed in Postgres at or after T
        elif in_pg and not in_ch:
            result.add("missing_in_ch", key)
        elif in_pg and pg_keys[key][0] != ch_keys[key]:
            result.add("different", key)
        elif in_ch and not in_pg:
            result.add("extra_in_ch", key)  # deleted in Postgres: the tombstone may be on its way


def recheck_extra(spec: TableSpec, keys: list[str], pg: PgQuery, ch: ChQuery) -> list[str]:
    """Keys still live in ClickHouse and still absent from Postgres, some minutes later."""
    if not keys:
        return []
    literals = ", ".join("'" + k.replace("\\", "\\\\").replace("'", "\\'") + "'" for k in keys)
    still_in_ch = {row[0] for row in ch(
        f"SELECT {_ch_key(spec)} AS k FROM ({spec.ch_source}) WHERE k IN ({literals})"
    )}
    back_in_pg = {row[0] for row in pg(
        f"SELECT {_pg_key(spec)} FROM ({spec.pg_source}) AS s"
        f" WHERE {_pg_key(spec)} = ANY(%(keys)s)",
        {"keys": list(keys)},
    )}
    return [k for k in keys if k in still_in_ch and k not in back_in_pg]


LAG_ALLOWED = timedelta(minutes=5)  # NFR-3


def pipeline_lagging(pg: PgQuery, ch: ChQuery, cutoff: datetime) -> bool:
    """True when Postgres has changes in [T, now - 5 min] but ClickHouse has no event since T:
    the stream is stopped or catching up (NFR-3), so every key would look like a mismatch.
    Reported as 'lagging' instead of reconciling. Changes younger than 5 min may still be on
    their way and prove nothing."""
    ((settled,),) = pg(
        "SELECT EXISTS (SELECT FROM orders WHERE updated_at >= %(t)s AND updated_at < %(s)s)"
        " OR EXISTS (SELECT FROM inventory WHERE updated_at >= %(t)s AND updated_at < %(s)s)"
        " OR EXISTS (SELECT FROM customers WHERE updated_at >= %(t)s AND updated_at < %(s)s)"
        " OR EXISTS (SELECT FROM products WHERE updated_at >= %(t)s AND updated_at < %(s)s)",
        {"t": cutoff, "s": cutoff + CUTOFF_LAG - LAG_ALLOWED},
    )
    if not settled:
        return False
    # event_time is the Postgres commit time; the bounds keep the scan to recent partitions.
    ((ch_recent,),) = ch(
        "SELECT count() FROM shopflow.raw_events"
        f" WHERE event_time >= {_ch_ts(cutoff)} AND event_time < now64(3) + INTERVAL 1 DAY"
    )
    return int(ch_recent) == 0


def _key_order(key: str):
    return tuple(int(part) for part in key.split(":"))
