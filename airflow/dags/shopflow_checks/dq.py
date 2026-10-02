"""Data quality checks of FR-9 (ADR-0010): one SQL file per check in airflow/dags/sql/dq/.

A check file is one ClickHouse SELECT returning a single row (violations, sample_keys). Its
header comments say which table it is about (`-- table:`) and what to do on a hit (`-- hint:`,
may span several lines). Checks read ClickHouse only, so the DAG runs with the laptop off.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

CHECKS_DIR = Path(__file__).resolve().parents[1] / "sql" / "dq"


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    hint: str
    sql: str


@dataclass
class CheckResult:
    """One dq_check_results row without the run columns (see shopflow_common.results)."""

    table_name: str
    status: str
    violations: int = 0
    pg_value: int | None = None
    ch_value: int | None = None
    details: dict = field(default_factory=dict)


def load_checks(directory: Path = CHECKS_DIR) -> list[Check]:
    checks = []
    for path in sorted(directory.glob("*.sql")):
        sql = path.read_text()
        meta: dict[str, list[str]] = {"table": [], "hint": []}
        for line in sql.splitlines():
            if not line.startswith("--"):
                break
            key, _, value = line.removeprefix("--").strip().partition(":")
            if key in meta:
                meta[key].append(value.strip())
        if not meta["table"] or not meta["hint"]:
            raise ValueError(f"{path.name}: header needs '-- table:' and '-- hint:'")
        checks.append(Check(
            name=path.stem.split("_", 1)[1],
            table=meta["table"][0],
            hint=" ".join(meta["hint"]),
            sql=sql,
        ))
    return checks


def run_check(check: Check, ch) -> CheckResult:
    """ch(sql) -> rows. A hit carries the sample keys and the hint for whoever reads the row."""
    ((violations, sample),) = ch(check.sql)
    violations = int(violations)
    details = {"sample": list(sample), "hint": check.hint} if violations else {}
    return CheckResult(
        table_name=check.table,
        status="violation" if violations else "ok",
        violations=violations,
        details=details,
    )
