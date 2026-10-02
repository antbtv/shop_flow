"""Retention checks (NFR-5, ADR-0010): ClickHouse enforces it, the DAG verifies and reports.

raw_events keeps 30 days through TTL with ttl_only_drop_parts: a part is dropped only when all
its rows expired, and parts never span days (PARTITION BY day), so a day disappears about
30 + 1 days after it started, plus up to merge_with_ttl_timeout (4 h) for the TTL merge. Events
older than 32 days mean TTL is not working. Marts and dimensions are kept forever (NFR-5).
Airflow task logs on the logs volume are cleaned here; the metadata DB is cleaned by a host
timer (infra/pi5/etc/systemd/system/shopflow-airflow-db-clean.*): Airflow 3 blocks the
metadata DB for task code, `airflow db clean` cannot run inside a DAG.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from shopflow_checks.dq import CheckResult

RAW_TABLE = "shopflow.raw_events"
RAW_RETENTION = "INTERVAL 32 DAY"
LOG_RETENTION_DAYS = 30


def raw_ttl_check(ch, table: str = RAW_TABLE, older_than: str = RAW_RETENTION) -> CheckResult:
    """Events past retention plus grace still present: TTL does not drop parts."""
    ((stale, oldest),) = ch(
        f"SELECT count(), toString(min(event_time)) FROM {table}"
        f" WHERE event_time < now64(3) - {older_than}"
    )
    stale = int(stale)
    if not stale:
        return CheckResult(table_name=table.split(".")[-1], status="ok")
    return CheckResult(
        table_name=table.split(".")[-1],
        status="violation",
        violations=stale,
        details={
            "oldest": oldest,
            "hint": "TTL did not drop expired parts: SHOW CREATE TABLE (TTL, ttl_only_drop_parts),"
                    " system.merges, or force ALTER TABLE ... MATERIALIZE TTL",
        },
    )


def table_sizes(ch, database: str = "shopflow") -> list[CheckResult]:
    """Active rows and bytes per table, for the disk trend on the dashboard (M5)."""
    rows = ch(
        "SELECT table, sum(rows), sum(bytes_on_disk) FROM system.parts"
        f" WHERE database = '{database}' AND active GROUP BY table ORDER BY table"
    )
    return [
        CheckResult(table_name=table, status="ok", ch_value=int(n),
                    details={"bytes_on_disk": int(size)})
        for table, n, size in rows
    ]


def clean_logs(root: Path, older_than_days: int = LOG_RETENTION_DAYS,
               now: float | None = None) -> dict:
    """Delete log files not modified for older_than_days, then empty directories. Returns
    counts for the report. Never follows symlinks out of root."""
    cutoff = (time.time() if now is None else now) - older_than_days * 86400
    files = freed = dirs = 0
    for dirpath, _dirnames, filenames in os.walk(root, topdown=False):
        for name in filenames:
            path = Path(dirpath, name)
            if path.is_symlink():
                continue
            stat = path.stat()
            if stat.st_mtime < cutoff:
                path.unlink()
                files += 1
                freed += stat.st_size
        here = Path(dirpath)
        if here != root and not any(here.iterdir()):
            here.rmdir()
            dirs += 1
    return {"files_removed": files, "bytes_freed": freed, "dirs_removed": dirs}
