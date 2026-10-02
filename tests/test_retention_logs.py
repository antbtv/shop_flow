"""Airflow log cleanup (airflow/dags/shopflow_checks/retention.py, NFR-5)."""

import os
import time

from shopflow_checks.retention import clean_logs

DAY = 86400


def touch(path, age_days, size=10):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    mtime = time.time() - age_days * DAY
    os.utime(path, (mtime, mtime))


def test_old_files_and_emptied_dirs_go_fresh_ones_stay(tmp_path):
    touch(tmp_path / "dag_id=a/run_id=old/task_id=t/attempt=1.log", 40, size=100)
    touch(tmp_path / "dag_id=a/run_id=new/task_id=t/attempt=1.log", 2)
    touch(tmp_path / "dag_processor/2026-09-01/a.py.log", 31, size=50)
    touch(tmp_path / "dag_processor/latest.log", 0)

    report = clean_logs(tmp_path, older_than_days=30)

    assert report == {"files_removed": 2, "bytes_freed": 150, "dirs_removed": 3}
    left = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.is_file())
    assert left == ["dag_id=a/run_id=new/task_id=t/attempt=1.log", "dag_processor/latest.log"]
    assert tmp_path.exists()  # the root itself is never removed


def test_symlinks_are_not_followed_or_removed(tmp_path):
    outside = tmp_path.parent / f"{tmp_path.name}-outside.log"
    touch(outside, 90)
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "link.log").symlink_to(outside)
    assert clean_logs(logs, older_than_days=30)["files_removed"] == 0
    assert outside.exists()
