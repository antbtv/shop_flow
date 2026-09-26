"""Milestone 0.9: parallel tasks to measure LocalExecutor fork overhead on Pi5."""

import time
from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["memtest"])
def memtest():
    @task
    def hold(seconds: int) -> int:
        payload = bytearray(50 * 1024 * 1024)  # 50 MB per task, like a small extract
        time.sleep(seconds)
        return len(payload)

    hold.expand(seconds=[90, 90, 90, 90])


memtest()
