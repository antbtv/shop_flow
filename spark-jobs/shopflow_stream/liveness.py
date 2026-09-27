"""Liveness of the streaming query (ADR-0008).

A batch stuck on a dead socket raises nothing: without a watchdog the driver stays up, Docker
restarts nothing, and the lag grows silently. The listener records every sign of life
(progress, or idle when Kafka has no new data) and touches a marker file for the Docker
healthcheck. The watchdog kills the process when there is no sign of life for too long, so
`restart: unless-stopped` brings the query back from the checkpoint.
"""

from __future__ import annotations

import logging
import os
import pathlib
import threading
import time

from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("shopflow.liveness")

MARKER = pathlib.Path("/tmp/shopflow-alive")


class Heartbeat:
    """Monotonic time of the last sign of life, shared by the listener and the watchdog."""

    def __init__(self, marker: pathlib.Path | None = MARKER) -> None:
        self._lock = threading.Lock()
        self._last = time.monotonic()
        self._marker = marker

    def beat(self) -> None:
        with self._lock:
            self._last = time.monotonic()
        if self._marker is not None:
            self._marker.touch()

    def silence_seconds(self) -> float:
        with self._lock:
            return time.monotonic() - self._last


class ProgressListener(StreamingQueryListener):
    """Logs one line per batch and feeds the heartbeat."""

    def __init__(self, heartbeat: Heartbeat) -> None:
        self._heartbeat = heartbeat

    def onQueryStarted(self, event) -> None:
        log.info("query started id=%s run=%s", event.id, event.runId)
        self._heartbeat.beat()

    def onQueryProgress(self, event) -> None:
        p = event.progress
        durations = p.durationMs or {}
        log.info(
            "batch=%s rows=%s rows/s=%.1f total_ms=%s add_batch_ms=%s",
            p.batchId,
            p.numInputRows,
            p.processedRowsPerSecond or 0.0,
            durations.get("triggerExecution"),
            durations.get("addBatch"),
        )
        self._heartbeat.beat()

    def onQueryIdle(self, event) -> None:
        self._heartbeat.beat()

    def onQueryTerminated(self, event) -> None:
        log.warning("query terminated id=%s exception=%s", event.id, event.exception)


def start_watchdog(heartbeat: Heartbeat, timeout_s: float, check_every_s: float = 30.0) -> None:
    """Exit the process with code 1 when the query shows no sign of life for timeout_s.

    os._exit: sys.exit in a thread ends only that thread, and the main thread may be blocked
    in a JVM call forever.
    """

    def run() -> None:
        while True:
            time.sleep(check_every_s)
            silence = heartbeat.silence_seconds()
            if silence > timeout_s:
                log.error("no progress for %.0f s (limit %.0f s): exiting", silence, timeout_s)
                logging.shutdown()
                os._exit(1)

    threading.Thread(target=run, name="shopflow-watchdog", daemon=True).start()
