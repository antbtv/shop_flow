"""Task callbacks (ADR-0010). FR-11 (Telegram) plugs into on_failure in M5, DAGs stay as is."""

import logging

log = logging.getLogger(__name__)


def on_failure(context) -> None:
    """A failed task means "a human is needed": mismatch, lagging, error, two days unreachable."""
    ti = context["ti"]
    log.error(
        "ShopFlow check failed: dag=%s task=%s run=%s try=%s error=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
        context.get("exception"),
    )


DEFAULT_ARGS = {
    "owner": "shopflow",
    "retries": 0,
    "on_failure_callback": on_failure,
}
