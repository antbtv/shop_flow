"""Is the alert channel reachable from Pi5? (ADR-0012, FR-11)

Telegram is blocked in the Pi5 network and reached through a tunnel on the host. When the tunnel
is down an alert fails with only an ERROR in the task log, and every DAG still succeeds, so
nothing on the dashboard turns red. A scheduled probe makes that failure visible.

The probe sends no token: a plain GET of the Bot API base URL. Any HTTP answer (even 404 or 302)
means the network path works; a timeout, a refused or unreachable connection means it does not.
Only the exception class is kept, as in shopflow_common.telegram: a message may carry a URL.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from shopflow_checks.outcome import Row

CHECK = "telegram_reachable"
DEFAULT_URL = "https://api.telegram.org"
TIMEOUT_S = 10


def api_url() -> str:
    return os.environ.get("TELEGRAM_API_URL", DEFAULT_URL).rstrip("/") + "/"


def probe(url: str, timeout: float = TIMEOUT_S, opener=urllib.request.urlopen) -> Row:
    """One summary row (table_name '') for dq_check_results: ok, or error with the class."""
    host = urlsplit(url).hostname or "?"
    try:
        with opener(url, timeout=timeout) as response:
            code = response.status
    except urllib.error.HTTPError as exc:  # the server answered: the path works
        code = exc.code
    except Exception as exc:  # noqa: BLE001 - URLError, timeout, reset: all mean "not reachable"
        return Row("", "error", details={"host": host, "error": type(exc).__name__})
    return Row("", "ok", details={"host": host, "http_status": code})
