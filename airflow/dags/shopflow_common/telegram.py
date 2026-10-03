"""Telegram sender for the FR-11 alerts (ADR-0011): standard library only, never raises.

TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID live only in the environment of the Airflow scheduler
(the tasks, and with them the failure callbacks, run there). Without them an alert is skipped
and logged: a missing token must not turn a failed check into a second failure.

The token is part of the request URL, so it must not leak: errors are re-raised without the
original exception (`from None`: urllib errors carry the URL), the log gets only the exception
class and the HTTP status, never a message, URL or response body, and the token is registered
with the Airflow secrets masker when that is available.
"""

from __future__ import annotations

import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)

API_URL = "https://api.telegram.org"
# Telegram refuses a message over 4096 characters; keep room for the cut marker.
MAX_TEXT = 3800
TIMEOUT_S = 10.0
RETRY_PAUSE_S = 2.0


class TelegramError(Exception):
    """A failed request. status is the HTTP code or None for a network error; no URL, no body."""

    def __init__(self, kind: str, status: int | None = None):
        super().__init__(f"{kind} (HTTP {status})" if status else kind)
        self.kind = kind
        self.status = status

    @property
    def retryable(self) -> bool:
        # Network trouble, Telegram's own 5xx and its 429 flood limit pass with time; a wrong
        # token or chat (401, 400, 403, 404) does not.
        return self.status is None or self.status >= 500 or self.status == 429


def credentials() -> tuple[str, str] | None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    return (token, chat_id) if token and chat_id else None


def _mask(token: str) -> None:
    """Register the token with the Airflow masker so a leak into a task log shows as ***."""
    try:
        from airflow.sdk.log import mask_secret
    except Exception:  # noqa: BLE001 - no Airflow (tests) or another layout: masking is a bonus
        return
    try:
        mask_secret(token)
    except Exception:  # noqa: BLE001
        log.debug("could not register the Telegram token with the masker")


def _post(url: str, chat_id: str, text: str, timeout: float) -> None:
    body = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
    ).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=timeout):
            return
    except urllib.error.HTTPError as exc:
        status = exc.code
    except Exception as exc:  # noqa: BLE001 - URLError, timeout, reset: all "network"
        kind = type(exc).__name__
        raise TelegramError(kind) from None
    raise TelegramError("HTTPError", status) from None


def send_message(text: str, *, timeout: float = TIMEOUT_S, retries: int = 1,
                 pause: float = RETRY_PAUSE_S) -> bool:
    """Send one plain-text message. True if Telegram accepted it, False otherwise; no exception.

    One retry on a network error, 5xx or 429. The upper bound of the call is about
    (retries + 1) * timeout + pause plus name resolution, which `timeout` does not cover.
    """
    found = credentials()
    if found is None:
        log.warning("Telegram alert skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set")
        return False
    token, chat_id = found
    _mask(token)
    if len(text) > MAX_TEXT:
        text = text[:MAX_TEXT] + "\n…"
    base = os.environ.get("TELEGRAM_API_URL", API_URL).rstrip("/")
    url = f"{base}/bot{token}/sendMessage"
    for attempt in range(retries + 1):
        try:
            _post(url, chat_id, text, timeout)
            return True
        except TelegramError as exc:
            if attempt < retries and exc.retryable:
                log.warning("Telegram send failed (%s), retrying", exc)
                time.sleep(pause)
                continue
            log.error("Telegram alert not delivered: %s", exc)
            return False
    return False
