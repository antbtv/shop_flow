"""shopflow_common/telegram.py (FR-11, ADR-0011) against a fake Bot API on localhost.

No network and no Airflow: the server is a thread, the token is a made-up string. What matters:
the message goes where Telegram expects it, a broken Telegram never raises, retries happen only
for errors that can pass, and the token appears in no log line and no exception.
"""

import logging
import sys
import threading
import time
import traceback
import types
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from shopflow_common import telegram

TOKEN = "123456:ABC-very-secret-token"
CHAT = "-100777"


class FakeBotApi:
    def __init__(self):
        self.requests = []  # (path, form fields)
        self.script = []  # status codes to answer with, last one repeats
        self.delay = 0.0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                size = int(self.headers.get("Content-Length", 0))
                form = urllib.parse.parse_qs(self.rfile.read(size).decode())
                owner.requests.append((self.path, {k: v[0] for k, v in form.items()}))
                time.sleep(owner.delay)
                code = owner.script[min(len(owner.requests), len(owner.script)) - 1]
                # A hostile body: some proxies echo the request line. It must never reach a log.
                body = f"error for {self.path}".encode()
                try:
                    self.send_response(code)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except OSError:
                    pass

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}"


@pytest.fixture
def api(monkeypatch):
    fake = FakeBotApi()
    fake.script = [200]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", CHAT)
    monkeypatch.setenv("TELEGRAM_API_URL", fake.url)
    yield fake
    fake.server.shutdown()


def send(text="alert", **kwargs):
    return telegram.send_message(text, pause=0, **kwargs)


def test_message_goes_to_the_bot_with_chat_and_plain_text(api):
    assert send("ShopFlow: сбой") is True
    (path, form), = api.requests
    assert path == f"/bot{TOKEN}/sendMessage"
    assert form["chat_id"] == CHAT and form["text"] == "ShopFlow: сбой"
    assert "parse_mode" not in form  # plain text: nothing to escape in keys and names


@pytest.mark.parametrize("missing", ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"])
def test_without_credentials_nothing_is_sent_and_nothing_raised(api, monkeypatch, missing, caplog):
    monkeypatch.delenv(missing)
    with caplog.at_level(logging.WARNING):
        assert send() is False
    assert api.requests == []
    assert "not set" in caplog.text


def test_server_error_is_retried_once_then_given_up(api):
    api.script = [500, 500, 200]
    assert send() is False
    assert len(api.requests) == 2


def test_a_second_attempt_can_succeed(api):
    api.script = [502, 200]
    assert send() is True
    assert len(api.requests) == 2


def test_flood_limit_is_retried(api):
    api.script = [429, 200]
    assert send() is True


@pytest.mark.parametrize("code", [400, 401, 403, 404])
def test_a_wrong_token_or_chat_is_not_retried(api, code):
    api.script = [code]
    assert send() is False
    assert len(api.requests) == 1


def test_unreachable_telegram_does_not_raise(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", CHAT)
    monkeypatch.setenv("TELEGRAM_API_URL", "http://127.0.0.1:9")  # nobody listens
    assert send(timeout=1) is False


def test_a_slow_telegram_is_cut_at_the_timeout(api):
    api.delay = 1.0
    started = time.monotonic()
    assert send(timeout=0.2, retries=0) is False
    assert time.monotonic() - started < 0.9


def test_long_text_is_cut_to_the_limit(api):
    assert send("x" * 10_000) is True
    sent = api.requests[0][1]["text"]
    assert len(sent) <= telegram.MAX_TEXT + 2
    assert sent.endswith("…")


@pytest.mark.parametrize("code", [401, 404, 500])
def test_the_token_reaches_no_log_line_and_no_error(api, caplog, code):
    api.script = [code]
    with caplog.at_level(logging.DEBUG):
        send()
    assert TOKEN not in caplog.text
    # The error itself, as the traceback machinery prints it (cause and context included).
    with pytest.raises(telegram.TelegramError) as caught:
        telegram._post(f"{api.url}/bot{TOKEN}/sendMessage", CHAT, "x", 2.0)
    exc = caught.value
    printed = "".join(traceback.format_exception(exc))
    assert TOKEN not in printed and TOKEN not in str(exc) and TOKEN not in repr(exc)
    assert exc.__cause__ is None and exc.__suppress_context__


def test_the_token_is_registered_with_the_airflow_masker(api, monkeypatch):
    seen = []
    fake = types.ModuleType("airflow.sdk.log")
    fake.mask_secret = seen.append
    monkeypatch.setitem(sys.modules, "airflow.sdk.log", fake)
    assert send() is True
    assert seen == [TOKEN]


def test_a_failing_masker_does_not_stop_the_alert(api, monkeypatch):
    def boom(secret):
        raise RuntimeError("masker broke")

    fake = types.ModuleType("airflow.sdk.log")
    fake.mask_secret = boom
    monkeypatch.setitem(sys.modules, "airflow.sdk.log", fake)
    assert send() is True
