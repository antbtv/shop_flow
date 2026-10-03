"""Probe of the alert channel (airflow/dags/shopflow_checks/channel.py, ADR-0012).

A local HTTP server plays the Bot API: any answer is "reachable", silence and refusals are not.
"""

import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from shopflow_checks.channel import CHECK, api_url, probe


class Handler(BaseHTTPRequestHandler):
    code = 200

    def do_GET(self):
        self.send_response(type(self).code)
        self.end_headers()
        self.wfile.write(b"x")

    def log_message(self, *args):
        pass


@pytest.fixture
def server():
    def start(code):
        handler = type("H", (Handler,), {"code": code})
        httpd = HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        servers.append(httpd)
        return f"http://127.0.0.1:{httpd.server_port}/"

    servers = []
    yield start
    for httpd in servers:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.parametrize("code", [200, 302, 401, 404, 500])
def test_any_http_answer_means_reachable(server, code):
    row = probe(server(code), timeout=3)
    assert (row.table_name, row.status) == ("", "ok")
    assert row.details == {"host": "127.0.0.1", "http_status": code}


def test_refused_connection_is_an_error_with_only_the_class_name():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]  # closed again: nothing listens
    row = probe(f"http://127.0.0.1:{port}/secret-path?token=abc", timeout=3)
    assert row.status == "error"
    assert row.details == {"host": "127.0.0.1", "error": "URLError"}
    assert "secret-path" not in str(row.details) and "abc" not in str(row.details)


def test_silent_server_is_a_timeout_error():
    with socket.socket() as sock:  # accepts the connection (backlog), never answers
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        started = time.monotonic()
        row = probe(f"http://127.0.0.1:{sock.getsockname()[1]}/", timeout=0.5)
    assert row.status == "error"
    assert row.details["error"] in ("TimeoutError", "URLError")
    assert time.monotonic() - started < 5


def test_unresolvable_name_is_an_error():
    row = probe("http://no-such-host.invalid/", timeout=3)
    assert row.status == "error" and row.details["host"] == "no-such-host.invalid"


def test_api_url_comes_from_the_environment(monkeypatch):
    monkeypatch.delenv("TELEGRAM_API_URL", raising=False)
    assert api_url() == "https://api.telegram.org/"
    monkeypatch.setenv("TELEGRAM_API_URL", "http://fake-tg:8081")
    assert api_url() == "http://fake-tg:8081/"


def test_check_name_is_the_one_the_dashboard_reads():
    assert CHECK == "telegram_reachable"
