"""Tests for airflow/dags/shopflow_common/errors.py: "laptop off" vs "fix the config" (ADR-0010)."""

import pytest
from shopflow_common.errors import is_source_unreachable

# Messages as libpq prints them (psycopg2.OperationalError, no pgcode).
UNREACHABLE = [
    'connection to server at "192.168.0.105", port 5432 failed: Connection refused\n'
    "\tIs the server running on that host and accepting TCP/IP connections?",
    'connection to server at "192.168.0.105", port 5432 failed: timeout expired',
    'connection to server at "192.168.0.105", port 5432 failed: No route to host',
    "could not connect to server: Network is unreachable",
    "server closed the connection unexpectedly\n"
    "\tThis probably means the server terminated abnormally",
    'could not translate host name "laptop" to address: Name or service not known',
]


@pytest.mark.parametrize("message", UNREACHABLE)
def test_unreachable_server_is_source_unavailable(message):
    assert is_source_unreachable(None, message)


@pytest.mark.parametrize(
    ("pgcode", "message"),
    [
        # pg_hba: Pi5 address changed or the entry is missing.
        ("28000", 'pg_hba.conf rejects connection for host "192.168.0.151", user "recon_reader"'),
        ("28P01", 'password authentication failed for user "recon_reader"'),
        ("53300", 'too many connections for role "recon_reader"'),
        # A server-side error whose text happens to contain an "unreachable" marker.
        ("57014", "canceling statement due to statement timeout (timed out)"),
    ],
)
def test_server_answer_is_an_error_not_unavailability(pgcode, message):
    assert not is_source_unreachable(pgcode, message)


def test_unknown_message_without_code_is_an_error():
    # Only known "no server" messages wait; anything else must fail and be looked at.
    assert not is_source_unreachable(None, "SSL error: certificate verify failed")
