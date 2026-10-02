"""Failure classes of the check DAGs (ADR-0010).

"The laptop is off" is not a data problem: the reconciliation waits for Postgres and reports
source_unavailable instead of failing. Only a failure to reach the server counts as that. A
server that answers with an error (pg_hba, password, connection limit) is a configuration
problem and must fail loudly, or it would hide behind "laptop off" for days.
No Airflow or psycopg2 imports here: the module is unit-tested on the laptop.
"""

# libpq messages for "no server to talk to": refused, timeout, no route, dropped mid-query.
_UNREACHABLE_MARKERS = (
    "connection refused",
    "timeout expired",
    "timed out",
    "no route to host",
    "network is unreachable",
    "could not connect to server",
    "server closed the connection unexpectedly",
    "could not translate host name",
    "connection reset by peer",
)


class SourceUnavailable(Exception):
    """Postgres on the laptop cannot be reached: the laptop is off, asleep or away."""


def is_source_unreachable(pgcode: str | None, message: str) -> bool:
    """True when a Postgres error means the server was not reached at all.

    Any SQLSTATE means the server answered: 28000/28P01 (pg_hba, password), 53300 (too many
    connections), 57014 (statement timeout) are errors to fix, not an absent laptop.
    """
    if pgcode:
        return False
    text = message.lower()
    return any(marker in text for marker in _UNREACHABLE_MARKERS)
