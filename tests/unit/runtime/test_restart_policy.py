"""Which failures a durable runner restarts after, and which it raises.

Restarting is safe for any failure (it resumes from the last checkpoint:
the M5 argument), but only worth it when waiting can fix the cause."""

from __future__ import annotations

import asyncio
import ssl

import psycopg
import pytest

from deltaflow.planner import PlanError
from deltaflow.replication.conninfo import ConnInfoError
from deltaflow.replication.stream import ReplicationEndedError
from deltaflow.replication.wire import PgError, TlsUnavailableError, WireProtocolError
from deltaflow.runtime.durable import is_transient
from deltaflow.sinks import SinkError
from deltaflow.sinks.postgres import FencedError


def pg(sqlstate: str) -> PgError:
    return PgError({"S": "FATAL", "C": sqlstate, "M": "test"})


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionResetError("reset by peer"),
        ConnectionRefusedError("server restarting"),
        TimeoutError(),
        asyncio.IncompleteReadError(b"", 5),  # the server closed mid-message
        ReplicationEndedError("server ended replication"),
        psycopg.OperationalError("server closed the connection unexpectedly"),
        pg("57P01"),  # terminating connection due to administrator command
        pg("57P03"),  # the database system is starting up
        pg("08006"),  # connection failure
        pg("53300"),  # too many connections
        pg("55006"),  # replication slot is active for another process
        ssl.SSLEOFError("EOF occurred in violation of protocol"),
    ],
)
def test_failures_that_waiting_can_fix_are_transient(exc: BaseException) -> None:
    assert is_transient(exc)


@pytest.mark.parametrize(
    "exc",
    [
        pg("28P01"),  # password authentication failed
        pg("42501"),  # insufficient privilege
        pg("42704"),  # replication slot does not exist
        ssl.SSLCertVerificationError("certificate verify failed"),
        TlsUnavailableError("server does not accept TLS"),
        ConnInfoError("several hosts"),
        WireProtocolError("unexpected message"),
        SinkError("drift between the view and the table"),
        FencedError("the shard moved"),
        PlanError("views changed"),
        AssertionError("a bug"),
        ValueError("a bug"),
    ],
)
def test_misconfiguration_drift_and_bugs_are_not(exc: BaseException) -> None:
    assert not is_transient(exc)


def test_psycopg_errors_are_judged_by_their_sqlstate() -> None:
    class AuthFailed(psycopg.OperationalError):
        sqlstate = "28P01"

    assert not is_transient(AuthFailed("password authentication failed"))
