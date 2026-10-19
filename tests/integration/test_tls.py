"""TLS on the replication connection, against the compose server (which runs
with the certificate its image ships for "localhost")."""

from __future__ import annotations

import ssl
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from deltaflow.aio import run
from deltaflow.replication.changes import RowInsert, Transaction
from deltaflow.replication.slot import create_publication, create_slot
from deltaflow.replication.stream import ReplicationStream
from deltaflow.replication.wire import ReplicationConnection

SERVER_CERT = "/etc/ssl/certs/ssl-cert-snakeoil.pem"


@pytest.fixture(scope="module")
def server_cert(dsn: str, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The server's self-signed certificate, which is its own root."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute("SELECT pg_read_file(%s)", (SERVER_CERT,)).fetchone()
    assert row is not None
    path = tmp_path_factory.mktemp("tls") / "root.crt"
    path.write_text(str(row[0]), encoding="ascii")
    return path


def _with(dsn: str, **settings: Any) -> str:
    return make_conninfo(dsn, **settings)


async def _server_view(dsn: str, pid: int | None) -> tuple[Any, ...] | None:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cur = await conn.execute("SELECT ssl, version FROM pg_stat_ssl WHERE pid = %s", (pid,))
        return await cur.fetchone()


def test_the_default_sslmode_encrypts_the_replication_connection(dsn: str) -> None:
    """prefer, like libpq: the server offers TLS, so it is used."""

    async def go() -> tuple[bool, Any]:
        repl = await ReplicationConnection.connect(dsn)
        try:
            return repl.tls, await _server_view(dsn, repl.backend_pid)
        finally:
            await repl.close()

    client_side, server_side = run(go())
    assert client_side is True
    assert server_side is not None
    assert server_side[0] is True


def test_sslmode_disable_is_plaintext(dsn: str) -> None:
    async def go() -> tuple[bool, Any]:
        repl = await ReplicationConnection.connect(_with(dsn, sslmode="disable"))
        try:
            return repl.tls, await _server_view(dsn, repl.backend_pid)
        finally:
            await repl.close()

    client_side, server_side = run(go())
    assert client_side is False
    assert server_side is not None
    assert server_side[0] is False


def test_changes_stream_over_verify_full(dsn: str, server_cert: Path) -> None:
    """The whole path, encrypted and authenticated: a transaction committed
    on the source arrives decoded over a verify-full connection."""
    table = f"tls_{uuid.uuid4().hex[:8]}"
    secure = _with(dsn, sslmode="verify-full", sslrootcert=str(server_cert))

    async def go() -> Transaction:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(f"CREATE TABLE {table} (id int PRIMARY KEY, note text)")
            try:
                await create_publication(sql, f"pub_{table}", (f"public.{table}",))
                repl = await ReplicationConnection.connect(secure)
                assert repl.tls
                info = await create_slot(repl, f"slot_{table}", temporary=True)
                stream = ReplicationStream(
                    repl, slot=f"slot_{table}", publications=[f"pub_{table}"],
                    start_lsn=info.consistent_point,
                )  # fmt: skip
                await stream.start()
                try:
                    await sql.execute(f"INSERT INTO {table} VALUES (1, 'over TLS')")
                    async for event in stream:
                        if isinstance(event, Transaction):
                            return event
                    raise AssertionError("stream ended")
                finally:
                    await stream.close()
            finally:
                await sql.execute(f"DROP PUBLICATION IF EXISTS pub_{table}")
                await sql.execute(f"DROP TABLE {table}")

    txn = run(go())
    (change,) = txn.changes
    assert isinstance(change, RowInsert)
    assert change.new["note"] == "over TLS"


def test_verify_full_refuses_a_host_name_the_certificate_does_not_name(
    dsn: str, server_cert: Path
) -> None:
    """The certificate names "localhost"; 127.0.0.1 is the same server, but
    verify-full checks the name, so the connection is refused."""
    by_ip = _with(dsn, host="127.0.0.1", sslmode="verify-full", sslrootcert=str(server_cert))
    with pytest.raises(ssl.SSLCertVerificationError, match="IP address mismatch"):
        run(ReplicationConnection.connect(by_ip))


def test_verify_ca_checks_the_chain_only(dsn: str, server_cert: Path) -> None:
    by_ip = _with(dsn, host="127.0.0.1", sslmode="verify-ca", sslrootcert=str(server_cert))

    async def go() -> bool:
        repl = await ReplicationConnection.connect(by_ip)
        try:
            return repl.tls
        finally:
            await repl.close()

    assert run(go()) is True


def test_the_system_roots_do_not_trust_a_self_signed_server(dsn: str) -> None:
    with pytest.raises(ssl.SSLCertVerificationError, match="self-signed"):
        run(ReplicationConnection.connect(_with(dsn, sslrootcert="system")))


def test_psycopg_and_the_replication_connection_agree(dsn: str, server_cert: Path) -> None:
    """The same settings give libpq (psycopg) and the replication
    connection the same outcome, so a worker never runs half encrypted."""
    secure = _with(dsn, sslmode="verify-full", sslrootcert=str(server_cert))
    host = conninfo_to_dict(dsn).get("host")
    assert host == "localhost", "the certificate names localhost"
    with psycopg.connect(secure) as conn:
        row = conn.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()").fetchone()
    assert row == (True,)
    by_ip = _with(secure, host="127.0.0.1")
    with pytest.raises(psycopg.OperationalError, match="does not match host name"):
        psycopg.connect(by_ip)
    with pytest.raises(ssl.SSLCertVerificationError):
        run(ReplicationConnection.connect(by_ip))
