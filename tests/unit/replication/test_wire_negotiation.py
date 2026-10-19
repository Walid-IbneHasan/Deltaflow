"""TLS negotiation and authentication policy of the replication connection,
against a scripted fake server: the cases a real server will not produce
on demand (no TLS support, a broken handshake, injected bytes)."""

from __future__ import annotations

import asyncio
import os
import ssl
import struct
from asyncio import StreamReader, StreamWriter
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

import pytest

from deltaflow.aio import run
from deltaflow.replication.wire import (
    ReplicationConnection,
    TlsUnavailableError,
    WireProtocolError,
)

from .certs import trust_anchor

SSL_REQUEST = struct.pack(">ii", 8, 80877103)
AUTH_OK = b"R" + struct.pack(">ii", 8, 0)
AUTH_CLEARTEXT = b"R" + struct.pack(">ii", 8, 3)
READY = b"Z" + struct.pack(">i", 5) + b"I"


def error(code: str, message: str) -> bytes:
    body = b"SFATAL\0C" + code.encode() + b"\0M" + message.encode() + b"\0\0"
    return b"E" + struct.pack(">i", len(body) + 4) + body


@dataclass
class Seen:
    """What the fake server received, per connection."""

    connections: list[list[str]] = field(default_factory=list)


type Script = Callable[[StreamReader, StreamWriter, list[str]], Coroutine[Any, Any, None]]


async def _first_packet(reader: StreamReader) -> bytes:
    (length,) = struct.unpack(">i", await reader.readexactly(4))
    return struct.pack(">i", length) + await reader.readexactly(length - 4)


async def _serve(script: Script, dsn_query: str) -> tuple[Seen, BaseException | None]:
    seen = Seen()

    async def handle(reader: StreamReader, writer: StreamWriter) -> None:
        log: list[str] = []
        seen.connections.append(log)
        try:
            await script(reader, writer, log)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    outcome: BaseException | None = None
    try:
        conn = await ReplicationConnection.connect(
            f"postgresql://alice:pw@127.0.0.1:{port}/db?{dsn_query}", timeout=5
        )
        await conn.close()
    except Exception as exc:
        outcome = exc
    finally:
        server.close()
        await server.wait_closed()
    return seen, outcome


async def startup(reader: StreamReader, writer: StreamWriter, log: list[str]) -> None:
    """Read a StartupMessage, accept it without a password."""
    packet = await _first_packet(reader)
    log.append("startup" if packet[4:8] == struct.pack(">i", 3 << 16) else f"?{packet!r}")
    writer.write(AUTH_OK + READY)
    await writer.drain()
    await reader.read()  # until the client's Terminate / close


async def no_tls(reader: StreamReader, writer: StreamWriter, log: list[str]) -> None:
    packet = await _first_packet(reader)
    if packet == SSL_REQUEST:
        log.append("sslrequest")
        writer.write(b"N")
        await writer.drain()
        await startup(reader, writer, log)
    else:
        log.append("startup")
        writer.write(AUTH_OK + READY)
        await writer.drain()
        await reader.read()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    for name in list(os.environ):
        if name.startswith("PG"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("APPDATA", str(tmp_path))


def test_disable_sends_no_ssl_request() -> None:
    seen, outcome = run(_serve(no_tls, "sslmode=disable"))
    assert outcome is None
    assert seen.connections == [["startup"]]


def test_prefer_falls_back_to_plaintext_when_the_server_has_no_tls() -> None:
    seen, outcome = run(_serve(no_tls, "sslmode=prefer"))
    assert outcome is None
    assert seen.connections == [["sslrequest", "startup"]]


@pytest.mark.parametrize("mode", ["require", "verify-ca", "verify-full"])
def test_require_refuses_a_server_without_tls(mode: str, tmp_path: Any) -> None:
    root = trust_anchor(tmp_path)
    seen, outcome = run(_serve(no_tls, f"sslmode={mode}&sslrootcert={root}"))
    assert isinstance(outcome, TlsUnavailableError)
    assert mode in str(outcome)
    assert seen.connections == [["sslrequest"]]  # no startup, so no password either


async def broken_tls(reader: StreamReader, writer: StreamWriter, log: list[str]) -> None:
    """Agrees to TLS, then sends bytes that are not a TLS handshake: here a
    plaintext AuthenticationOk + ReadyForQuery, as a man in the middle would
    inject them (CVE-2021-23222). A plaintext retry is served normally."""
    packet = await _first_packet(reader)
    if packet == SSL_REQUEST:
        log.append("sslrequest")
        writer.write(b"S" + AUTH_OK + READY)
        await writer.drain()
        await reader.read()  # the ClientHello, until the client gives up
        return
    log.append("startup")
    writer.write(AUTH_OK + READY)
    await writer.drain()
    await reader.read()


def test_bytes_injected_after_the_tls_answer_are_not_taken_as_the_server() -> None:
    _, outcome = run(_serve(broken_tls, "sslmode=require"))
    assert isinstance(outcome, ssl.SSLError)


def test_prefer_retries_in_plaintext_after_a_failed_handshake() -> None:
    """libpq's prefer: if the TLS attempt fails, a new plaintext connection."""
    seen, outcome = run(_serve(broken_tls, "sslmode=prefer"))
    assert outcome is None
    assert seen.connections == [["sslrequest"], ["startup"]]


async def plaintext_rejected(reader: StreamReader, writer: StreamWriter, log: list[str]) -> None:
    """pg_hba allows only hostssl: a plaintext startup is rejected."""
    packet = await _first_packet(reader)
    if packet == SSL_REQUEST:
        log.append("sslrequest")
        writer.write(b"N")
        await writer.drain()
        return
    log.append("startup")
    writer.write(error("28000", "no pg_hba.conf entry, no encryption"))
    await writer.drain()


def test_allow_tries_plaintext_first_then_tls() -> None:
    seen, outcome = run(_serve(plaintext_rejected, "sslmode=allow"))
    assert seen.connections == [["startup"], ["sslrequest"]]
    assert outcome is not None  # this server offers no TLS either


async def asks_cleartext(reader: StreamReader, writer: StreamWriter, log: list[str]) -> None:
    await _first_packet(reader)
    log.append("startup")
    writer.write(AUTH_CLEARTEXT)
    await writer.drain()
    data = await reader.read(1)
    if data[:1] == b"p":
        log.append("password sent")


def test_require_auth_refuses_before_sending_the_password() -> None:
    seen, outcome = run(_serve(asks_cleartext, "sslmode=disable&require_auth=scram-sha-256"))
    assert isinstance(outcome, WireProtocolError)
    assert "require_auth" in str(outcome)
    assert seen.connections == [["startup"]]


def test_require_auth_refuses_a_server_that_asks_for_nothing() -> None:
    """A trust server (or an impostor) authenticates without a password;
    require_auth=scram-sha-256 must not accept that."""
    _, outcome = run(_serve(no_tls, "sslmode=disable&require_auth=scram-sha-256"))
    assert isinstance(outcome, WireProtocolError)
    assert "require_auth" in str(outcome)


async def skips_scram_final(reader: StreamReader, writer: StreamWriter, log: list[str]) -> None:
    """Starts SCRAM, then accepts the client without sending server-final:
    what an impostor that does not know the password would do."""
    await _first_packet(reader)
    log.append("startup")
    mechanisms = b"SCRAM-SHA-256\0\0"
    writer.write(b"R" + struct.pack(">ii", 8 + len(mechanisms), 10) + mechanisms)
    await writer.drain()
    await reader.readexactly(1)  # "p": SASLInitialResponse
    (length,) = struct.unpack(">i", await reader.readexactly(4))
    body = await reader.readexactly(length - 4)
    client_first = body[body.index(b"\0") + 5 :].decode()
    nonce = client_first.split("r=", 1)[1]
    server_first = f"r={nonce}server,s=c2FsdHNhbHQ=,i=4096".encode()
    writer.write(b"R" + struct.pack(">ii", 8 + len(server_first), 11) + server_first)
    await writer.drain()
    await reader.readexactly(1)
    (length,) = struct.unpack(">i", await reader.readexactly(4))
    await reader.readexactly(length - 4)  # client-final, with the proof
    log.append("client-final")
    writer.write(AUTH_OK + READY)  # no AuthenticationSASLFinal
    await writer.drain()
    await reader.read()


def test_a_server_that_skips_the_scram_proof_is_refused() -> None:
    seen, outcome = run(_serve(skips_scram_final, "sslmode=disable"))
    assert seen.connections == [["startup", "client-final"]]
    assert isinstance(outcome, WireProtocolError)
    assert "never proved it knows the password" in str(outcome)
