"""Minimal PostgreSQL wire-protocol (v3) client for logical replication connections.

psycopg has no support for the streaming replication sub-protocol (CopyBoth),
so the replication connection speaks the protocol directly. Only what
replication needs is implemented:

* startup with `replication=database`, authentication by trust, cleartext
  password, MD5 or SCRAM-SHA-256;
* simple queries (replication commands such as CREATE_REPLICATION_SLOT);
* CopyBoth mode for START_REPLICATION.

There is no TLS: connect over localhost or a trusted network only.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import struct
import sys
from typing import Any

from psycopg.conninfo import conninfo_to_dict

from deltaflow.replication import scram
from deltaflow.replication._buffer import ByteReader

log = logging.getLogger(__name__)

_PROTOCOL_V3 = 3 << 16
_HEADER = struct.Struct(">ci")
_I32 = struct.Struct(">i")

AUTH_OK, AUTH_CLEARTEXT, AUTH_MD5 = 0, 3, 5
AUTH_SASL, AUTH_SASL_CONTINUE, AUTH_SASL_FINAL = 10, 11, 12


class PgError(Exception):
    """An ErrorResponse from the server."""

    def __init__(self, fields: dict[str, str]) -> None:
        self.fields = fields
        self.sqlstate = fields.get("C", "")
        message = f"{fields.get('S', 'ERROR')} {self.sqlstate}: {fields.get('M', '')}"
        if "D" in fields:
            message += f" DETAIL: {fields['D']}"
        super().__init__(message)


class WireProtocolError(Exception):
    """The server sent a message that is not valid at this point of the protocol."""


def _cstr(s: str) -> bytes:
    return s.encode() + b"\0"


def _fields(payload: bytes) -> dict[str, str]:
    r = ByteReader(payload)
    out: dict[str, str] = {}
    while (code := r.byte()) != b"\0":
        out[code.decode()] = r.cstring()
    return out


class ReplicationConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self.parameters: dict[str, str] = {}
        self.backend_pid: int | None = None
        self._closed = False

    @classmethod
    async def connect(
        cls, dsn: str, *, application_name: str = "deltaflow", timeout: float = 10.0
    ) -> ReplicationConnection:
        info: dict[str, Any] = conninfo_to_dict(dsn)
        host = str(info.get("host") or "localhost")
        port = int(info.get("port") or 5432)
        user = str(info["user"])
        dbname = str(info.get("dbname") or user)
        password = info.get("password")
        async with asyncio.timeout(timeout):
            if host.startswith("/") and sys.platform != "win32":
                reader, writer = await asyncio.open_unix_connection(f"{host}/.s.PGSQL.{port}")
            else:
                reader, writer = await asyncio.open_connection(host, port)
            conn = cls(reader, writer)
            try:
                await conn._startup(
                    user, dbname, None if password is None else str(password), application_name
                )
            except BaseException:
                conn._abort()
                raise
        return conn

    # -- framing -----------------------------------------------------------

    def _send(self, tag: bytes, payload: bytes = b"") -> None:
        self._writer.write(_HEADER.pack(tag, len(payload) + 4) + payload)

    async def _read(self) -> tuple[bytes, bytes]:
        header = await self._reader.readexactly(_HEADER.size)
        tag, length = _HEADER.unpack(header)
        return tag, await self._reader.readexactly(length - 4)

    async def _read_until_ready(self) -> None:
        """Discard messages up to ReadyForQuery (used after errors)."""
        while (await self._read())[0] != b"Z":
            pass

    def _background(self, tag: bytes, payload: bytes) -> bool:
        """Handle messages that may arrive at any time. True if consumed."""
        if tag == b"N":
            log.info("server notice: %s", _fields(payload).get("M", ""))
            return True
        if tag == b"S":
            r = ByteReader(payload)
            self.parameters[r.cstring()] = r.cstring()
            return True
        return False

    # -- startup -----------------------------------------------------------

    async def _startup(self, user: str, dbname: str, password: str | None, app: str) -> None:
        params = {
            "user": user,
            "database": dbname,
            "replication": "database",
            "application_name": app,
            "client_encoding": "UTF8",
            # Text output formats that psycopg's loaders parse unambiguously.
            "DateStyle": "ISO, YMD",
            "IntervalStyle": "postgres",
            "TimeZone": "UTC",
            "extra_float_digits": "3",
        }
        body = _I32.pack(_PROTOCOL_V3) + b"".join(_cstr(k) + _cstr(v) for k, v in params.items())
        body += b"\0"
        self._writer.write(_I32.pack(len(body) + 4) + body)

        exchange: scram.ScramSha256 | None = None
        while True:
            tag, payload = await self._read()
            if self._background(tag, payload):
                continue
            match tag:
                case b"R":
                    exchange = self._authenticate(payload, user, password, exchange)
                case b"K":
                    self.backend_pid = _I32.unpack_from(payload)[0]
                case b"Z":
                    return
                case b"E":
                    raise PgError(_fields(payload))
                case _:
                    raise WireProtocolError(f"unexpected {tag!r} during startup")

    def _authenticate(
        self, payload: bytes, user: str, password: str | None, exchange: scram.ScramSha256 | None
    ) -> scram.ScramSha256 | None:
        r = ByteReader(payload)
        code = r.i32()
        if code == AUTH_OK:
            return None
        if password is None:
            raise WireProtocolError(f"server requires a password (auth method {code})")
        if code == AUTH_CLEARTEXT:
            self._send(b"p", _cstr(password))
        elif code == AUTH_MD5:
            salt = r.take(4)
            inner = hashlib.md5((password + user).encode()).hexdigest().encode()
            self._send(b"p", _cstr("md5" + hashlib.md5(inner + salt).hexdigest()))
        elif code == AUTH_SASL:
            mechanisms = []
            while (m := r.cstring()) != "":
                mechanisms.append(m)
            if scram.MECHANISM not in mechanisms:
                raise WireProtocolError(f"no supported SASL mechanism in {mechanisms}")
            exchange = scram.ScramSha256(password)
            first = exchange.client_first()
            self._send(b"p", _cstr(scram.MECHANISM) + _I32.pack(len(first)) + first)
        elif code == AUTH_SASL_CONTINUE and exchange is not None:
            self._send(b"p", exchange.client_final(r.rest()))
        elif code == AUTH_SASL_FINAL and exchange is not None:
            exchange.verify_server_final(r.rest())
        else:
            raise WireProtocolError(f"unsupported authentication request {code}")
        return exchange

    # -- simple query ------------------------------------------------------

    async def simple_query(self, sql: str) -> list[tuple[str | None, ...]]:
        """Run one command; return its rows as text. Raises PgError on failure."""
        self._send(b"Q", _cstr(sql))
        rows: list[tuple[str | None, ...]] = []
        error: PgError | None = None
        while True:
            tag, payload = await self._read()
            if self._background(tag, payload):
                continue
            match tag:
                case b"D":
                    r = ByteReader(payload)
                    row: list[str | None] = []
                    for _ in range(r.i16()):
                        n = r.i32()
                        row.append(None if n < 0 else r.take(n).decode())
                    rows.append(tuple(row))
                case b"T" | b"C" | b"I":
                    pass
                case b"E":
                    error = PgError(_fields(payload))
                case b"Z":
                    if error is not None:
                        raise error
                    return rows
                case _:
                    raise WireProtocolError(f"unexpected {tag!r} in simple query response")

    # -- CopyBoth ----------------------------------------------------------

    async def start_copy_both(self, sql: str) -> None:
        """Send a command that switches to CopyBoth (START_REPLICATION)."""
        self._send(b"Q", _cstr(sql))
        while True:
            tag, payload = await self._read()
            if self._background(tag, payload):
                continue
            if tag == b"W":
                return
            if tag == b"E":
                error = PgError(_fields(payload))
                await self._read_until_ready()
                raise error
            raise WireProtocolError(f"expected CopyBothResponse, got {tag!r}")

    async def read_copy_data(self) -> bytes | None:
        """Next CopyData payload, or None once the server has ended the copy."""
        while True:
            tag, payload = await self._read()
            if self._background(tag, payload):
                continue
            match tag:
                case b"d":
                    return payload
                case b"c":
                    return None
                case b"E":
                    raise PgError(_fields(payload))
                case _:
                    raise WireProtocolError(f"unexpected {tag!r} in CopyBoth stream")

    def send_copy_data(self, data: bytes) -> None:
        """Queue one complete CopyData message (safe to call from any task)."""
        self._send(b"d", data)

    async def drain(self) -> None:
        await self._writer.drain()

    def send_copy_done(self) -> None:
        """Ask the server to leave CopyBoth. It keeps sending CopyData until it
        answers with CopyDone, so the reader must keep reading."""
        self._send(b"c")

    async def finish_command(self) -> None:
        """After the server's CopyDone: consume CommandComplete .. ReadyForQuery."""
        error: PgError | None = None
        while True:
            tag, payload = await self._read()
            if tag == b"E":
                error = PgError(_fields(payload))
            elif tag == b"Z":
                break
        if error is not None:
            raise error

    # -- teardown ----------------------------------------------------------

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(ConnectionError, OSError):
            self._send(b"X")
            await self._writer.drain()
        self._writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await self._writer.wait_closed()

    def _abort(self) -> None:
        self._closed = True
        self._writer.close()
