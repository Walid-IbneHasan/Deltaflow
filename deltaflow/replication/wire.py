"""Minimal PostgreSQL wire-protocol (v3) client for logical replication connections.

psycopg has no support for the streaming replication sub-protocol (CopyBoth),
so the replication connection speaks the protocol directly. Only what
replication needs is implemented:

* TLS, negotiated as libpq does for every `sslmode` (conninfo.py resolves
  the settings, environment variables included, and refuses those it
  cannot honour);
* startup with `replication=database`, authentication by trust, cleartext
  password, MD5 or SCRAM-SHA-256, restricted by `require_auth` if set;
* simple queries (replication commands such as CREATE_REPLICATION_SLOT);
* CopyBoth mode for START_REPLICATION.

TLS is negotiated on the raw socket before any stream exists: after the
server's one-byte "S", everything must be TLS. Bytes a man in the middle
appends to that answer (CVE-2021-23222) then fail the handshake instead of
being read as the server's first messages.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import socket
import ssl
import struct
import sys

from deltaflow.replication import conninfo, scram
from deltaflow.replication._buffer import ByteReader
from deltaflow.replication.conninfo import AuthPolicy, ConnectSettings

log = logging.getLogger(__name__)

_PROTOCOL_V3 = 3 << 16
_SSL_REQUEST = struct.pack(">ii", 8, 80877103)
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


class TlsUnavailableError(ConnectionError):
    """The server does not accept TLS connections, and sslmode requires TLS."""


class _NoTls(Exception):
    """The server answered "N" to SSLRequest."""


#: authentication request codes, as require_auth names them
_METHODS = {
    AUTH_CLEARTEXT: "password", AUTH_MD5: "md5", AUTH_SASL: "scram-sha-256", 7: "gss", 9: "sspi",
}  # fmt: skip


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
        """Connect as libpq would for `dsn`. `timeout` applies unless the
        settings give a `connect_timeout` (0 there means no limit)."""
        settings = conninfo.resolve(dsn)
        limit = timeout if settings.connect_timeout is None else settings.connect_timeout or None
        async with asyncio.timeout(limit):
            return await cls._negotiate(settings, application_name)

    @classmethod
    async def _negotiate(cls, s: ConnectSettings, app: str) -> ReplicationConnection:
        """libpq's order of attempts for each sslmode. Unix sockets never use
        TLS, as in libpq."""
        if s.unix_socket or s.sslmode == "disable":
            return await cls._attempt(s, app, None)
        tls = conninfo.ssl_context(s)
        assert tls is not None
        if s.sslmode == "allow":
            try:
                return await cls._attempt(s, app, None)
            except PgError as plaintext_refused:
                log.info("plaintext connection refused (%s); retrying with TLS", plaintext_refused)
                try:
                    return await cls._attempt(s, app, tls)
                except _NoTls:
                    raise plaintext_refused from None
        if s.sslmode == "prefer":
            try:
                # A server without TLS answers "N", and the same connection
                # continues in plaintext.
                return await cls._attempt(s, app, tls, plaintext_if_refused=True)
            except (ssl.SSLError, ConnectionError) as exc:
                log.warning(
                    "TLS handshake with %s:%s failed (%s); retrying without TLS",
                    s.host, s.port, exc,
                )  # fmt: skip
            return await cls._attempt(s, app, None)
        try:
            return await cls._attempt(s, app, tls)
        except _NoTls:
            raise TlsUnavailableError(
                f"{s.host}:{s.port} does not accept TLS connections, and sslmode={s.sslmode} "
                "requires TLS"
            ) from None

    @classmethod
    async def _attempt(
        cls,
        s: ConnectSettings,
        app: str,
        tls: ssl.SSLContext | None,
        *,
        plaintext_if_refused: bool = False,
    ) -> ReplicationConnection:
        if s.unix_socket and sys.platform != "win32":
            reader, writer = await asyncio.open_unix_connection(f"{s.host}/.s.PGSQL.{s.port}")
        else:
            sock = await _tcp_connect(s)
            try:
                if tls is not None and await _request_tls(sock):
                    reader, writer = await asyncio.open_connection(
                        sock=sock, ssl=tls, server_hostname=conninfo.server_hostname(s)
                    )
                elif tls is None or plaintext_if_refused:
                    reader, writer = await asyncio.open_connection(sock=sock)
                else:
                    raise _NoTls
            except BaseException:
                sock.close()
                raise
        conn = cls(reader, writer)
        try:
            await conn._startup(s, app)
        except BaseException:
            conn._abort()
            raise
        return conn

    @property
    def tls(self) -> bool:
        """Whether this connection is encrypted."""
        return self._writer.get_extra_info("ssl_object") is not None

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

    async def _startup(self, s: ConnectSettings, app: str) -> None:
        params = {
            "user": s.user,
            "database": s.dbname,
            "replication": "database",
            "application_name": app,
            "client_encoding": "UTF8",
            # Text output formats that psycopg's loaders parse unambiguously.
            "DateStyle": "ISO, YMD",
            "IntervalStyle": "postgres",
            "TimeZone": "UTC",
            "extra_float_digits": "3",
        }
        if s.options:
            params["options"] = s.options
        body = _I32.pack(_PROTOCOL_V3) + b"".join(_cstr(k) + _cstr(v) for k, v in params.items())
        body += b"\0"
        self._writer.write(_I32.pack(len(body) + 4) + body)

        exchange: scram.ScramSha256 | None = None
        method = "none"
        while True:
            tag, payload = await self._read()
            if self._background(tag, payload):
                continue
            match tag:
                case b"R":
                    code = _I32.unpack_from(payload)[0]
                    if code == AUTH_OK:
                        _check_policy(s.require_auth, method, "authenticated the connection with")
                        if exchange is not None and not exchange.verified:
                            raise WireProtocolError(
                                "server accepted the connection before completing SCRAM "
                                "authentication, so it never proved it knows the password"
                            )
                    elif code in _METHODS and method != _METHODS[code]:
                        method = _METHODS[code]
                        _check_policy(s.require_auth, method, "asks for")
                    exchange = self._authenticate(payload, s.user, s.password, exchange)
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


def _check_policy(policy: AuthPolicy, method: str, what: str) -> None:
    """Refuse a method require_auth does not allow, before answering it."""
    if not policy.allows(method):
        raise WireProtocolError(
            f"server {what} {method} authentication, which require_auth={policy.spec} "
            "does not allow"
        )


async def _tcp_connect(s: ConnectSettings) -> socket.socket:
    """A connected, non-blocking TCP socket to the first address that answers."""
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(s.host, s.port, type=socket.SOCK_STREAM)
    error: OSError | None = None
    for family, kind, proto, _, address in infos:
        sock = socket.socket(family, kind, proto)
        try:
            sock.setblocking(False)
            await loop.sock_connect(sock, address)
        except OSError as exc:
            sock.close()
            error = exc
            continue
        except BaseException:
            sock.close()
            raise
        conninfo.apply_keepalives(sock, s)
        return sock
    raise error or OSError(f"could not resolve {s.host!r}")


async def _request_tls(sock: socket.socket) -> bool:
    """SSLRequest, and whether the server agreed. Exactly one byte of the
    answer is read, so nothing sent after it can bypass the handshake."""
    loop = asyncio.get_running_loop()
    await loop.sock_sendall(sock, _SSL_REQUEST)
    answer = await loop.sock_recv(sock, 1)
    if answer in (b"S", b"N"):
        return answer == b"S"
    if not answer:
        raise ConnectionError("server closed the connection during TLS negotiation")
    raise WireProtocolError(f"unexpected answer {answer!r} to SSLRequest")
