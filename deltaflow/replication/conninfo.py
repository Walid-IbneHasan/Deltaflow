"""Connection settings for the replication connection, resolved the way libpq resolves them.

Every psycopg connection goes through libpq; the replication connection
(wire.py) does not. It therefore resolves its settings here, with libpq's own
parser and defaults (`PQconninfoParse` and `PQconndefaults`, via psycopg.pq).
Environment variables such as PGSSLMODE apply to both kinds of connection,
so one process never mixes TLS and plaintext connections by accident.

Implemented as libpq documents it:

* `sslmode` disable / allow / prefer / require / verify-ca / verify-full;
  `sslrootcert` (a file, or `system`), `sslcert`, `sslkey`, `sslpassword`,
  `sslcertmode` allow/disable, `sslsni`, `ssl_min_protocol_version`,
  `ssl_max_protocol_version`, `sslkeylogfile`. As in libpq, a root
  certificate file that exists is used to verify the server in every mode
  but disable (so `require` with `~/.postgresql/root.crt` acts as verify-ca);
* the password: from the DSN, PGPASSWORD, or the password file;
* `connect_timeout`, TCP keepalives, `tcp_user_timeout` (Linux),
  `require_auth`, `options`.

* several hosts (`host=a,b` with a matching `port` list, or one port for
  all) tried in order, and `target_session_attrs` any / read-write /
  primary, so that after a failover the connection finds the new primary
  (wire.py checks each server's reported state).

Everything else libpq would act on is **refused** with `ConnInfoError`, not
ignored: `hostaddr`, service files, CRLs, `channel_binding=require`, GSS
encryption, the other `target_session_attrs`, direct TLS negotiation, and
`options` that change the output formats the decoder parses. A setting
left at libpq's default is always fine.

One documented difference: with no host at all, libpq on POSIX uses a Unix
socket in its compiled-in directory; this module uses localhost over TCP.
"""

from __future__ import annotations

import logging
import os
import re
import socket
import ssl
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import pq

log = logging.getLogger(__name__)


class ConnInfoError(ValueError):
    """A connection setting the replication connection cannot honour."""


SSLMODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
VERIFYING = ("verify-ca", "verify-full")

_TLS_VERSIONS = {
    "TLSv1": ssl.TLSVersion.TLSv1,
    "TLSv1.1": ssl.TLSVersion.TLSv1_1,
    "TLSv1.2": ssl.TLSVersion.TLSv1_2,
    "TLSv1.3": ssl.TLSVersion.TLSv1_3,
}
_AUTH_METHODS = frozenset({"password", "md5", "gss", "sspi", "scram-sha-256", "none"})

#: Server settings the startup packet pins, because the decoder parses
#: text in exactly these formats (wire.py). `options` must not change them.
_PINNED = re.compile(
    r"\b(datestyle|intervalstyle|timezone|extra_float_digits|client_encoding)\b", re.IGNORECASE
)

#: Settings libpq acts on that are accepted only at these values.
_ONLY: dict[str, frozenset[str]] = {
    "channel_binding": frozenset({"disable", "prefer"}),
    "gssencmode": frozenset({"disable", "prefer"}),
    "target_session_attrs": frozenset({"any", "read-write", "primary"}),
    "load_balance_hosts": frozenset({"disable"}),
    "sslnegotiation": frozenset({"postgres"}),
    "sslcertmode": frozenset({"allow", "disable"}),
    "replication": frozenset({"database"}),
    "client_encoding": frozenset({"utf8", "unicode"}),  # compared case- and dash-insensitively
    "min_protocol_version": frozenset({"3.0"}),
    "max_protocol_version": frozenset({"3.0"}),
}

#: Settings this module implements, or that have no effect without GSS
#: (which is never used). `sslcompression` has no effect in libpq either.
_HANDLED = frozenset({
    "host", "port", "user", "password", "passfile", "dbname", "connect_timeout", "options",
    "application_name", "fallback_application_name",
    "keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count", "tcp_user_timeout",
    "sslmode", "sslrootcert", "sslcert", "sslkey", "sslpassword", "sslsni",
    "ssl_min_protocol_version", "ssl_max_protocol_version", "sslkeylogfile", "sslcompression",
    "require_auth", "krbsrvname", "gsslib", "gssdelegation",
}) | frozenset(_ONLY)  # fmt: skip


@dataclass(frozen=True)
class AuthPolicy:
    """libpq's `require_auth`: the authentication methods the server may
    use. A list of names allows exactly those; a list of `!name` refuses
    those. `none` means the server asked for nothing at all."""

    methods: frozenset[str]
    negated: bool
    spec: str

    @classmethod
    def parse(cls, value: str | None) -> AuthPolicy:
        if not value:
            return cls(frozenset(), negated=True, spec="")
        items = [item.strip() for item in value.split(",")]
        negations = {item.startswith("!") for item in items}
        if len(negations) > 1:
            raise ConnInfoError(f"require_auth={value!r} mixes required and refused methods")
        names = frozenset(item.removeprefix("!") for item in items)
        if unknown := names - _AUTH_METHODS:
            raise ConnInfoError(f"require_auth: unknown method(s) {sorted(unknown)}")
        return cls(names, negated=negations == {True}, spec=value)

    def allows(self, method: str) -> bool:
        return method not in self.methods if self.negated else method in self.methods


@dataclass(frozen=True)
class ConnectSettings:
    host: str
    port: int
    user: str
    dbname: str
    password: str | None
    options: str
    #: seconds; None when unset (the caller's default applies), 0 for no limit
    connect_timeout: float | None
    keepalives: bool
    keepalives_idle: int | None
    keepalives_interval: int | None
    keepalives_count: int | None
    tcp_user_timeout: int | None
    sslmode: str
    sslrootcert: str | None
    sslcert: str | None
    sslkey: str | None
    sslpassword: str | None
    sslcertmode: str
    sslsni: bool
    ssl_min_protocol_version: str
    ssl_max_protocol_version: str | None
    sslkeylogfile: str | None
    require_auth: AuthPolicy

    @property
    def unix_socket(self) -> bool:
        return self.host.startswith("/")


def resolve(dsn: str) -> ConnectSettings:
    """The settings libpq would use for `dsn`'s first host, or ConnInfoError."""
    return resolve_hosts(dsn)[0][0]


def resolve_hosts(dsn: str) -> tuple[list[ConnectSettings], str]:
    """The settings for each of `dsn`'s hosts, in the order libpq tries
    them, and `target_session_attrs`."""
    values, explicit = _merge(dsn)
    hosts = (values["host"] or "localhost").split(",")
    ports = (values["port"] or "").split(",")
    if len(ports) == 1:
        ports *= len(hosts)
    if len(ports) != len(hosts):
        raise ConnInfoError(
            f"{len(hosts)} hosts and {len(ports)} ports: give one port, or one each"
        )
    attrs = values["target_session_attrs"] or "any"
    return [
        _settings(values, explicit, host or "localhost", port or "5432")
        for host, port in zip(hosts, ports, strict=True)
    ], attrs


def _settings(
    values: dict[str, str | None], explicit: set[str], host: str, port: str
) -> ConnectSettings:
    user = values["user"]
    if not user:
        raise ConnInfoError("no user name in the connection settings")
    dbname = values["dbname"] or user
    options = values["options"] or ""
    if m := _PINNED.search(options):
        raise ConnInfoError(
            f"options sets {m.group(1)}, which the replication connection pins: "
            "the decoder parses text in that format"
        )
    password = values["password"]
    if password is None:
        passfile = Path(values["passfile"]) if values["passfile"] else _default_file("pgpass")
        password = pgpass_password(passfile, host, int(port), dbname, user)
    return ConnectSettings(
        host=host,
        port=int(port),
        user=user,
        dbname=dbname,
        password=password,
        options=options,
        require_auth=AuthPolicy.parse(values["require_auth"]),
        **_tcp(values),
        **_tls(values, explicit),
    )


def _tcp(values: dict[str, str | None]) -> dict[str, Any]:
    timeout = _int(values, "connect_timeout")
    tcp_user_timeout = _int(values, "tcp_user_timeout")
    if tcp_user_timeout and _USER_TIMEOUT is None:
        raise ConnInfoError(f"tcp_user_timeout is not supported on {sys.platform}")
    keepalive = {key: _int(values, key) for key in _KEEPALIVE_OPTIONS}
    for key, value in keepalive.items():
        if value is not None and _KEEPALIVE_OPTIONS[key] is None:
            raise ConnInfoError(f"{key} is not supported on {sys.platform}")
    return {
        # libpq: zero or negative waits forever, and 2 s is the minimum
        "connect_timeout": None if timeout is None else (0 if timeout <= 0 else max(timeout, 2)),
        "keepalives": values["keepalives"] != "0",
        "tcp_user_timeout": tcp_user_timeout,
        **keepalive,
    }


def _tls(values: dict[str, str | None], explicit: set[str]) -> dict[str, Any]:
    sslmode = values["sslmode"] or "prefer"
    if sslmode not in SSLMODES:
        raise ConnInfoError(f"invalid sslmode {sslmode!r}; one of {', '.join(SSLMODES)}")
    sslrootcert = values["sslrootcert"] or None
    if sslrootcert == "system":
        # libpq 16: the system's roots imply a full check, and weaker modes
        # are an error rather than a silent downgrade.
        if "sslmode" not in explicit:
            sslmode = "verify-full"
        elif sslmode != "verify-full":
            raise ConnInfoError(
                f'sslmode={sslmode} may not be used with sslrootcert=system (use "verify-full")'
            )
    sslsni = values["sslsni"] != "0"
    if not sslsni and sslmode == "verify-full":
        raise ConnInfoError("sslsni=0 with sslmode=verify-full is not supported")
    versions = {
        "ssl_min_protocol_version": values["ssl_min_protocol_version"] or "TLSv1.2",
        "ssl_max_protocol_version": values["ssl_max_protocol_version"] or None,
    }
    for key, version in versions.items():
        if version is not None and version not in _TLS_VERSIONS:
            raise ConnInfoError(f"{key}={version!r}; one of {', '.join(_TLS_VERSIONS)}")
    return {
        "sslmode": sslmode,
        "sslrootcert": sslrootcert,
        "sslcert": values["sslcert"] or None,
        "sslkey": values["sslkey"] or None,
        "sslpassword": values["sslpassword"] or None,
        "sslcertmode": values["sslcertmode"] or "allow",
        "sslsni": sslsni,
        "sslkeylogfile": values["sslkeylogfile"] or None,
        **versions,
    }


def _merge(dsn: str) -> tuple[dict[str, str | None], set[str]]:
    """libpq's view of `dsn`: its values over the defaults (environment, then
    compiled-in), plus which keys were set explicitly (DSN or environment).
    Refuses anything set that this module does not honour."""
    try:
        parsed = {o.keyword.decode(): o.val for o in pq.Conninfo.parse(dsn.encode())}
    except Exception as exc:  # psycopg raises its own error types for a bad DSN
        raise ConnInfoError(f"invalid connection string: {exc}") from exc
    values: dict[str, str | None] = {}
    compiled: dict[str, str | None] = {}
    explicit: set[str] = set()
    for option in pq.Conninfo.get_defaults():
        key = option.keyword.decode()
        raw = parsed.get(key)
        if raw is not None or (option.val is not None and option.val != option.compiled):
            explicit.add(key)
        if raw is None:
            raw = option.val
        values[key] = None if raw is None else raw.decode()
        compiled[key] = None if option.compiled is None else option.compiled.decode()
    for key, value in values.items():
        if value is None or value == "" or value == compiled.get(key):
            continue
        if key in _ONLY:
            normal = value.lower().replace("-", "") if key == "client_encoding" else value
            if normal not in _ONLY[key]:
                allowed = ", ".join(sorted(_ONLY[key]))
                raise ConnInfoError(
                    f"{key}={value!r} is not supported by the replication connection "
                    f"(supported: {allowed})"
                )
        elif key not in _HANDLED:
            raise ConnInfoError(f"{key}={value!r} is not supported by the replication connection")
    for key in _HANDLED:
        values.setdefault(key, None)  # older libpq versions lack some keys
    return values, explicit


def _int(values: dict[str, str | None], key: str) -> int | None:
    raw = values[key]
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        raise ConnInfoError(f"{key}={raw!r} is not an integer") from None


#: socket options per libpq setting; None where the platform lacks one
#: (macOS calls the idle time TCP_KEEPALIVE)
_KEEPALIVE_OPTIONS: dict[str, int | None] = {
    "keepalives_idle": getattr(socket, "TCP_KEEPIDLE", None)
    or getattr(socket, "TCP_KEEPALIVE", None),
    "keepalives_interval": getattr(socket, "TCP_KEEPINTVL", None),
    "keepalives_count": getattr(socket, "TCP_KEEPCNT", None),
}
_USER_TIMEOUT: int | None = getattr(socket, "TCP_USER_TIMEOUT", None)  # Linux


def apply_keepalives(sock: socket.socket, settings: ConnectSettings) -> None:
    """libpq's TCP options: keepalives on by default, timings if set."""
    if settings.keepalives:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for key, value in (
            ("keepalives_idle", settings.keepalives_idle),
            ("keepalives_interval", settings.keepalives_interval),
            ("keepalives_count", settings.keepalives_count),
        ):
            option = _KEEPALIVE_OPTIONS[key]
            if value is not None and option is not None:  # resolve() refused the rest
                sock.setsockopt(socket.IPPROTO_TCP, option, value)
    if settings.tcp_user_timeout and _USER_TIMEOUT is not None:
        sock.setsockopt(socket.IPPROTO_TCP, _USER_TIMEOUT, settings.tcp_user_timeout)


# --- TLS ------------------------------------------------------------------


def ssl_context(settings: ConnectSettings) -> ssl.SSLContext | None:
    """The TLS context for `settings`, or None for sslmode=disable."""
    if settings.sslmode == "disable":
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = _TLS_VERSIONS[settings.ssl_min_protocol_version]
    if settings.ssl_max_protocol_version is not None:
        ctx.maximum_version = _TLS_VERSIONS[settings.ssl_max_protocol_version]
    verify = False
    if settings.sslrootcert == "system":
        ctx.load_default_certs()
        verify = True
    else:
        root = Path(settings.sslrootcert) if settings.sslrootcert else _default_file("root.crt")
        if root.is_file():
            # libpq verifies against an existing root file in every mode
            # (fe-secure-openssl.c, initialize_SSL).
            ctx.load_verify_locations(cafile=root)
            verify = True
        elif settings.sslmode in VERIFYING:
            raise ConnInfoError(
                f"root certificate file {root} does not exist; sslmode={settings.sslmode} needs "
                "one to verify the server (or sslrootcert=system for the system's roots)"
            )
    if verify:
        ctx.check_hostname = settings.sslmode == "verify-full"
        ctx.verify_mode = ssl.CERT_REQUIRED
    else:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if settings.sslcertmode != "disable":
        _load_client_certificate(ctx, settings)
    if settings.sslkeylogfile:
        ctx.keylog_filename = settings.sslkeylogfile
    return ctx


def server_hostname(settings: ConnectSettings) -> str:
    """The name sent for SNI and checked by verify-full ("" sends none)."""
    return settings.host if settings.sslsni or settings.sslmode == "verify-full" else ""


def _load_client_certificate(ctx: ssl.SSLContext, settings: ConnectSettings) -> None:
    cert = Path(settings.sslcert) if settings.sslcert else _default_file("postgresql.crt")
    if not cert.is_file():
        if settings.sslcert:
            raise ConnInfoError(f"client certificate file {cert} does not exist")
        return
    key = Path(settings.sslkey) if settings.sslkey else _default_file("postgresql.key")
    if sys.platform != "win32":
        mode = key.stat().st_mode
        owner_root = key.stat().st_uid == 0
        if mode & (stat.S_IRWXO | (stat.S_IWGRP | stat.S_IXGRP if owner_root else stat.S_IRWXG)):
            raise ConnInfoError(
                f"private key file {key} has group or world access; permissions should be "
                "u=rw (0600) or less, or u=rw,g=r (0640) or less if owned by root"
            )
    ctx.load_cert_chain(cert, key, password=settings.sslpassword)


# --- files ----------------------------------------------------------------


def _default_file(name: str) -> Path:
    """libpq's default locations: ~/.postgresql/<name> and ~/.pgpass on
    POSIX, %APPDATA%/postgresql/<name> and .../pgpass.conf on Windows."""
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA") or Path.home()) / "postgresql"
        return base / ("pgpass.conf" if name == "pgpass" else name)
    else:  # explicit else: mypy treats each platform branch as reachable
        home = Path(os.environ.get("HOME") or Path.home())
        return home / ".pgpass" if name == "pgpass" else home / ".postgresql" / name


def pgpass_password(path: Path, host: str, port: int, dbname: str, user: str) -> str | None:
    """The password the libpq password file gives for this connection.

    Lines are `host:port:database:user:password`; `*` matches anything,
    `\\:` and `\\\\` escape. As in libpq, the file is ignored on POSIX if
    group or others can read it."""
    try:
        st = path.stat()
    except OSError:
        return None
    if not stat.S_ISREG(st.st_mode):
        return None
    if sys.platform != "win32" and st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        log.warning("password file %s has group or world access; ignoring it", path)
        return None
    if host.startswith("/"):
        host = "localhost"  # libpq matches socket connections as localhost
    wanted = (host, str(port), dbname, user)
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        fields = _pgpass_fields(line)
        if len(fields) == 5 and all(f in ("*", w) for f, w in zip(fields[:4], wanted, strict=True)):
            return fields[4]
    return None


def _pgpass_fields(line: str) -> list[str]:
    fields: list[str] = []
    current: list[str] = []
    chars = iter(line)
    for ch in chars:
        if ch == "\\":
            current.append(next(chars, ""))
        elif ch == ":" and len(fields) < 4:
            fields.append("".join(current))
            current = []
        else:
            current.append(ch)
    fields.append("".join(current))
    return fields
