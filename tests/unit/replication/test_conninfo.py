"""Connection settings for the replication connection: resolved as libpq
resolves them, and refused where they cannot be honoured."""

from __future__ import annotations

import os
import ssl
import sys
from pathlib import Path

import pytest

from deltaflow.replication.conninfo import (
    AuthPolicy,
    ConnInfoError,
    pgpass_password,
    resolve,
    resolve_hosts,
    ssl_context,
)

from .certs import trust_anchor

BASE = "postgresql://alice:secret@db.example.com:6543/shop"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No PG* variables from the developer's shell, and a home directory
    without ~/.postgresql or ~/.pgpass."""
    for name in list(os.environ):
        if name.startswith("PG"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))


def test_dsn_fields_are_resolved() -> None:
    s = resolve(BASE)
    assert (s.host, s.port, s.user, s.password, s.dbname) == (
        "db.example.com", 6543, "alice", "secret", "shop",
    )  # fmt: skip
    assert s.sslmode == "prefer"  # libpq's default
    assert s.keepalives is True  # libpq's default for TCP
    assert s.options == ""


def test_the_database_defaults_to_the_user() -> None:
    assert resolve("postgresql://alice@localhost").dbname == "alice"


def test_environment_variables_apply_like_libpq(monkeypatch: pytest.MonkeyPatch) -> None:
    """psycopg connections honour PGSSLMODE; if the replication connection
    did not, one process would mix TLS and plaintext connections."""
    monkeypatch.setenv("PGSSLMODE", "require")
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "7")
    s = resolve(BASE)
    assert s.sslmode == "require"
    assert s.connect_timeout == 7
    assert resolve(BASE + "?sslmode=disable").sslmode == "disable"  # the DSN wins


def test_connect_timeout_follows_libpq() -> None:
    assert resolve(BASE + "?connect_timeout=0").connect_timeout == 0  # no limit
    assert resolve(BASE + "?connect_timeout=1").connect_timeout == 2  # libpq's minimum
    assert resolve(BASE).connect_timeout is None  # unset: the caller's default applies


@pytest.mark.parametrize(
    ("dsn", "message"),
    [
        ("host=h1,h2,h3 port=5432,5433 user=alice", "3 hosts and 2 ports"),
        (BASE + "?hostaddr=10.0.0.1", "hostaddr"),
        (BASE + "?service=prod", "service"),
        (BASE + "?sslcrl=/x.crl", "sslcrl"),
        (BASE + "?channel_binding=require", "channel_binding"),
        (BASE + "?gssencmode=require", "gssencmode"),
        (BASE + "?target_session_attrs=standby", "target_session_attrs"),
        (BASE + "?load_balance_hosts=random", "load_balance_hosts"),
        (BASE + "?client_encoding=LATIN1", "client_encoding"),
        (BASE + "?sslnegotiation=direct", "sslnegotiation"),
        (BASE + "?sslcertmode=require", "sslcertmode"),
        (BASE + "?replication=true", "replication"),
        (BASE + "?options=-c%20DateStyle%3DSQL", "DateStyle"),
        (BASE + "?options=-c%20timezone%3DEurope/Amsterdam", "timezone"),
        (BASE + "?sslmode=require&sslrootcert=system", "verify-full"),
        (BASE + "?sslmode=verify-full&sslsni=0", "sslsni"),
        (BASE + "?sslmode=bogus", "sslmode"),
        (BASE + "?require_auth=md5,!password", "require_auth"),
    ],
)
def test_settings_that_cannot_be_honoured_are_refused(dsn: str, message: str) -> None:
    with pytest.raises(ConnInfoError, match=message):
        resolve(dsn)


def test_settings_at_their_harmless_values_are_accepted() -> None:
    resolve(
        BASE + "?channel_binding=disable&gssencmode=disable&target_session_attrs=any"
        "&client_encoding=UTF8&replication=database&sslnegotiation=postgres&sslcertmode=allow"
        "&options=-c%20statement_timeout%3D0&application_name=x"
    )


def test_several_hosts_are_tried_in_order_as_libpq_does() -> None:
    """A port list matches the host list; one port applies to every host;
    target_session_attrs read-write or primary keeps standbys out."""
    hosts, attrs = resolve_hosts("host=a,b,c port=5432,,6432 user=alice dbname=shop")
    assert [(h.host, h.port) for h in hosts] == [("a", 5432), ("b", 5432), ("c", 6432)]
    assert attrs == "any"
    hosts, attrs = resolve_hosts(
        "postgresql://alice@a:5433,b:5434/shop?target_session_attrs=read-write"
    )
    assert [(h.host, h.port, h.dbname) for h in hosts] == [("a", 5433, "shop"), ("b", 5434, "shop")]
    assert attrs == "read-write"
    hosts, _ = resolve_hosts("host=a,b port=6000 user=alice")
    assert [h.port for h in hosts] == [6000, 6000]
    assert resolve("host=a,b user=alice").host == "a"


def test_sslrootcert_system_defaults_to_verify_full() -> None:
    s = resolve(BASE + "?sslrootcert=system")
    assert s.sslmode == "verify-full"


# --- TLS contexts ---------------------------------------------------------


def _ctx(dsn: str) -> ssl.SSLContext:
    ctx = ssl_context(resolve(dsn))
    assert ctx is not None
    return ctx


def test_disable_has_no_tls_context() -> None:
    assert ssl_context(resolve(BASE + "?sslmode=disable")) is None


@pytest.mark.parametrize("mode", ["allow", "prefer", "require"])
def test_unverified_modes_encrypt_without_checking_the_server(mode: str) -> None:
    ctx = _ctx(f"{BASE}?sslmode={mode}")
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2  # libpq's default floor


def test_verify_ca_checks_the_chain_but_not_the_name(tmp_path: Path) -> None:
    root = trust_anchor(tmp_path)
    ctx = _ctx(f"{BASE}?sslmode=verify-ca&sslrootcert={root}")
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is False


def test_verify_full_checks_chain_and_name(tmp_path: Path) -> None:
    root = trust_anchor(tmp_path)
    ctx = _ctx(f"{BASE}?sslmode=verify-full&sslrootcert={root}")
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_verification_without_a_root_certificate_is_refused() -> None:
    with pytest.raises(ConnInfoError, match="root certificate"):
        ssl_context(resolve(BASE + "?sslmode=verify-ca"))


@pytest.mark.parametrize("mode", ["allow", "prefer", "require"])
def test_a_default_root_certificate_is_used_to_verify(mode: str, tmp_path: Path) -> None:
    """libpq's documentation: with a root CA file, sslmode=require behaves as
    verify-ca. Its code (initialize_SSL) verifies in every mode but disable."""
    if sys.platform == "win32":
        default_dir = Path(os.environ["APPDATA"]) / "postgresql"
    else:
        default_dir = Path(os.environ["HOME"]) / ".postgresql"
    default_dir.mkdir(parents=True)
    (default_dir / "root.crt").write_bytes(trust_anchor(tmp_path).read_bytes())
    ctx = _ctx(f"{BASE}?sslmode={mode}")
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is False


def test_tls_version_bounds_apply() -> None:
    ctx = _ctx(BASE + "?ssl_min_protocol_version=TLSv1.3&ssl_max_protocol_version=TLSv1.3")
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3
    assert ctx.maximum_version == ssl.TLSVersion.TLSv1_3
    with pytest.raises(ConnInfoError, match="ssl_min_protocol_version"):
        resolve(BASE + "?ssl_min_protocol_version=SSLv3")


# --- passwords: .pgpass ---------------------------------------------------


def test_pgpass_matches_like_libpq(tmp_path: Path) -> None:
    passfile = tmp_path / "pgpass"
    passfile.write_text(
        "# comment\n"
        "other.example.com:*:*:*:wrong\n"
        r"db.example.com:6543:shop:alice:pa\:ss\\word"
        "\n"
        "*:*:*:alice:fallback\n",
        encoding="utf-8",
    )
    if sys.platform != "win32":
        passfile.chmod(0o600)
    assert pgpass_password(passfile, "db.example.com", 6543, "shop", "alice") == r"pa:ss\word"
    assert pgpass_password(passfile, "db.example.com", 5432, "shop", "alice") == "fallback"
    assert pgpass_password(passfile, "db.example.com", 6543, "shop", "bob") is None


def test_pgpass_is_used_when_the_dsn_has_no_password(tmp_path: Path) -> None:
    passfile = tmp_path / "pgpass"
    passfile.write_text("*:*:*:alice:from-pgpass\n", encoding="utf-8")
    if sys.platform != "win32":
        passfile.chmod(0o600)
    dsn = f"postgresql://alice@db.example.com:6543/shop?passfile={passfile}"
    assert resolve(dsn).password == "from-pgpass"
    assert resolve(BASE + f"?passfile={passfile}").password == "secret"  # the DSN wins


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")
def test_a_world_readable_pgpass_is_ignored_like_libpq(tmp_path: Path) -> None:
    passfile = tmp_path / "pgpass"
    passfile.write_text("*:*:*:alice:leaked\n", encoding="utf-8")
    passfile.chmod(0o644)
    assert pgpass_password(passfile, "h", 5432, "db", "alice") is None


# --- require_auth ---------------------------------------------------------


def test_require_auth_allow_and_deny_lists() -> None:
    allow = AuthPolicy.parse("scram-sha-256")
    assert allow.allows("scram-sha-256")
    assert not allow.allows("md5")
    assert not allow.allows("none")
    deny = AuthPolicy.parse("!password,!md5")
    assert deny.allows("scram-sha-256")
    assert deny.allows("none")
    assert not deny.allows("password")
    anything = AuthPolicy.parse(None)
    assert anything.allows("password")
    assert anything.allows("none")
