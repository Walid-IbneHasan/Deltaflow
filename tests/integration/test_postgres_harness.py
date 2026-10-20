"""The compose Postgres is configured the way logical replication needs it."""

from __future__ import annotations

import psycopg


def _setting(dsn: str, name: str) -> str:
    with psycopg.connect(dsn) as conn:
        row = conn.execute("SELECT current_setting(%s)", (name,)).fetchone()
    assert row is not None
    return str(row[0])


def test_server_is_postgres_15_or_newer(dsn: str) -> None:
    """The oldest version supported: pg_publication_tables has the row
    filter and column list columns the source checks read (snapshot.py)."""
    assert int(_setting(dsn, "server_version_num")) >= 150000


def test_wal_level_is_logical(dsn: str) -> None:
    assert _setting(dsn, "wal_level") == "logical"


def test_replication_slots_and_senders_are_available(dsn: str) -> None:
    assert int(_setting(dsn, "max_replication_slots")) >= 4
    assert int(_setting(dsn, "max_wal_senders")) >= 4


def test_connecting_role_may_replicate(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT rolreplication FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
    assert row == (True,)
