"""Connection settings, read from the environment with docker-compose defaults."""

from __future__ import annotations

import os

from psycopg.conninfo import conninfo_to_dict, make_conninfo

# Host port 55432 rather than 5432 so the harness does not collide with a
# locally installed Postgres. Must match docker-compose.yml.
DEFAULT_DSN = "postgresql://deltaflow:deltaflow@localhost:55432/deltaflow"
DEFAULT_TEST_DSN = "postgresql://deltaflow:deltaflow@localhost:55432/deltaflow_test"


def pg_dsn() -> str:
    """DSN of the source database used by `make seed` / `make load`."""
    return os.environ.get("DELTAFLOW_PG_DSN", DEFAULT_DSN)


def test_pg_dsn() -> str:
    """DSN of the scratch database the integration tests are free to wipe."""
    return os.environ.get("DELTAFLOW_TEST_PG_DSN", DEFAULT_TEST_DSN)


#: seconds; libpq's default is to wait for ever
DEFAULT_CONNECT_TIMEOUT = 10


def with_connect_timeout(dsn: str, seconds: int = DEFAULT_CONNECT_TIMEOUT) -> str:
    """`dsn` with a connect_timeout, unless it (or PGCONNECT_TIMEOUT) sets
    one. libpq waits for ever on a host that accepts the TCP connection and
    then says nothing, and with several hosts (a failover setup) it would
    then never move on to the next."""
    if os.environ.get("PGCONNECT_TIMEOUT") or "connect_timeout" in conninfo_to_dict(dsn):
        return dsn
    return make_conninfo(dsn, connect_timeout=seconds)
