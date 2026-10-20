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


def prefer_host(dsn: str, host: str, port: int) -> str:
    """`dsn` with `host`:`port`, the server that just answered, first in its
    host list (hostaddr and port lists move with it). libpq tries the hosts
    in order, so after a failover every new connection would otherwise wait
    connect_timeout on the old primary before reaching the new one.
    Unchanged when that server is already first, or not in the list."""
    params = conninfo_to_dict(dsn)
    hosts = str(params.get("host") or "").split(",")
    if len(hosts) < 2:
        return dsn
    ports = str(params.get("port") or "").split(",")
    if len(ports) == 1:
        ports *= len(hosts)  # one port for every host
    if len(ports) != len(hosts):
        return dsn
    found = [
        i for i, (h, p) in enumerate(zip(hosts, ports, strict=True))
        if h == host and (p or "5432") == str(port)
    ]  # fmt: skip
    if not found or found[0] == 0:
        return dsn
    order = [found[0], *(i for i in range(len(hosts)) if i != found[0])]
    moved: dict[str, str] = {"host": ",".join(hosts[i] for i in order)}
    if params.get("port") and "," in str(params["port"]):
        moved["port"] = ",".join(ports[i] for i in order)
    addrs = str(params.get("hostaddr") or "").split(",")
    if len(addrs) == len(hosts):
        moved["hostaddr"] = ",".join(addrs[i] for i in order)
    return make_conninfo(dsn, **moved)
