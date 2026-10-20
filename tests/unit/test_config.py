"""Connection strings: the connect timeout every connection gets, and the
host that last answered tried first."""

from __future__ import annotations

import pytest
from psycopg.conninfo import conninfo_to_dict

from deltaflow.config import prefer_host, with_connect_timeout


def test_a_connect_timeout_is_added_unless_one_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    assert conninfo_to_dict(with_connect_timeout("host=a dbname=d"))["connect_timeout"] == "10"
    assert with_connect_timeout("host=a connect_timeout=3") == "host=a connect_timeout=3"
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "4")
    assert with_connect_timeout("host=a") == "host=a"  # libpq reads the variable itself


def hosts(dsn: str) -> tuple[str, str]:
    params = conninfo_to_dict(dsn)
    return str(params.get("host")), str(params.get("port"))


def test_the_host_that_answered_moves_first() -> None:
    dsn = "host=db1,db2,db3 port=5432,5433,5434 dbname=shop target_session_attrs=read-write"
    moved = prefer_host(dsn, "db2", 5433)
    assert hosts(moved) == ("db2,db1,db3", "5433,5432,5434")
    params = conninfo_to_dict(moved)
    assert params["dbname"] == "shop"
    assert params["target_session_attrs"] == "read-write"


def test_one_port_for_every_host_stays_one_port() -> None:
    assert hosts(prefer_host("host=db1,db2 port=6543", "db2", 6543)) == ("db2,db1", "6543")


def test_a_url_with_several_hosts() -> None:
    moved = prefer_host("postgresql://u:pw@db1:5432,db2:5433/shop", "db2", 5433)
    assert hosts(moved) == ("db2,db1", "5433,5432")
    assert conninfo_to_dict(moved)["password"] == "pw"


def test_hostaddr_moves_with_its_host() -> None:
    dsn = "host=db1,db2 hostaddr=10.0.0.1,10.0.0.2 port=5432,5432"
    assert conninfo_to_dict(prefer_host(dsn, "db2", 5432))["hostaddr"] == "10.0.0.2,10.0.0.1"


def test_hosts_that_repeat_are_told_apart_by_port() -> None:
    dsn = "host=localhost,localhost port=55441,55442"
    assert hosts(prefer_host(dsn, "localhost", 55442)) == ("localhost,localhost", "55442,55441")


@pytest.mark.parametrize(
    ("dsn", "host", "port"),
    [
        ("host=db1,db2 port=5432,5433", "db1", 5432),  # already first
        ("host=db1,db2 port=5432,5433", "db9", 5432),  # not in the list
        ("host=db1,db2 port=5432,5433", "db2", 5432),  # that host, another port
        ("host=db1 port=5432", "db1", 5432),  # one host
        ("dbname=shop", "localhost", 5432),  # libpq's default host
    ],
)
def test_otherwise_the_dsn_is_unchanged(dsn: str, host: str, port: int) -> None:
    assert prefer_host(dsn, host, port) == dsn
