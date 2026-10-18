from __future__ import annotations

import psycopg
import pytest

from deltaflow.config import test_pg_dsn


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Everything in this directory talks to the docker compose Postgres.
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def dsn() -> str:
    """DSN of the scratch test database; fails (never skips) if it is down."""
    target = test_pg_dsn()
    try:
        with psycopg.connect(target, connect_timeout=5):
            pass
    except psycopg.OperationalError as exc:
        pytest.fail(f"Postgres not reachable at {target!r} - run `make up` first.\n{exc}")
    return target
