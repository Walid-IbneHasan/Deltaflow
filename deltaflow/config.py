"""Connection settings, read from the environment with docker-compose defaults."""

from __future__ import annotations

import os

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
