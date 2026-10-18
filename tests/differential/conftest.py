from __future__ import annotations

import os
import secrets

import psycopg
import pytest

from deltaflow.config import test_pg_dsn


@pytest.fixture
def dsn() -> str:
    target = test_pg_dsn()
    try:
        with psycopg.connect(target, connect_timeout=5):
            pass
    except psycopg.OperationalError as exc:
        pytest.fail(f"Postgres not reachable at {target!r} - run `make up` first.\n{exc}")
    return target


@pytest.fixture
def workload_seed() -> int:
    """Random per run unless DELTAFLOW_SEED is set; always printed (pytest shows
    captured output for failing tests)."""
    raw = os.environ.get("DELTAFLOW_SEED")
    seed = int(raw) if raw else secrets.randbelow(2**32)
    print(f"differential seed={seed}  replay: DELTAFLOW_SEED={seed} make differential")
    return seed
