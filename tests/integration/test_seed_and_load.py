"""`make seed` / `make load` against real Postgres: the database tracks the model exactly."""

from __future__ import annotations

import psycopg

from deltaflow.aio import run
from deltaflow.devtools.loadgen import LoadStats, load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import OpKind, TableState, Workload


async def _state(dsn: str) -> TableState:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        return await load_state(conn)


async def _load(dsn: str, seed: int, txns: int) -> tuple[Workload, LoadStats]:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        workload = Workload(seed, await load_state(conn))
        stats = await run_load(conn, workload, txns=txns)
    return workload, stats


def test_seeded_database_equals_the_seed_model(dsn: str) -> None:
    expected = run(seed_database(dsn, seed=3, customers=50, orders=400))
    assert run(_state(dsn)) == expected


def test_seeding_twice_starts_from_scratch(dsn: str) -> None:
    run(seed_database(dsn, seed=3, customers=50, orders=400))
    expected = run(seed_database(dsn, seed=4, customers=20, orders=30))
    assert run(_state(dsn)) == expected


def test_database_equals_model_after_randomized_load(dsn: str) -> None:
    run(seed_database(dsn, seed=3, customers=50, orders=400))
    workload, stats = run(_load(dsn, seed=42, txns=800))

    assert stats.committed > 0
    assert stats.rolled_back > 0
    assert set(stats.statements) == set(OpKind)
    assert run(_state(dsn)) == workload.state


def test_load_is_reproducible_from_the_same_start_state(dsn: str) -> None:
    run(seed_database(dsn, seed=3, customers=50, orders=400))
    run(_load(dsn, seed=99, txns=300))
    first = run(_state(dsn))

    run(seed_database(dsn, seed=3, customers=50, orders=400))
    run(_load(dsn, seed=99, txns=300))
    assert run(_state(dsn)) == first


def test_large_values_are_stored_out_of_line(dsn: str) -> None:
    # The M1 TOAST tests are only meaningful if Postgres really toasts `bio`.
    run(seed_database(dsn, seed=3, customers=200, orders=0))
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid = 'customers'::regclass"
        ).fetchone()
        assert row is not None
        toast_rows = conn.execute(f"SELECT count(*) FROM {row[0]}").fetchone()
    assert toast_rows is not None
    assert toast_rows[0] > 0
