"""Chunked bootstrap (decision 0009) against the real server: exact under
concurrent load, VACUUM keeps working during it, and an interrupted load
continues where it stopped."""

from __future__ import annotations

import asyncio
import re
import uuid
from pathlib import Path
from typing import Literal

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.devtools.loadgen import load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.worker import SINK_DDL, views
from deltaflow.devtools.workload import Workload
from deltaflow.replication.slot import slot_confirmed_flush
from deltaflow.runtime import DurableRunner
from deltaflow.sinks.postgres import ensure_watermark_table
from tests.integration.test_durable_runner import drop, load_and_sync, until
from tests.worker_harness import sink_differences

PUBLICATION = "it_chunked"


async def fresh(dsn: str, *, customers: int = 300, orders: int = 3_000) -> None:
    await seed_database(dsn, seed=11, customers=customers, orders=orders)
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        await sql.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
        await sql.execute(SINK_DDL)
        await ensure_watermark_table(sql)
        await sql.execute("DELETE FROM _deltaflow.watermarks")


def runner(
    dsn: str,
    state: Path,
    slot: str,
    *,
    snapshot: Literal["chunked", "single"] = "chunked",
    snapshot_chunk_rows: int = 97,
) -> DurableRunner:
    return DurableRunner(
        views(),
        dsn,
        state_dir=state,
        slot=slot,
        publication=PUBLICATION,
        flush_interval=0.05,
        checkpoint_interval=0.2,
        snapshot=snapshot,
        snapshot_chunk_rows=snapshot_chunk_rows,
    )


def loaded(r: DurableRunner, counter: str = "snapshot_rows") -> int:
    """The runner's engine's load counter (0 before it has an engine)."""
    return 0 if r.engine is None else r.engine.counters[counter]


def test_a_chunked_bootstrap_under_write_load_matches_postgres(dsn: str, tmp_path: Path) -> None:
    """Writes land throughout the load: in pages already read, in pages
    still ahead, and in the one being read."""
    slot = f"it_chunked_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn)
        r = runner(dsn, tmp_path, slot)
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                load = asyncio.create_task(
                    run_load(conn, Workload(3, await load_state(conn)), txns=400)
                )
                await r.start()
                await load
            assert r.started_by == "bootstrapped"
            assert r.counters["bootstraps"] == 1
            assert r.engine is not None
            assert r.engine.counters["snapshot_chunks"] > 30
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                await load_and_sync(dsn, r, sql, seed=4, txns=100)
                assert await sink_differences(sql) == []
            await r.close()
        finally:
            await drop(dsn, slot)

    run(go())


async def vacuum_leaves_dead(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> int:
    """Rows VACUUM found dead but could not remove (an older snapshot may
    still see them), from VACUUM VERBOSE's report."""
    notices: list[str] = []
    conn.add_notice_handler(lambda d: notices.append(d.message_primary or ""))
    await conn.execute("VACUUM (VERBOSE) public.it_vacuum_probe")
    report = "\n".join(notices)
    found = re.search(r"(\d+) are dead but not yet removable", report)
    assert found is not None, report
    return int(found.group(1))


@pytest.mark.parametrize(("snapshot", "held"), [("chunked", False), ("single", True)])
def test_vacuum_removes_dead_rows_while_a_chunked_load_runs(
    dsn: str, tmp_path: Path, snapshot: Literal["chunked", "single"], held: bool
) -> None:
    """Rows deleted while the load runs. A chunked load holds no snapshot
    between chunks, so VACUUM removes them. The single-snapshot bootstrap
    holds one for the whole load, and they stay: the test can tell."""
    slot = f"it_chunked_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn, customers=400, orders=20_000)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await conn.execute("DROP TABLE IF EXISTS public.it_vacuum_probe")
            await conn.execute(
                "CREATE TABLE public.it_vacuum_probe AS"
                " SELECT g AS id FROM generate_series(1, 500) g"
            )
            r = runner(dsn, tmp_path, slot, snapshot=snapshot, snapshot_chunk_rows=50)
            loading = asyncio.create_task(r.start())
            try:
                # Into the load: the first rows are in, most are still ahead.
                await until(lambda: loaded(r) > 0)
                await conn.execute("DELETE FROM public.it_vacuum_probe")
                # A chunk being read when the rows were deleted may still see
                # them; once two more chunks are done, none can.
                chunks = loaded(r, "snapshot_chunks")
                await until(lambda: loaded(r, "snapshot_chunks") >= chunks + 2)
                dead = await vacuum_leaves_dead(conn)
                assert r.engine is not None
                assert not r.engine.live, "the load finished too early"
                assert (dead > 0) is held, dead
                await loading
                async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                    assert await sink_differences(sql) == []
            finally:
                if not loading.done():
                    loading.cancel()
                    await asyncio.gather(loading, return_exceptions=True)
                await r.close(final_checkpoint=False)
                await conn.execute("DROP TABLE IF EXISTS public.it_vacuum_probe")
                await drop(dsn, slot)

    run(go())


def test_a_stopped_load_resumes_from_its_checkpoint(dsn: str, tmp_path: Path) -> None:
    """Stopped halfway, the load is checkpointed and the slot acknowledged
    up to it. The next start continues with the next chunk: the rows read
    before are not read again. (Killed mid-load under write load: the
    chaos suite, load:after_chunk.)"""
    slot = f"it_chunked_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn, orders=6_000)
        first = runner(dsn, tmp_path, slot, snapshot_chunk_rows=200)
        stop = asyncio.Event()
        loading = asyncio.create_task(first.start(stop))
        try:
            await until(lambda: loaded(first) >= 2_000)
            stop.set()
            await loading
            assert first.started_by == "loading"
            read_before = loaded(first)
            await first.close()
            assert first.checkpointed_lsn is not None
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                assert await slot_confirmed_flush(conn, slot) == first.checkpointed_lsn

            second = runner(dsn, tmp_path, slot, snapshot_chunk_rows=200)
            await second.start()
            assert second.started_by == "bootstrapped"
            assert second.counters["load_resumes"] == 1
            assert second.engine is not None
            # every customer and order read once in all: none read again
            assert read_before + second.engine.counters["snapshot_rows"] == 300 + 6_000
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                assert await sink_differences(sql) == []
                await load_and_sync(dsn, second, sql, seed=8, txns=100)
                assert await sink_differences(sql) == []
            await second.close()
        finally:
            if not loading.done():
                loading.cancel()
            await drop(dsn, slot)

    run(go())
