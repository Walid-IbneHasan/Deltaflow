"""Read-your-writes: once `_deltaflow.wait_for` returns true, the views
reflect the caller's committed writes, promptly even while the sources are
otherwise idle; it times out without a runner, refuses unknown views, and
any role may call it."""

from __future__ import annotations

import asyncio
import statistics
import time
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.client import await_for, wait_for
from deltaflow.devtools.worker import views
from deltaflow.runtime import DurableRunner
from tests.integration.test_chunked_bootstrap import fresh
from tests.integration.test_durable_runner import drop

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]


async def totals(sql: Sql) -> tuple[int, int]:
    """(orders in the view, orders in the table)."""
    async with sql.cursor() as cur:
        await cur.execute(
            "SELECT (SELECT orders FROM analytics.order_totals),"
            " (SELECT count(*) FROM public.orders)"
        )
        row = await cur.fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def test_after_wait_for_the_view_has_the_write(dsn: str, tmp_path: Path) -> None:
    slot = f"it_ryw_{uuid.uuid4().hex[:8]}"
    latencies: list[float] = []

    async def go() -> None:
        await fresh(dsn)
        # A long checkpoint interval: an idle watermark would otherwise
        # move only every 30 s.
        r = DurableRunner(
            views(), dsn, state_dir=tmp_path, slot=slot, publication="it_ryw",
            flush_interval=0.05, checkpoint_interval=30,
        )  # fmt: skip
        await r.start()
        stop = asyncio.Event()
        task = asyncio.create_task(r.run(stop))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                await sql.execute("DROP TABLE IF EXISTS public.it_ryw_busy")
                await sql.execute("CREATE TABLE public.it_ryw_busy (id serial, x text)")
                for i in range(15):
                    await sql.execute(
                        "INSERT INTO public.orders (id, customer_id, status, amount, created_at)"
                        " VALUES (%s, 1, 'paid', 1, now())",
                        (800_000 + i,),
                    )
                    if i % 2:
                        # Unpublished WAL after the write: the target is past
                        # the write's transaction, and nothing published follows.
                        await sql.execute("INSERT INTO public.it_ryw_busy (x) VALUES ('w')")
                    started = time.monotonic()
                    assert await await_for(sql, views=["order_totals"], timeout=10)
                    latencies.append(time.monotonic() - started)
                    seen, actual = await totals(sql)
                    assert seen == actual
                await sql.execute("DROP TABLE public.it_ryw_busy")
            assert r.counters["wakes"] >= 1
        finally:
            stop.set()
            await task
            await r.close()
            await drop(dsn, slot)

    run(go())
    print(f"wait_for latency: median {statistics.median(latencies) * 1000:.0f} ms, "
          f"max {max(latencies) * 1000:.0f} ms")  # fmt: skip
    assert statistics.median(latencies) < 1.0, latencies  # not the 30 s checkpoint interval


def test_wait_for_wakes_the_runner_when_nothing_else_commits(dsn: str, tmp_path: Path) -> None:
    """The wake-up message must reach the runner by itself. Emitted without
    a flush, it waited in the WAL buffers for the next commit (the WAL
    writer flushes only completed pages), and where nothing else committed,
    wait_for ran out its timeout."""
    slot = f"it_ryw_{uuid.uuid4().hex[:8]}"

    async def go() -> tuple[float, int]:
        await fresh(dsn)
        r = DurableRunner(
            views(), dsn, state_dir=tmp_path, slot=slot, publication="it_ryw",
            flush_interval=0.05, checkpoint_interval=30,
        )  # fmt: skip
        await r.start()
        stop = asyncio.Event()
        task = asyncio.create_task(r.run(stop))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                assert await await_for(sql, views=["order_totals"], timeout=10)
                # The runner's watermark commit is now the last WAL, and
                # nothing commits after it.
                await asyncio.sleep(1)
                started = time.monotonic()
                assert await await_for(sql, views=["order_totals"], timeout=10)
                return time.monotonic() - started, sql.info.server_version
        finally:
            stop.set()
            await task
            await r.close()
            await drop(dsn, slot)

    seconds, version = run(go())
    print(f"wait_for with nothing else committing: {seconds:.3f} s (server {version})")
    # Unflushed, the message waited for the background writer's running-xacts
    # record, up to 15 s. PostgreSQL 17 and later flush it at once; before,
    # the WAL writer flushes the page the filler completes within its sleep
    # (5 s at most).
    assert seconds < (1 if version >= 170000 else 6)


def test_without_a_runner_wait_for_times_out(dsn: str) -> None:
    async def go() -> None:
        await fresh(dsn)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(
                "INSERT INTO _deltaflow.watermarks VALUES ('it_ryw_stale', '0/1', now())"
                " ON CONFLICT (view) DO UPDATE SET lsn = '0/1'"
            )
            started = time.monotonic()
            assert not await await_for(sql, views=["it_ryw_stale"], timeout=0.3)
            # The server's clock times it (a VM's, under Docker Desktop); the
            # client's ticks in ~16 ms steps on Windows.
            assert 0.25 <= time.monotonic() - started < 3
            with pytest.raises(psycopg.errors.UndefinedObject, match="no view named nope"):
                await await_for(sql, views=["nope"], timeout=0.1)

    run(go())


def test_any_role_may_wait(dsn: str) -> None:
    """SECURITY DEFINER: a reader needs no grant on the watermark table."""
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("DROP ROLE IF EXISTS it_ryw_reader")
        admin.execute("CREATE ROLE it_ryw_reader LOGIN PASSWORD 'it_ryw_reader'")
        admin.execute(
            "INSERT INTO _deltaflow.watermarks VALUES ('it_ryw_done', 'FFFFFFFF/0', now())"
            " ON CONFLICT (view) DO UPDATE SET lsn = 'FFFFFFFF/0'"
        )
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    info.update(user="it_ryw_reader", password="it_ryw_reader")
    try:
        with psycopg.connect(**info) as reader:  # type: ignore[arg-type]
            assert wait_for(reader, views=["it_ryw_done"], timeout=1)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                reader.execute("SELECT * FROM _deltaflow.watermarks")
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("DROP ROLE IF EXISTS it_ryw_reader")
