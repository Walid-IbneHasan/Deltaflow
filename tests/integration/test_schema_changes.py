"""Source tables changing shape while their views run: ADD COLUMN is
followed (rows keep the columns the views were planned with), a change of
type stops the runner rather than mixing values of two types."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.devtools.worker import views
from deltaflow.runtime import DurableRunner
from deltaflow.state import SchemaChangedError
from tests.integration.test_chunked_bootstrap import fresh
from tests.integration.test_durable_runner import drop, load_and_sync
from tests.worker_harness import emit_marker, sink_differences, wait_caught_up


def runner(dsn: str, state: Path, slot: str) -> DurableRunner:
    return DurableRunner(
        views(), dsn, state_dir=state, slot=slot, publication="it_schema",
        flush_interval=0.05, checkpoint_interval=0.2,
    )  # fmt: skip


def test_add_column_while_views_run(dsn: str, tmp_path: Path) -> None:
    slot = f"it_schema_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn)
        r = runner(dsn, tmp_path, slot)
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                await r.start()
                stop = asyncio.Event()
                task = asyncio.create_task(r.run(stop))
                await sql.execute("ALTER TABLE public.orders ADD COLUMN channel text DEFAULT 'web'")
                await sql.execute("UPDATE public.orders SET channel = 'app', amount = amount + 1"
                                  " WHERE id % 4 = 0")  # fmt: skip
                await sql.execute(
                    "INSERT INTO public.orders (id, customer_id, status, amount, created_at,"
                    " channel) SELECT 900000 + g, 1, 'paid', g, now(), 'shop'"
                    " FROM generate_series(1, 50) g"
                )
                await wait_caught_up(sql, await emit_marker(sql))
                assert await sink_differences(sql) == []
                stop.set()
                await task
                await r.close()
                # A restart reads the new column's catalog state: still exact.
                again = runner(dsn, tmp_path, slot)
                await again.start()
                assert again.started_by == "resumed"
                await load_and_sync(dsn, again, sql, seed=5, txns=60)
                assert await sink_differences(sql) == []
                await again.close()
        finally:
            await drop(dsn, slot)
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                await sql.execute("ALTER TABLE public.orders DROP COLUMN IF EXISTS channel")

    run(go())


def test_a_changed_column_type_stops_the_runner(dsn: str, tmp_path: Path) -> None:
    slot = f"it_schema_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn)
        r = runner(dsn, tmp_path, slot)
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                await r.start()
                # A rewrite: every stored amount gets scale 4; none of it replicated.
                await sql.execute(
                    "ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric(14, 4)"
                )
                await sql.execute("UPDATE public.orders SET amount = amount + 1 WHERE id = 1")
                with pytest.raises(SchemaChangedError, match="column amount changed type"):
                    async with asyncio.timeout(30):
                        await r.run(asyncio.Event())
                await r.close(final_checkpoint=False)
        finally:
            await drop(dsn, slot)

    run(go())
