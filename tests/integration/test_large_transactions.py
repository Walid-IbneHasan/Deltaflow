"""Large transactions against the real server: applied in parts, sinks that
spill, and a stop inside one. The views stay exactly their SQL."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import psycopg

from deltaflow.aio import run
from deltaflow.devtools.worker import views
from deltaflow.runtime import DurableRunner
from tests.integration.test_chunked_bootstrap import fresh
from tests.integration.test_durable_runner import drop, until
from tests.worker_harness import emit_marker, sink_differences, wait_caught_up

BIG = [
    "UPDATE public.orders SET amount = amount + 1",
    "INSERT INTO public.orders (id, customer_id, status, amount, note, created_at)"
    " SELECT 100000 + g, 1 + g % 300, 'paid', g, NULL, now() FROM generate_series(1, 2500) g",
    "DELETE FROM public.orders WHERE id % 3 = 0",
    "UPDATE public.customers SET country = 'NL' WHERE id % 2 = 0",
]


def runner(dsn: str, state: Path, slot: str) -> DurableRunner:
    return DurableRunner(
        views(), dsn, state_dir=state, slot=slot, publication="it_large",
        flush_interval=0.05, checkpoint_interval=0.2,
        max_transaction_changes=50, sink_spill_after=20,
    )  # fmt: skip


def test_large_transactions_come_in_parts_and_the_views_stay_exact(
    dsn: str, tmp_path: Path
) -> None:
    slot = f"it_large_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn, orders=3_000)
        r = runner(dsn, tmp_path, slot)
        await r.start()
        stop = asyncio.Event()
        task = asyncio.create_task(r.run(stop))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                for statement in BIG:
                    await sql.execute(statement)
                await wait_caught_up(sql, await emit_marker(sql))
                assert await sink_differences(sql) == []
            assert r.counters["transaction_parts"] > 100
            for sink in r.sinks:
                assert sink.buffered <= 20  # the sinks spilled instead of buffering
        finally:
            stop.set()
            await task
            await r.close()
            await drop(dsn, slot)

    run(go())


def test_a_stop_inside_a_large_transaction_replays_it_whole(dsn: str, tmp_path: Path) -> None:
    """Stopped with half a transaction applied, the runner takes no final
    checkpoint (the state is not at a boundary). The next start resumes
    from the last checkpoint and applies the transaction again, whole."""
    slot = f"it_large_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn, orders=6_000)
        first = runner(dsn, tmp_path, slot)
        await first.start()
        stop = asyncio.Event()
        task = asyncio.create_task(first.run(stop))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                await sql.execute("UPDATE public.orders SET amount = amount * 2")
                await until(lambda: first.engine is not None and first.engine.partial)
                stop.set()
                await task
                assert first.engine is not None
                assert first.engine.partial
                checkpointed = first.checkpointed_lsn
                await first.close()  # no final checkpoint: mid-transaction
                assert first.checkpointed_lsn == checkpointed

                second = runner(dsn, tmp_path, slot)
                await second.start()
                assert second.started_by == "resumed"
                again = asyncio.Event()
                task = asyncio.create_task(second.run(again))
                await wait_caught_up(sql, await emit_marker(sql))
                assert await sink_differences(sql) == []
                again.set()
                await task
                await second.close()
        finally:
            stop.set()
            if not task.done():
                task.cancel()
            await drop(dsn, slot)

    run(go())
