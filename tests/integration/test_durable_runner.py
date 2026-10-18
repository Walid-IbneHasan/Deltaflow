"""The durable runner in-process: bootstrap, clean restart (resume), and the
cases where it must refuse or re-bootstrap."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.devtools.loadgen import load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.worker import SINK_DDL, views
from deltaflow.devtools.workload import Workload
from deltaflow.planner import Count, Deltaflow, Max, PlanError, Sum
from deltaflow.replication.slot import drop_slot, slot_exists
from deltaflow.replication.wire import ReplicationConnection
from deltaflow.runtime import DurableRunner
from deltaflow.sinks import MemorySink, PostgresSink
from deltaflow.sinks.postgres import ensure_watermark_table
from tests.worker_harness import Sql, emit_marker, sink_differences, wait_caught_up


async def fresh(dsn: str) -> None:
    await seed_database(dsn, seed=5, customers=40, orders=200)
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        await sql.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
        await sql.execute(SINK_DDL)
        await ensure_watermark_table(sql)
        await sql.execute("DELETE FROM _deltaflow.watermarks")


async def drop(dsn: str, slot: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        exists = await slot_exists(sql, slot)
    if exists:
        repl = await ReplicationConnection.connect(dsn)
        try:
            await drop_slot(repl, slot)
        finally:
            await repl.close()


async def load_and_sync(dsn: str, runner: DurableRunner, sql: Sql, seed: int, txns: int) -> None:
    """Run load, then wait until the sinks catch up. If the runner fails
    meanwhile, its error is raised at once (and the runner closed), instead
    of the wait timing out with the slot still held."""
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))
    try:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await run_load(conn, Workload(seed, await load_state(conn)), txns=txns)
        caught_up = asyncio.create_task(wait_caught_up(sql, await emit_marker(sql)))
        await asyncio.wait({task, caught_up}, return_when=asyncio.FIRST_COMPLETED)
        if task.done():
            caught_up.cancel()
            task.result()  # raises the runner's error
            raise AssertionError("runner stopped before the sinks caught up")
        await caught_up
    except BaseException:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.close(final_checkpoint=False)
        raise
    stop.set()
    await task


def test_bootstrap_then_resume_after_a_clean_stop(dsn: str, tmp_path: Path) -> None:
    slot = f"it_durable_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn)
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                runner = DurableRunner(
                    views(), dsn, state_dir=tmp_path, slot=slot, publication="it_durable",
                    flush_interval=0.05, checkpoint_interval=0.2,
                )  # fmt: skip
                await runner.start()
                assert runner.counters["bootstraps"] == 1
                assert await sink_differences(sql) == []  # the snapshot, written
                await load_and_sync(dsn, runner, sql, seed=1, txns=150)
                assert await sink_differences(sql) == []
                await runner.close()

                again = DurableRunner(
                    views(), dsn, state_dir=tmp_path, slot=slot, publication="it_durable",
                    flush_interval=0.05, checkpoint_interval=0.2,
                )  # fmt: skip
                await again.start()
                assert again.counters == {"resumes": 1}
                await load_and_sync(dsn, again, sql, seed=2, txns=150)
                assert await sink_differences(sql) == []
                assert again.counters["checkpoints"] >= 1
                await again.close()
        finally:
            await drop(dsn, slot)

    run(go())


def test_changed_views_or_a_missing_slot_force_a_bootstrap(dsn: str, tmp_path: Path) -> None:
    slot = f"it_durable_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        await fresh(dsn)
        try:
            first = DurableRunner(
                views(), dsn, state_dir=tmp_path, slot=slot, publication="it_durable"
            )
            await first.start()
            await first.close()
            await drop(dsn, slot)  # e.g. an operator dropped it
            second = DurableRunner(
                views(), dsn, state_dir=tmp_path, slot=slot, publication="it_durable"
            )
            await second.start()
            assert second.counters["bootstraps"] == 1
            await second.close()

            changed = Deltaflow()
            changed.view("order_totals").source("public.orders").agg(
                orders=Count(), revenue=Sum("amount"), smallest=Max("amount"), largest=Max("amount")
            ).sink(PostgresSink("analytics.order_totals"))
            third = DurableRunner(
                changed, dsn, state_dir=tmp_path, slot=slot, publication="it_durable"
            )
            await third.start()
            assert third.counters["bootstraps"] == 1
            await third.close()
        finally:
            await drop(dsn, slot)

    run(go())


def test_a_non_durable_sink_is_refused() -> None:
    df = Deltaflow()
    df.view("v").source("public.orders").sink(MemorySink())
    with pytest.raises(PlanError, match="durable sinks"):
        DurableRunner(df, "postgresql://unused", state_dir=Path("unused"), slot="unused")


def test_resume_far_behind_the_sinks_replays_without_applying_twice(
    dsn: str, tmp_path: Path
) -> None:
    """Regression test for a bug the chaos suite found. A runner resumed from
    a checkpoint well behind its sinks flushed *during the replay* and moved
    the watermark backwards; the rest of the replay was then applied a second
    time. Here only the bootstrap checkpoint exists (huge checkpoint
    interval) while the sinks advance far past it; then the runner "crashes"."""
    slot = f"it_durable_{uuid.uuid4().hex[:8]}"

    def runner() -> DurableRunner:
        return DurableRunner(
            views(), dsn, state_dir=tmp_path, slot=slot, publication="it_durable",
            flush_interval=0.02, max_flush_transactions=5, checkpoint_interval=3600,
        )  # fmt: skip

    async def go() -> None:
        await fresh(dsn)
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                first = runner()
                await first.start()
                await load_and_sync(dsn, first, sql, seed=3, txns=200)
                assert first.checkpointed_lsn is not None
                assert first.flushed_lsn is not None
                assert first.checkpointed_lsn < first.flushed_lsn
                await first.close(final_checkpoint=False)  # a crash: no final checkpoint

                second = runner()
                await second.start()
                assert second.counters["resumes"] == 1
                await load_and_sync(dsn, second, sql, seed=4, txns=100)
                assert second.counters["replayed_flushes_skipped"] > 0
                assert await sink_differences(sql) == []
                await second.close()
        finally:
            await drop(dsn, slot)

    run(go())
