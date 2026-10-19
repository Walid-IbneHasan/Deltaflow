"""The durable runner in-process: bootstrap, clean restart (resume), and the
cases where it must refuse or re-bootstrap."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.devtools.loadgen import load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.worker import SINK_DDL, views
from deltaflow.devtools.workload import Workload
from deltaflow.planner import Count, Deltaflow, Max, PlanError, Sum
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.slot import drop_slot, slot_confirmed_flush, slot_exists
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


async def until(check: Callable[[], bool], timeout: float = 60.0) -> None:
    async with asyncio.timeout(timeout):
        while True:
            if check():
                return
            await asyncio.sleep(0.05)


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


def test_an_idle_source_does_not_pin_wal_on_a_busy_server(dsn: str, tmp_path: Path) -> None:
    """The views' tables are quiet while another table is busy. Postgres
    sends the slot nothing (it skips transactions with no published
    changes), so a runner that acknowledges only applied transactions never
    moves the slot, and the server keeps every byte of WAL since the last
    published change: the source's disk fills. The runner must advance to
    the keepalive positions instead, through a flush and a checkpoint, so
    exactly-once still holds."""
    slot = f"it_durable_{uuid.uuid4().hex[:8]}"

    async def go() -> dict[str, Any]:
        await fresh(dsn)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute("DROP TABLE IF EXISTS public.unrelated_busy")
            await sql.execute(
                "CREATE TABLE public.unrelated_busy (id bigserial PRIMARY KEY, payload text)"
            )
            runner = DurableRunner(
                views(), dsn, state_dir=tmp_path, slot=slot, publication="it_durable",
                flush_interval=0.05, checkpoint_interval=0.3, status_interval=0.2,
            )  # fmt: skip
            await runner.start()
            stop = asyncio.Event()
            task = asyncio.create_task(runner.run(stop))
            try:
                before = await slot_confirmed_flush(sql, slot)
                for _ in range(20):  # about 2 MB of WAL, none of it published
                    await sql.execute(
                        "INSERT INTO public.unrelated_busy (payload)"
                        " SELECT repeat('x', 1000) FROM generate_series(1, 100)"
                    )
                async with sql.cursor() as cur:
                    await cur.execute("SELECT pg_current_wal_lsn()::text")
                    row = await cur.fetchone()
                assert row is not None
                busy_end = Lsn.parse(str(row[0]))
                deadline = asyncio.get_running_loop().time() + 20
                confirmed = before
                while confirmed < busy_end and asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(0.2)
                    confirmed = await slot_confirmed_flush(sql, slot)
                applied = runner.counters["transactions"]
                # A published change still arrives, and the views are exact.
                async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                    await run_load(conn, Workload(3, await load_state(conn)), txns=20)
                await wait_caught_up(sql, await emit_marker(sql))
                differences = await sink_differences(sql)
            finally:
                stop.set()
                await task
                await runner.close()
                await sql.execute("DROP TABLE public.unrelated_busy")
                await drop(dsn, slot)
        return {
            "advanced": confirmed >= busy_end,
            "confirmed": str(confirmed),
            "busy_end": str(busy_end),
            "applied_while_busy": applied,
            "idle_advances": runner.counters["idle_advances"],
            "differences": differences,
        }

    outcome = run(go())
    assert outcome["applied_while_busy"] == 0, outcome  # nothing published changed
    assert outcome["advanced"], outcome
    assert outcome["idle_advances"] >= 1, outcome
    assert outcome["differences"] == [], outcome


def test_serve_rides_out_dropped_connections_exactly_once(dsn: str, tmp_path: Path) -> None:
    """Mid-load, the walsender and then the runner's other sessions are
    terminated, as a server restart or a network failure would. serve()
    restarts the pipeline in-process, resuming from its checkpoint, and the
    sinks stay exact: nothing lost, nothing applied twice."""
    slot = f"it_durable_{uuid.uuid4().hex[:8]}"

    async def go() -> tuple[dict[str, int], list[str]]:
        await fresh(dsn)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            runner = DurableRunner(
                views(), dsn, state_dir=tmp_path, slot=slot, publication="it_durable",
                flush_interval=0.05, checkpoint_interval=0.2, status_interval=0.2,
            )  # fmt: skip
            stop = asyncio.Event()
            task = asyncio.create_task(runner.serve(stop, first_delay=0.1))
            try:
                await until(lambda: runner.counters["bootstraps"] > 0)
                async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                    load = asyncio.create_task(
                        run_load(conn, Workload(4, await load_state(conn)), txns=400)
                    )
                    await asyncio.sleep(0.5)
                    await sql.execute(
                        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots"
                        " WHERE slot_name = %s AND active_pid IS NOT NULL",
                        (slot,),
                    )
                    await until(lambda: runner.counters["restarts"] > 0)
                    await asyncio.sleep(1.0)
                    # the sink connection (and the slot monitor's)
                    await sql.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                        " WHERE application_name = 'deltaflow' AND backend_type = 'client backend'"
                    )
                    await load
                caught_up = asyncio.create_task(wait_caught_up(sql, await emit_marker(sql)))
                await asyncio.wait({task, caught_up}, return_when=asyncio.FIRST_COMPLETED)
                if task.done():
                    caught_up.cancel()
                    task.result()
                await caught_up
                differences = await sink_differences(sql)
            finally:
                stop.set()
                await asyncio.gather(task, return_exceptions=True)
                await runner.close()
                await drop(dsn, slot)
        return dict(runner.counters), differences

    counters, differences = run(go())
    assert counters["bootstraps"] == 1, counters
    assert counters["restarts"] >= 2, counters
    assert counters["resumes"] >= 2, counters
    assert differences == []
