"""Partitioned tables as sources: published via their root, read through
it, exact under rows moving between partitions; a partition dropped or
detached (its rows gone without a single DELETE in the stream) stops the
runner instead of leaving them in the views."""

from __future__ import annotations

import asyncio
import random
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.planner import Count, Deltaflow, Sum
from deltaflow.planner.dsl import PlanError
from deltaflow.runtime import DurableRunner
from deltaflow.runtime.durable import is_transient
from deltaflow.runtime.snapshot import PartitionsChangedError, UnsupportedSourceError
from deltaflow.sinks import PostgresSink
from deltaflow.sinks.postgres import ensure_watermark_table
from tests.integration.test_durable_runner import drop, until
from tests.sql_oracle import first_difference, query_multiset

DDL = """
DROP TABLE IF EXISTS public.it_sales CASCADE;
DROP TABLE IF EXISTS public.it_sales_ap;  -- left over once detached
DROP SCHEMA IF EXISTS it_sales_out CASCADE;
CREATE SCHEMA it_sales_out;
CREATE TABLE public.it_sales (
    id int, region text, amount numeric(10, 2), PRIMARY KEY (id, region)
) PARTITION BY LIST (region);
CREATE TABLE public.it_sales_eu PARTITION OF public.it_sales FOR VALUES IN ('eu');
CREATE TABLE public.it_sales_us PARTITION OF public.it_sales FOR VALUES IN ('us');
CREATE TABLE public.it_sales_ap PARTITION OF public.it_sales FOR VALUES IN ('ap');
CREATE TABLE it_sales_out.by_region (region text PRIMARY KEY, n bigint, total numeric);
"""
EXPECTED = "SELECT region, count(*) AS n, sum(amount) AS total FROM public.it_sales GROUP BY region"
REGIONS = ["eu", "us", "ap"]


def views() -> Deltaflow:
    df = Deltaflow()
    (
        df.view("sales_by_region")
        .source("public.it_sales")
        .group_by("region")
        .agg(n=Count(), total=Sum("amount"))
        .sink(PostgresSink("it_sales_out.by_region"))
    )
    return df


def runner(dsn: str, state: Path, slot: str, registry: Deltaflow | None = None) -> DurableRunner:
    return DurableRunner(
        registry or views(), dsn, state_dir=state, slot=slot, publication="it_sales",
        flush_interval=0.05, checkpoint_interval=0.2, snapshot_chunk_rows=30,
    )  # fmt: skip


async def change(sql: psycopg.AsyncConnection[tuple[Any, ...]], rng: random.Random) -> None:
    roll = rng.random()
    i = rng.randint(1, 200)
    if roll < 0.5:
        await sql.execute(
            "INSERT INTO public.it_sales VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (i, rng.choice(REGIONS), rng.randint(1, 999)),
        )
    elif roll < 0.8:  # may move the row to another partition
        await sql.execute(
            "UPDATE public.it_sales SET region = %s, amount = amount + 1"
            " WHERE id = %s AND region = %s AND NOT EXISTS (SELECT 1 FROM public.it_sales s"
            " WHERE s.id = %s AND s.region = %s)",
            (r := rng.choice(REGIONS), i, rng.choice(REGIONS), i, r),
        )
    else:
        await sql.execute("DELETE FROM public.it_sales WHERE id = %s", (i,))


async def matches(sql: psycopg.AsyncConnection[tuple[Any, ...]]) -> str | None:
    actual = await query_multiset(sql, "SELECT * FROM it_sales_out.by_region")
    expected = await query_multiset(sql, EXPECTED)
    return None if actual == expected else first_difference(actual, expected)


async def caught_up(r: DurableRunner, sql: psycopg.AsyncConnection[tuple[Any, ...]]) -> None:
    from deltaflow.replication.lsn import Lsn  # noqa: PLC0415

    async with sql.cursor() as cur:
        await cur.execute("SELECT pg_logical_emit_message(true, 'it', 'sync')::text")
        row = await cur.fetchone()
    assert row is not None
    target = Lsn.parse(str(row[0]))
    await until(lambda: r.flushed_lsn is not None and r.flushed_lsn > target)


def test_a_partitioned_source_is_exact_as_rows_move_between_partitions(
    dsn: str, tmp_path: Path
) -> None:
    slot = f"it_sales_{uuid.uuid4().hex[:8]}"
    rng = random.Random(3)

    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(DDL)
            await ensure_watermark_table(sql)
            for _ in range(150):
                await change(sql, rng)
            r = runner(dsn, tmp_path, slot)
            try:
                await r.start()
                assert await matches(sql) is None
                stop = asyncio.Event()
                task = asyncio.create_task(r.run(stop))
                for _ in range(300):
                    await change(sql, rng)
                await sql.execute("TRUNCATE public.it_sales")  # the root: replicated
                for _ in range(100):
                    await change(sql, rng)
                await caught_up(r, sql)
                assert await matches(sql) is None
                stop.set()
                await task
                await r.close()
            finally:
                await drop(dsn, slot)

    run(go())


@pytest.mark.parametrize(
    "removal",
    [
        "ALTER TABLE public.it_sales DETACH PARTITION public.it_sales_ap",
        "DROP TABLE public.it_sales_ap",
    ],
    ids=["detach", "drop"],
)
def test_a_removed_partition_stops_the_runner_and_a_restart_bootstraps(
    dsn: str, tmp_path: Path, removal: str
) -> None:
    slot = f"it_sales_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(DDL)
            await ensure_watermark_table(sql)
            await sql.execute("INSERT INTO public.it_sales VALUES (1, 'ap', 5), (2, 'eu', 7)")
            r = runner(dsn, tmp_path, slot)
            try:
                await r.start()
                await sql.execute(removal)
                with pytest.raises(
                    PartitionsChangedError, match=r"it_sales_ap .*no longer a partition"
                ):
                    async with asyncio.timeout(30):
                        await r.run(asyncio.Event())
                await r.close(final_checkpoint=False)
                # Started again (serve() does it by itself: the error is one a
                # restart outlives), the checkpoint no longer describes the
                # table, so the runner bootstraps: exact again.
                again = runner(dsn, tmp_path, slot)
                await again.start()
                assert again.counters["bootstraps"] == 1
                assert await matches(sql) is None
                await again.close()
            finally:
                await drop(dsn, slot)

    run(go())


def test_a_partition_itself_is_refused_as_a_source(dsn: str, tmp_path: Path) -> None:
    slot = f"it_sales_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(DDL)
            await ensure_watermark_table(sql)
            await sql.execute(
                "CREATE TABLE it_sales_out.eu_copy"
                " (id int, region text, amount numeric, PRIMARY KEY (id, region))"
            )
        df = Deltaflow()
        df.view("eu_copy").source("public.it_sales_eu").sink(PostgresSink("it_sales_out.eu_copy"))
        r = runner(dsn, tmp_path, slot, df)
        try:
            with pytest.raises(UnsupportedSourceError, match=r"partition of public\.it_sales"):
                await r.start()
        finally:
            await r.close(final_checkpoint=False)
            await drop(dsn, slot)

    run(go())
    assert issubclass(UnsupportedSourceError, PlanError)
    assert is_transient(PartitionsChangedError("x"))
