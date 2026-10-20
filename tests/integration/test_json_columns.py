"""json, jsonb and array columns through the whole pipeline: a chunked
bootstrap (which reads them as text), the stream, a view grouping by a
jsonb value, and sinks of the same types. The sinks must equal their SQL
after random changes, across a resume."""

from __future__ import annotations

import asyncio
import random
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.core import Row
from deltaflow.core.jsontext import JsonError, jsonb_in
from deltaflow.core.values import UnsupportedValueError, get
from deltaflow.planner import Count, Deltaflow
from deltaflow.replication.lsn import Lsn
from deltaflow.runtime import DurableRunner
from deltaflow.sinks import PostgresSink
from deltaflow.sinks.postgres import ensure_watermark_table
from tests.integration.test_durable_runner import drop, until
from tests.integration.test_json_values import document
from tests.sql_oracle import first_difference, query_multiset

DDL = """
DROP TABLE IF EXISTS public.it_docs;
DROP SCHEMA IF EXISTS it_docs_out CASCADE;
CREATE SCHEMA it_docs_out;
CREATE TABLE public.it_docs (
    id int PRIMARY KEY, doc jsonb, raw json, tags text[], nums int[], grid int[][], items jsonb[]
);
CREATE TABLE it_docs_out.copy (
    id int PRIMARY KEY, doc jsonb, raw json, tags text[], nums int[], grid int[][], items jsonb[]
);
CREATE TABLE it_docs_out.by_kind (kind jsonb PRIMARY KEY, n bigint NOT NULL);
"""
EXPECTED = {
    "it_docs_out.copy": "SELECT * FROM public.it_docs",
    "it_docs_out.by_kind": "SELECT doc -> 'kind' AS kind, count(*) AS n FROM public.it_docs"
    " WHERE doc -> 'kind' IS NOT NULL GROUP BY 1",
}
KINDS = ['"a"', '"b"', "1", "1.0", "1.00", "[1, 2]", '{"x": 1}', "null"]


def kind_of(row: Row) -> dict[str, Any]:
    return {"kind": None if row["doc"] is None else get(row["doc"], "kind")}


def has_kind(row: Row) -> bool:
    return row["kind"] is not None


def views() -> Deltaflow:
    df = Deltaflow()
    df.view("docs_copy").source("public.it_docs").sink(PostgresSink("it_docs_out.copy"))
    (
        df.view("docs_by_kind")
        .source("public.it_docs")
        .map(kind_of)
        .filter(has_kind)
        .group_by("kind")
        .agg(n=Count())
        .sink(PostgresSink("it_docs_out.by_kind"))
    )
    return df


def values(rng: random.Random) -> tuple[Any, ...]:
    while True:
        text = document(rng)
        try:
            jsonb_in(text)  # the jsonb column needs a jsonb
            break
        except JsonError:
            continue
    doc = None if rng.random() < 0.1 else text
    if doc is not None and rng.random() < 0.7:
        doc = f'{{"kind": {rng.choice(KINDS)}, "body": {doc}}}'
    tags = [rng.choice(["", "a", "b c", 'q"', "x,y", "{}", None]) for _ in range(rng.randint(0, 3))]
    nums = [rng.choice([1, -2, None, 2_000_000_000]) for _ in range(rng.randint(0, 4))]
    width = rng.randint(1, 3)
    grid = [[rng.randint(0, 9) for _ in range(width)] for _ in range(rng.randint(1, 2))]
    items = [jsonb_in(f'{{"i": {i}}}') for i in range(rng.randint(0, 2))]
    return doc, document(rng), tags, nums, grid, items


async def change(
    conn: psycopg.AsyncConnection[tuple[Any, ...]], rng: random.Random, ids: set[int]
) -> None:
    roll = rng.random()
    if roll < 0.5 or not ids:
        new = max(ids, default=0) + 1
        ids.add(new)
        await conn.execute(
            "INSERT INTO public.it_docs VALUES (%s, %s::jsonb, %s::json, %s, %s, %s, %s::jsonb[])",
            (new, *values(rng)),
        )
    elif roll < 0.85:
        await conn.execute(
            "UPDATE public.it_docs SET doc = %s::jsonb, raw = %s::json, tags = %s, nums = %s,"
            " grid = %s, items = %s::jsonb[] WHERE id = %s",
            (*values(rng), rng.choice(sorted(ids))),
        )
    else:
        gone = rng.choice(sorted(ids))
        ids.discard(gone)
        await conn.execute("DELETE FROM public.it_docs WHERE id = %s", (gone,))


async def differences(sql: psycopg.AsyncConnection[tuple[Any, ...]]) -> list[str]:
    problems = []
    for table, query in EXPECTED.items():
        actual = await query_multiset(sql, f"SELECT * FROM {table}")
        expected = await query_multiset(sql, query)
        if actual != expected:
            problems.append(f"{table}: {first_difference(actual, expected)}")
    return problems


async def caught_up(runner: DurableRunner, sql: psycopg.AsyncConnection[tuple[Any, ...]]) -> None:
    async with sql.cursor() as cur:
        await cur.execute("SELECT pg_current_wal_lsn()::text")
        row = await cur.fetchone()
    assert row is not None
    target = Lsn.parse(str(row[0]))
    await until(lambda: runner.flushed_lsn is not None and runner.flushed_lsn >= target)


def test_json_jsonb_and_array_columns_stay_exact(dsn: str, tmp_path: Path) -> None:
    slot = f"it_json_{uuid.uuid4().hex[:8]}"
    rng = random.Random(7)

    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(DDL)
            await ensure_watermark_table(sql)
            ids: set[int] = set()
            for _ in range(120):  # rows the bootstrap reads, as text
                await change(sql, rng, ids)
            try:
                for _round in range(2):  # the second round resumes
                    runner = DurableRunner(
                        views(), dsn, state_dir=tmp_path, slot=slot, publication="it_json",
                        flush_interval=0.05, checkpoint_interval=0.2, snapshot_chunk_rows=25,
                    )  # fmt: skip
                    await runner.start()
                    stop = asyncio.Event()
                    task = asyncio.create_task(runner.run(stop))
                    for _ in range(150):
                        await change(sql, rng, ids)
                    await sql.execute("SELECT pg_logical_emit_message(true, 'it', 'sync')")
                    await caught_up(runner, sql)
                    assert await differences(sql) == []
                    stop.set()
                    await task
                    await runner.close()
            finally:
                await drop(dsn, slot)

    run(go())


def test_an_array_with_another_lower_bound_is_refused(dsn: str, tmp_path: Path) -> None:
    slot = f"it_json_{uuid.uuid4().hex[:8]}"

    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(DDL)
            await ensure_watermark_table(sql)
            await sql.execute("INSERT INTO public.it_docs (id, nums) VALUES (1, '[0:1]={1,2}')")
        runner = DurableRunner(views(), dsn, state_dir=tmp_path, slot=slot, publication="it_json")
        try:
            with pytest.raises(UnsupportedValueError, match="lower bound other than 1"):
                await runner.start()
        finally:
            await runner.close(final_checkpoint=False)
            await drop(dsn, slot)

    run(go())
