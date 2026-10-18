"""Shared by the durable-runner integration tests and the chaos suite: wait
until the worker's sinks have caught up, and compare them with SQL."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import psycopg

from deltaflow.devtools.worker import VIEW_SQL
from deltaflow.replication.lsn import Lsn
from tests.sql_oracle import first_difference, query_multiset

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]

VIEWS = ("orders_per_customer", "order_details", "order_totals", "customer_statuses")
KEYS = {
    "analytics.orders_per_customer": "country",
    "analytics.order_details": "order_id",
    "analytics.customer_statuses": "customer_id, status",
}


async def emit_marker(sql: Sql) -> Lsn:
    """A transaction every view sees: once a view's watermark is past this
    LSN, its sink reflects everything committed before it."""
    async with sql.cursor() as cur:
        await cur.execute("SELECT pg_logical_emit_message(true, 'deltaflow.test', 'sync')::text")
        row = await cur.fetchone()
    assert row is not None
    return Lsn.parse(str(row[0]))


async def watermarks(sql: Sql) -> dict[str, Lsn]:
    async with sql.cursor() as cur:
        await cur.execute(
            "SELECT view, lsn::text FROM _deltaflow.watermarks WHERE view = ANY(%s)", (list(VIEWS),)
        )
        return {str(v): Lsn.parse(str(lsn)) for v, lsn in await cur.fetchall()}


async def wait_caught_up(sql: Sql, lsn: Lsn, timeout: float = 120.0) -> float:
    """Seconds until every view's watermark is past `lsn`."""
    started = time.monotonic()
    while True:
        marks = await watermarks(sql)
        if len(marks) == len(VIEWS) and min(marks.values()) > lsn:
            return time.monotonic() - started
        if time.monotonic() - started > timeout:
            raise AssertionError(f"sinks did not catch up to {lsn} in {timeout}s: {marks}")
        await asyncio.sleep(0.1)


async def sink_differences(sql: Sql) -> list[str]:
    """Every way the sink tables differ from a full recompute, plus any
    duplicate keys and any keyless table without exactly one row."""
    problems = []
    for table, query in VIEW_SQL.items():
        actual = await query_multiset(sql, f"SELECT * FROM {table}")
        expected = await query_multiset(sql, query)
        if actual != expected:
            problems.append(f"{table}: {first_difference(actual, expected)}")
        if any(n > 1 for _, n in actual.items()):
            problems.append(f"{table}: duplicate rows")
        key = KEYS.get(table)
        if key is not None:
            async with sql.cursor() as cur:
                await cur.execute(
                    f"SELECT {key}, count(*) FROM {table} GROUP BY {key} HAVING count(*) > 1"
                )
                if dupes := await cur.fetchall():
                    problems.append(f"{table}: duplicate keys {dupes[:3]}")
        elif actual.size() != 1:
            problems.append(f"{table}: {actual.size()} rows, expected exactly 1")
    return problems
