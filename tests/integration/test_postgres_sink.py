"""The Postgres sink against a real table: writes and watermark commit
together; drift between the view and the table is refused and rolled back."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.core import Delta, Frontier, Row
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks import PostgresSink, SinkError
from deltaflow.sinks.postgres import (
    describe_sink_table,
    ensure_watermark_table,
    flush,
    load_watermark,
)

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]

DDL = """
DROP SCHEMA IF EXISTS sinktest CASCADE;
CREATE SCHEMA sinktest;
CREATE TABLE sinktest.per_country (country text PRIMARY KEY, n bigint NOT NULL);
CREATE TABLE sinktest.total (n bigint NOT NULL);
DELETE FROM _deltaflow.watermarks WHERE view LIKE 'sinktest%';
"""


async def setup(sql: Sql) -> tuple[PostgresSink, PostgresSink]:
    await ensure_watermark_table(sql)
    await sql.execute(DDL)
    keyed, single = PostgresSink("sinktest.per_country"), PostgresSink("sinktest.total")
    for view, sink in (("sinktest_keyed", keyed), ("sinktest_single", single)):
        columns, key = await describe_sink_table(sql, sink.table)
        sink.bind(view=view, columns=columns, key=key, watermark=None)
    return keyed, single


async def rows(sql: Sql, table: str) -> set[tuple[Any, ...]]:
    async with sql.cursor() as cur:
        await cur.execute(f"SELECT * FROM {table}")
        return set(await cur.fetchall())


def step(sink: PostgresSink, t: int, *deltas: tuple[dict[str, Any], int]) -> None:
    sink.accept([Delta(Row(r), Lsn(t), d) for r, d in deltas])
    sink.advance(Frontier.at(Lsn(t + 1)))


def test_writes_and_watermark_commit_together(dsn: str) -> None:
    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            keyed, single = await setup(sql)
            assert (await describe_sink_table(sql, keyed.table))[1] == ("country",)
            step(keyed, 1, ({"country": "NL", "n": 1}, 1), ({"country": "DE", "n": 2}, 1))
            step(single, 1, ({"n": 3}, 1))
            assert await flush(sql, [keyed, single], Lsn(2)) == 3
            assert await rows(sql, "sinktest.per_country") == {("NL", 1), ("DE", 2)}
            assert await rows(sql, "sinktest.total") == {(3,)}
            assert await load_watermark(sql, "sinktest_keyed") == Lsn(2)

            step(keyed, 2, ({"country": "NL", "n": 1}, -1), ({"country": "NL", "n": 5}, 1))
            step(keyed, 3, ({"country": "DE", "n": 2}, -1))
            step(single, 2, ({"n": 3}, -1), ({"n": 4}, 1))
            await flush(sql, [keyed, single], Lsn(4))
            assert await rows(sql, "sinktest.per_country") == {("NL", 5)}
            assert await rows(sql, "sinktest.total") == {(4,)}
            assert await load_watermark(sql, "sinktest_single") == Lsn(4)

            # replace: the table is emptied and rebuilt (a fresh snapshot)
            step(keyed, 5, ({"country": "JP", "n": 9}, 1))
            await flush(sql, [keyed], Lsn(6), replace=True)
            assert await rows(sql, "sinktest.per_country") == {("JP", 9)}

    run(go())


@pytest.mark.parametrize(
    "drift",
    [
        "insert of a key the table already has",
        "update of a key the table does not have",
        "delete of a key the table does not have",
        "second row in a keyless table",
    ],
)
def test_drift_between_view_and_table_is_refused_and_rolled_back(dsn: str, drift: str) -> None:
    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            keyed, single = await setup(sql)
            step(keyed, 1, ({"country": "NL", "n": 1}, 1))
            step(single, 1, ({"n": 1}, 1))
            await flush(sql, [keyed, single], Lsn(2))
            sink = keyed
            match drift:
                case "insert of a key the table already has":
                    step(keyed, 2, ({"country": "NL", "n": 7}, 1))
                case "update of a key the table does not have":
                    step(keyed, 2, ({"country": "FR", "n": 1}, -1), ({"country": "FR", "n": 2}, 1))
                case "delete of a key the table does not have":
                    step(keyed, 2, ({"country": "FR", "n": 1}, -1))
                case _:
                    sink = single
                    step(single, 2, ({"n": 2}, 1))
            with pytest.raises(SinkError, match="drifted apart"):
                await flush(sql, [sink], Lsn(3))
            assert await load_watermark(sql, sink.view or "") == Lsn(2)  # rolled back
            assert await rows(sql, "sinktest.per_country") == {("NL", 1)}

    run(go())


def test_a_missing_table_is_reported(dsn: str) -> None:
    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await describe_sink_table(sql, "sinktest.nope")

    with pytest.raises(SinkError, match="does not exist"):
        run(go())
