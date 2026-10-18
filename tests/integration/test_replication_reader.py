"""The replication reader against a real Postgres: one behaviour per test."""

from __future__ import annotations

import asyncio
import random
import string
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from decimal import Decimal

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.devtools.seed import seed_database
from deltaflow.replication.changes import (
    RowDelete,
    RowInsert,
    RowUpdate,
    TableTruncate,
    Transaction,
)
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST
from deltaflow.replication.slot import (
    create_publication,
    create_slot,
    drop_slot,
    slot_confirmed_flush,
)
from deltaflow.replication.stream import ReplicationStream
from deltaflow.replication.wire import PgError, ReplicationConnection

PUBLICATION = "deltaflow_it"
TABLES = ("public.customers", "public.orders")
# Incompressible, so Postgres must store it out of line.
BIG = "".join(random.Random(0).choices(string.ascii_letters, k=20_000))

type Sql = psycopg.AsyncConnection[tuple[object, ...]]


@asynccontextmanager
async def replicating(
    dsn: str, *, slot: str | None = None, temporary: bool = True
) -> AsyncIterator[tuple[Sql, ReplicationStream]]:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        repl = await ReplicationConnection.connect(dsn)
        name = slot or f"it_{uuid.uuid4().hex[:12]}"
        if temporary or slot is None:
            await create_slot(repl, name, temporary=True)
        stream = ReplicationStream(repl, slot=name, publications=[PUBLICATION], status_interval=0.2)
        await stream.start()
        try:
            yield sql, stream
        finally:
            await stream.close()


async def fresh(dsn: str) -> None:
    await seed_database(dsn, seed=1, customers=5, orders=10)
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        await create_publication(sql, PUBLICATION, TABLES)


async def next_txn(stream: ReplicationStream, timeout: float = 10.0) -> Transaction:
    async with asyncio.timeout(timeout):
        async for event in stream:
            if isinstance(event, Transaction):
                return event
    raise AssertionError("stream ended")


def scenario(body: Callable[[Sql, ReplicationStream], Awaitable[None]]) -> Callable[[str], None]:
    def test(dsn: str) -> None:
        async def go() -> None:
            await fresh(dsn)
            async with replicating(dsn) as (sql, stream):
                await body(sql, stream)

        run(go())

    test.__name__ = body.__name__
    test.__doc__ = body.__doc__
    return test


@scenario
async def test_insert_is_typed(sql: Sql, stream: ReplicationStream) -> None:
    await sql.execute(
        "INSERT INTO orders VALUES (1000, 1, 'paid', 12.30, NULL, '2026-10-18 12:00:00+02')"
    )
    txn = await next_txn(stream)
    [change] = txn.changes
    assert isinstance(change, RowInsert)
    assert change.relation.qualified_name == "public.orders"
    assert change.new["id"] == 1000
    assert change.new["amount"] == Decimal("12.30")
    assert change.new["note"] is None
    assert change.new["created_at"].isoformat() == "2026-10-18T10:00:00+00:00"
    assert txn.commit_lsn < txn.end_lsn


@scenario
async def test_rolled_back_transaction_is_never_delivered(
    sql: Sql, stream: ReplicationStream
) -> None:
    async with sql.transaction(force_rollback=True):
        await sql.execute("INSERT INTO customers VALUES (900, 'ghost', 'NL', NULL, NULL)")
    await sql.execute("INSERT INTO customers VALUES (901, 'real', 'NL', NULL, NULL)")
    txn = await next_txn(stream)
    [change] = txn.changes
    assert isinstance(change, RowInsert)
    assert change.new["id"] == 901


@scenario
async def test_update_leaving_toasted_column_untouched(sql: Sql, stream: ReplicationStream) -> None:
    await sql.execute("UPDATE customers SET bio = %s WHERE id = 1", (BIG,))
    await sql.execute("UPDATE customers SET name = 'renamed' WHERE id = 1")
    first = await next_txn(stream)
    second = await next_txn(stream)
    [set_bio] = first.changes
    [rename] = second.changes
    assert isinstance(set_bio, RowUpdate)
    assert set_bio.new["bio"] == BIG
    assert isinstance(rename, RowUpdate)
    assert rename.new["name"] == "renamed"
    assert rename.new["bio"] is UNCHANGED_TOAST
    assert rename.old is None
    assert rename.key == {"id": 1}


@scenario
async def test_primary_key_update_sends_old_key(sql: Sql, stream: ReplicationStream) -> None:
    await sql.execute("UPDATE orders SET id = 5000 WHERE id = 3")
    [change] = (await next_txn(stream)).changes
    assert isinstance(change, RowUpdate)
    assert change.old_kind == "key"
    assert change.old == {"id": 3}
    assert change.new["id"] == 5000


@scenario
async def test_replica_identity_full_sends_whole_old_row(
    sql: Sql, stream: ReplicationStream
) -> None:
    await sql.execute("ALTER TABLE customers REPLICA IDENTITY FULL")
    await sql.execute("UPDATE customers SET country = 'JP' WHERE id = 2")
    [change] = (await next_txn(stream)).changes
    assert isinstance(change, RowUpdate)
    assert change.old_kind == "full"
    assert change.old is not None
    assert set(change.old) == {"id", "name", "country", "email", "bio"}
    assert change.new["country"] == "JP"


@scenario
async def test_cascading_delete_arrives_in_one_transaction(
    sql: Sql, stream: ReplicationStream
) -> None:
    async with sql.cursor() as cur:
        await cur.execute("SELECT count(*) FROM orders WHERE customer_id = 1")
        row = await cur.fetchone()
    assert row is not None
    await sql.execute("DELETE FROM customers WHERE id = 1")
    txn = await next_txn(stream)
    deletes = [c for c in txn.changes if isinstance(c, RowDelete)]
    assert deletes[0].relation.name == "customers"
    assert deletes[0].key == {"id": 1}
    assert sum(1 for d in deletes if d.relation.name == "orders") == row[0]


@scenario
async def test_truncate(sql: Sql, stream: ReplicationStream) -> None:
    await sql.execute("TRUNCATE customers, orders")
    [change] = (await next_txn(stream)).changes
    assert isinstance(change, TableTruncate)
    assert {r.name for r in change.relations} == {"customers", "orders"}


@scenario
async def test_transactions_arrive_in_commit_order(sql: Sql, stream: ReplicationStream) -> None:
    for i in range(5):
        await sql.execute("INSERT INTO customers VALUES (%s, 'n', 'NL', NULL, NULL)", (2000 + i,))
    txns = [await next_txn(stream) for _ in range(5)]
    ids = [c.new["id"] for t in txns for c in t.changes if isinstance(c, RowInsert)]
    assert ids == [2000, 2001, 2002, 2003, 2004]
    ends = [t.end_lsn for t in txns]
    assert ends == sorted(ends)


@scenario
async def test_ack_advances_confirmed_flush_lsn(sql: Sql, stream: ReplicationStream) -> None:
    await sql.execute("INSERT INTO customers VALUES (3000, 'n', 'NL', NULL, NULL)")
    txn = await next_txn(stream)
    stream.ack(txn.end_lsn)
    await stream.send_status()
    async with asyncio.timeout(10):
        # Polls another session's catalog; there is no event to await.
        while (confirmed := await slot_confirmed_flush(sql, stream.slot)) < txn.end_lsn:  # noqa: ASYNC110
            await asyncio.sleep(0.05)
    assert confirmed >= txn.end_lsn


def test_resuming_a_slot_skips_acknowledged_transactions(dsn: str) -> None:
    async def go() -> None:
        await fresh(dsn)
        name = f"it_resume_{uuid.uuid4().hex[:8]}"
        repl = await ReplicationConnection.connect(dsn)
        await create_slot(repl, name)
        await repl.close()
        try:
            async with replicating(dsn, slot=name, temporary=False) as (sql, stream):
                await sql.execute("INSERT INTO customers VALUES (4000, 'a', 'NL', NULL, NULL)")
                await sql.execute("INSERT INTO customers VALUES (4001, 'b', 'NL', NULL, NULL)")
                first = await next_txn(stream)
                stream.ack(first.end_lsn)  # second is received but not acknowledged
                await next_txn(stream)
            async with replicating(dsn, slot=name, temporary=False) as (sql, stream):
                again = await next_txn(stream)
                [change] = again.changes
                assert isinstance(change, RowInsert)
                assert change.new["id"] == 4001
        finally:
            repl = await ReplicationConnection.connect(dsn)
            await drop_slot(repl, name)
            await repl.close()

    run(go())


def test_wrong_password_is_rejected(dsn: str) -> None:
    bad = psycopg.conninfo.make_conninfo(dsn, password="definitely-wrong")
    with pytest.raises(PgError) as info:
        run(ReplicationConnection.connect(bad))
    assert info.value.sqlstate == "28P01"


def test_replication_commands_report_server_errors(dsn: str) -> None:
    async def go() -> None:
        repl = await ReplicationConnection.connect(dsn)
        try:
            with pytest.raises(PgError) as info:
                await drop_slot(repl, "no_such_slot_anywhere")
            assert info.value.sqlstate == "42704"
            # The connection is still usable after an error.
            rows = await repl.simple_query("IDENTIFY_SYSTEM")
            assert Lsn.parse(str(rows[0][2])) > 0
        finally:
            await repl.close()

    run(go())
