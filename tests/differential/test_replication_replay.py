"""M1 acceptance: the decoded replication stream, replayed into empty tables,
reproduces the source tables byte-for-byte.

1. Seed `customers`/`orders` (with NULLs and out-of-line TOAST values).
2. Create a slot that exports a snapshot and copy that snapshot into the
   `replica` schema; the slot streams exactly what commits after it.
3. Run >= 10k randomized statements (with rollbacks, PK changes, cascades,
   UPDATEs that leave TOASTed values untouched) while concurrently replaying
   every decoded transaction into `replica`, one replica transaction per
   source transaction.
4. After the end-marker message arrives, compare every row's text
   representation (`t::text`) in both schemas, and the typed rows against
   the load generator's model.

Replay is deliberately naive. It uses only what the stream carries: key
columns to locate rows, and new values minus UNCHANGED_TOAST to update them.
Any decoding mistake therefore shows up as a difference.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter

import psycopg
from psycopg import sql

from deltaflow.aio import run
from deltaflow.devtools.loadgen import LoadStats, load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import OpKind, TableState, Workload
from deltaflow.replication.changes import (
    Row,
    RowDelete,
    RowInsert,
    RowUpdate,
    TableTruncate,
    Transaction,
)
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Message
from deltaflow.replication.slot import create_publication, create_slot
from deltaflow.replication.stream import ReplicationStream
from deltaflow.replication.wire import ReplicationConnection

TARGET_STATEMENTS = 10_000
PUBLICATION = "deltaflow_diff"
TABLES = ("customers", "orders")
MARKER_PREFIX = "deltaflow.test"
TIMEOUT_S = 600

type Conn = psycopg.AsyncConnection[tuple[object, ...]]


def test_replayed_stream_reproduces_source_tables(dsn: str, workload_seed: int) -> None:
    replay = run(_scenario(dsn, workload_seed))
    print(f"replay stats: {dict(replay.counters)}")
    print(
        f"load stats: committed={replay.load.committed} rolled_back={replay.load.rolled_back} "
        f"statements={dict(replay.load.statements)}"
    )

    context = f"seed={workload_seed} (replay: DELTAFLOW_SEED={workload_seed} make differential)"
    for table in TABLES:
        src, dst = replay.source_text[table], replay.replica_text[table]
        assert src == dst, f"{table} differs: {_first_difference(src, dst)}; {context}"
    assert replay.replica_state == replay.model, f"replica != load-generator model; {context}"

    # The comparison only means something if the hard cases actually happened.
    total = sum(replay.load.statements.values())
    assert total >= TARGET_STATEMENTS
    assert replay.load.rolled_back > 0
    assert replay.load.statements[OpKind.REKEY_ORDER] > 0
    assert replay.load.statements[OpKind.DELETE_CUSTOMER] > 0
    assert replay.counters["unchanged_toast"] > 0, context
    assert replay.counters["null_values"] > 0, context
    assert replay.counters["old_key_updates"] > 0, context
    assert replay.counters["toasted_rows_in_replica"] > 0, context


class _Replay:
    def __init__(self) -> None:
        self.counters: Counter[str] = Counter()
        self.load = LoadStats()
        self.model = TableState({}, {})
        self.replica_state = TableState({}, {})
        self.source_text: dict[str, list[str]] = {}
        self.replica_text: dict[str, list[str]] = {}


async def _scenario(dsn: str, seed: int) -> _Replay:
    result = _Replay()
    await seed_database(dsn, seed=seed, customers=300, orders=2_000)
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
        await create_publication(admin, PUBLICATION, [f"public.{t}" for t in TABLES])
        await admin.execute("DROP SCHEMA IF EXISTS replica CASCADE")
        await admin.execute("CREATE SCHEMA replica")
        for t in TABLES:
            # LIKE copies columns, PK and CHECKs but not the FK; replay order
            # within a transaction (customer delete, then its cascaded order
            # deletes) is the source's order, not one an FK would accept.
            await admin.execute(
                sql.SQL("CREATE TABLE replica.{} (LIKE public.{} INCLUDING ALL)").format(
                    sql.Identifier(t), sql.Identifier(t)
                )
            )

    repl = await ReplicationConnection.connect(dsn)
    slot = await create_slot(
        repl, f"diff_{uuid.uuid4().hex[:12]}", temporary=True, export_snapshot=True
    )
    assert slot.snapshot_name is not None
    await _copy_snapshot(dsn, slot.snapshot_name)

    stream = ReplicationStream(repl, slot=slot.name, publications=[PUBLICATION], status_interval=1)
    await stream.start()
    try:
        async with asyncio.timeout(TIMEOUT_S):
            model, _ = await asyncio.gather(
                _load(dsn, seed, result.load), _replay(dsn, stream, result.counters)
            )
    finally:
        await stream.close()
    result.model = model

    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        for t in TABLES:
            result.source_text[t] = await _row_texts(conn, "public", t)
            result.replica_text[t] = await _row_texts(conn, "replica", t)
        await conn.execute("SET search_path = replica")
        result.replica_state = await load_state(conn)
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM replica.customers WHERE length(bio) >= 8192")
            row = await cur.fetchone()
            assert row is not None
            result.counters["toasted_rows_in_replica"] = int(str(row[0]))
    return result


async def _copy_snapshot(dsn: str, snapshot: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await conn.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
        async with conn.transaction():
            await conn.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot)))
            for t in TABLES:
                await conn.execute(
                    sql.SQL("INSERT INTO replica.{} SELECT * FROM public.{}").format(
                        sql.Identifier(t), sql.Identifier(t)
                    )
                )


async def _load(dsn: str, seed: int, stats: LoadStats) -> TableState:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        workload = Workload(seed, await load_state(conn))
        while sum(stats.statements.values()) < TARGET_STATEMENTS:
            await run_load(conn, workload, txns=100, stats=stats)
        await conn.execute("SELECT pg_logical_emit_message(true, %s, 'end')", (MARKER_PREFIX,))
        return workload.state


async def _replay(dsn: str, stream: ReplicationStream, counters: Counter[str]) -> None:
    async with await psycopg.AsyncConnection.connect(dsn) as replica:
        async for event in stream:
            if not isinstance(event, Transaction):
                continue
            finished = await _apply(replica, event, counters)
            stream.ack(event.end_lsn)
            if finished:
                return
    raise AssertionError("stream ended before the end marker")


async def _apply(conn: Conn, txn: Transaction, counters: Counter[str]) -> bool:
    finished = False
    counters["transactions"] += 1
    async with conn.transaction(), conn.cursor() as cur:
        for change in txn.changes:
            match change:
                case RowInsert():
                    _count_values(change.new, counters)
                    cols = list(change.new)
                    await cur.execute(
                        sql.SQL("INSERT INTO replica.{} ({}) VALUES ({})").format(
                            sql.Identifier(change.relation.name),
                            sql.SQL(", ").join(map(sql.Identifier, cols)),
                            sql.SQL(", ").join(sql.Placeholder() * len(cols)),
                        ),
                        [change.new[c] for c in cols],
                    )
                    counters["inserts"] += 1
                case RowUpdate():
                    _count_values(change.new, counters)
                    if change.old_kind == "key":
                        counters["old_key_updates"] += 1
                    new = {k: v for k, v in change.new.items() if v is not UNCHANGED_TOAST}
                    await _execute_keyed(
                        cur,
                        "UPDATE replica.{} SET {} WHERE {}",
                        change.relation.name,
                        new,
                        change.key,
                    )
                    counters["updates"] += 1
                case RowDelete():
                    await _execute_keyed(
                        cur,
                        "DELETE FROM replica.{} WHERE {}",
                        change.relation.name,
                        None,
                        change.key,
                    )
                    counters["deletes"] += 1
                case TableTruncate():
                    for rel in change.relations:
                        await cur.execute(
                            sql.SQL("TRUNCATE replica.{}").format(sql.Identifier(rel.name))
                        )
                    counters["truncates"] += 1
                case Message():
                    finished = finished or change.prefix == MARKER_PREFIX
    return finished


async def _execute_keyed(
    cur: psycopg.AsyncCursor[tuple[object, ...]],
    template: str,
    table: str,
    assignments: Row | None,
    key: Row,
) -> None:
    where = sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()) for k in key
    )
    params = list(key.values())
    parts: list[sql.Composable] = [sql.Identifier(table)]
    if assignments is not None:
        parts.append(
            sql.SQL(", ").join(
                sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()) for k in assignments
            )
        )
        params = [*assignments.values(), *params]
    parts.append(where)
    await cur.execute(sql.SQL(template).format(*parts), params)
    if cur.rowcount != 1:
        raise AssertionError(f"{template} on {table} key={key} affected {cur.rowcount} rows")


def _count_values(row: Row, counters: Counter[str]) -> None:
    for v in row.values():
        if v is None:
            counters["null_values"] += 1
        elif v is UNCHANGED_TOAST:
            counters["unchanged_toast"] += 1


async def _row_texts(conn: Conn, schema: str, table: str) -> list[str]:
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL("SELECT t::text FROM {}.{} t ORDER BY id").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )
        return [str(r[0]) for r in await cur.fetchall()]


def _first_difference(src: list[str], dst: list[str]) -> str:
    if len(src) != len(dst):
        missing = set(src) - set(dst)
        extra = set(dst) - set(src)
        return (
            f"{len(src)} source rows vs {len(dst)} replica rows; "
            f"{len(missing)} missing, {len(extra)} extra; "
            f"e.g. missing {sorted(missing)[:1]!r:.300} extra {sorted(extra)[:1]!r:.300}"
        )
    for i, (a, b) in enumerate(zip(src, dst, strict=True)):
        if a != b:
            return f"row {i}: source {a!r:.300} replica {b!r:.300}"
    return "no difference"
