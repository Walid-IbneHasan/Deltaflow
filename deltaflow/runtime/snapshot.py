"""Source validation against the catalog, and the initial snapshot load.

Everything here runs before the first change is consumed, so a source the
engine cannot maintain correctly is rejected up front. In particular, the
engine rejects any configuration in which changes would silently not
arrive:

* a source table not in the publication;
* a publication that does not publish every operation;
* a publication with a row filter or column list;
* a partitioned table in a publication that does not publish via the
  root (its changes would arrive under the partitions' names), or a
  partition as a source (its changes arrive under the root's).

Postgres does not replicate rows that leave a partitioned table with its
partition (DETACH, DROP), nor TRUNCATE of one partition, nor the rows a
table brings when ATTACHed. `partitions` is how the runner notices the
first: a partition of a source that is no longer one.

Two ways to load the initial rows:

* `load_snapshot`: every table inside one transaction on the slot's
  exported snapshot. That transaction holds the server's xmin, and with it
  VACUUM everywhere, for the whole load;
* `ChunkReader`: one primary-key page at a time, each read with its own
  exported snapshot from a temporary slot, so its consistent point is
  exact (decision 0009). xmin is held for one page.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import psycopg
from psycopg import sql

from deltaflow.core import Row
from deltaflow.planner import PlanError
from deltaflow.replication.changes import ValueDecoder
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.slot import SlotInfo, create_slot, drop_slot
from deltaflow.replication.wire import ReplicationConnection
from deltaflow.runtime.engine import Engine
from deltaflow.state import TableSchema

log = logging.getLogger(__name__)

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]


class UnsupportedSourceError(PlanError):
    """A source table cannot be maintained incrementally as configured."""


class PartitionsChangedError(RuntimeError):
    """A partition of a source was detached or dropped: its rows left the
    table without a DELETE the stream carries. The checkpoint no longer
    describes the table; starting again bootstraps."""


_DESCRIBE = """
SELECT c.relkind::text,
       c.relreplident::text,
       (SELECT rn.nspname || '.' || r.relname FROM pg_class r
        JOIN pg_namespace rn ON rn.oid = r.relnamespace
        WHERE c.relispartition AND r.oid = pg_partition_root(c.oid)),
       ARRAY(SELECT a.attname::text FROM pg_attribute a
             WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
               AND a.attgenerated = ''
             ORDER BY a.attnum),
       ARRAY(SELECT a.atttypid::int8 FROM pg_attribute a
             WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
               AND a.attgenerated = ''
             ORDER BY a.attnum),
       ARRAY(SELECT a.atttypmod FROM pg_attribute a
             WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
               AND a.attgenerated = ''
             ORDER BY a.attnum),
       ARRAY(SELECT a.attname::text
             FROM pg_index i
             CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
             WHERE i.indrelid = c.oid AND i.indisprimary
             ORDER BY k.ord),
       ARRAY(SELECT a.attname::text
             FROM pg_index i
             CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
             WHERE i.indrelid = c.oid AND i.indisreplident
             ORDER BY k.ord)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
"""


async def describe_table(conn: Conn, table: str) -> TableSchema:
    """Columns as pgoutput sends them, and the key the arrangement uses."""
    namespace, name = table.split(".", 1)
    async with conn.cursor() as cur:
        await cur.execute(_DESCRIBE, (namespace, name))
        found = await cur.fetchone()
    if found is None:
        raise UnsupportedSourceError(f"source table {table} does not exist")
    relkind, identity, root, columns, types, typmods, pk, replident = found
    if root is not None:
        raise UnsupportedSourceError(
            f"{table} is a partition of {root}; use {root} as the source: its changes are "
            "published under the root's name"
        )
    if relkind not in ("r", "p"):
        raise UnsupportedSourceError(
            f"{table} is not a table (relkind {relkind!r}); views and foreign tables are not "
            "supported as sources"
        )
    match identity:
        case "d" | "f" if pk:
            key = pk
        case "d":
            raise UnsupportedSourceError(
                f"{table} has no primary key, so its UPDATEs and DELETEs cannot be "
                "matched to rows; add a primary key"
            )
        case "f":
            raise UnsupportedSourceError(
                f"{table} has REPLICA IDENTITY FULL but no primary key; rows that are "
                "equal in every column cannot be told apart. Add a primary key"
            )
        case "i":
            key = replident
        case _:
            raise UnsupportedSourceError(
                f"{table} has REPLICA IDENTITY NOTHING; its UPDATEs and DELETEs carry no key"
            )
    return TableSchema(
        table,
        tuple(columns),
        tuple(key),
        tuple(int(t) for t in types),
        relkind == "p",
        tuple(int(m) for m in typmods),
    )


async def check_publication(conn: Conn, publication: str, schema: TableSchema) -> None:
    namespace, name = schema.table.split(".", 1)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate,
                   pt.rowfilter, pt.attnames::text[], p.pubviaroot
            FROM pg_publication p
            JOIN pg_publication_tables pt ON pt.pubname = p.pubname
            WHERE p.pubname = %s AND pt.schemaname = %s AND pt.tablename = %s
            """,
            (publication, namespace, name),
        )
        found = await cur.fetchone()
    if found is None:
        raise UnsupportedSourceError(f"publication {publication!r} does not include {schema.table}")
    all_ops, rowfilter, attnames, via_root = found
    if schema.partitioned and not via_root:
        raise UnsupportedSourceError(
            f"publication {publication!r} must publish {schema.table} via its root "
            "(WITH (publish_via_partition_root = true)): its changes would otherwise arrive "
            "under the partitions' names"
        )
    if not all_ops:
        raise UnsupportedSourceError(
            f"publication {publication!r} does not publish every operation "
            "(insert, update, delete, truncate); views would miss changes"
        )
    if rowfilter is not None:
        raise UnsupportedSourceError(
            f"publication {publication!r} has a row filter on {schema.table} ({rowfilter}); "
            "rows moving out of the filter would not be retracted correctly"
        )
    if attnames is not None and tuple(attnames) != schema.columns:
        raise UnsupportedSourceError(
            f"publication {publication!r} publishes only some columns of {schema.table}"
        )


async def partitions(conn: Conn, table: str) -> list[str]:
    """The leaf partitions of a partitioned table, schema-qualified."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT n.nspname || '.' || c.relname FROM pg_partition_tree(%s::regclass) t"
            " JOIN pg_class c ON c.oid = t.relid JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE t.isleaf ORDER BY 1",
            (table,),
        )
        return [str(r[0]) for r in await cur.fetchall()]


async def partitioned_sources(conn: Conn, tables: Sequence[str]) -> list[str]:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT n.nspname || '.' || c.relname FROM pg_class c"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE c.relkind = 'p' AND n.nspname || '.' || c.relname = ANY(%s)",
            (list(tables),),
        )
        return [str(r[0]) for r in await cur.fetchall()]


def removed_partitions(before: dict[str, list[str]], now: dict[str, list[str]]) -> str | None:
    """A partition in `before` that `now` lacks, described; None if none.
    (Added ones are fine: their rows arrive as inserts. One ATTACHed with
    rows already in it is the case Postgres cannot report; docs/sql.md.)"""
    for table, leaves in sorted(before.items()):
        gone = sorted(set(leaves) - set(now.get(table, ())))
        if gone:
            return (
                f"{', '.join(gone)} no longer a partition of {table} (detached or dropped): "
                "its rows left the table without a DELETE the stream carries"
            )
    return None


async def load_snapshot(
    engine: Engine,
    conn: Conn,
    *,
    snapshot_name: str,
    consistent_point: Lsn,
    publication: str,
    tables: Sequence[str] | None = None,
) -> None:
    """Install every source's rows as of the exported snapshot, then start
    the engine at the slot's consistent point.

    The catalog is read inside the same snapshot, so the schema matches the
    rows exactly. The snapshot is only importable while the replication
    connection that exported it has not run another command, so call this
    before START_REPLICATION.
    """
    await conn.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
    async with conn.transaction():
        await conn.execute(
            sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot_name))
        )
        schemas = []
        for table in tables if tables is not None else engine.sources:
            schema = await describe_table(conn, table)
            await check_publication(conn, publication, schema)
            engine.declare(schema)  # a left join may pad with it before it loads
            schemas.append(schema)
        decode = ValueDecoder()
        for schema in schemas:
            query = sql.SQL("SELECT {} FROM {}").format(select_list(schema, decode), _table(schema))
            # Binary, server-side cursor: the table is streamed into the
            # engine in chunks, never held in memory as a whole.
            loader = engine.begin_install(schema)
            async with conn.cursor(name="deltaflow_snapshot", binary=True) as cur:
                cur.itersize = 200
                await cur.execute(query)
                async for record in cur:
                    loader.add(row_of(schema, record, decode))
            loader.finish()
    engine.start(consistent_point)


# --- chunks (decision 0009) -----------------------------------------------------

#: keys per re-read query (each is one snapshot)
REREAD_BATCH = 1_000


@dataclass(frozen=True, slots=True)
class Page:
    #: the rows are the table as of this consistent point
    cp: Lsn
    rows: list[Row]
    #: the last key, when the page was full (more may follow); None when it
    #: was the table's last page
    after: tuple[Any, ...] | None


class ChunkReader:
    """Reads source tables a primary-key page at a time, each page with its
    own snapshot: a temporary slot exporting one, so that the page is the
    table exactly as of the slot's consistent point. A page's transaction
    is the only one held open, for as long as the page takes to read.

    Pages follow the key index's own order (its collation, for text), and
    the keys are only ever compared by Postgres.
    """

    def __init__(self, dsn: str, *, slot: str, application_name: str) -> None:
        self._dsn = dsn
        # Unique per page: after a crash the server may not have dropped the
        # previous session's temporary slot yet.
        self._prefix = f"{slot[:44]}_l"
        self._app = application_name
        self._repl: ReplicationConnection | None = None
        self._conn: Conn | None = None
        self._key_types: dict[str, tuple[str, ...]] = {}
        self._decode = ValueDecoder()

    async def __aenter__(self) -> ChunkReader:
        self._conn = await psycopg.AsyncConnection.connect(self._dsn, application_name=self._app)
        await self._conn.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
        self._repl = await ReplicationConnection.connect(self._dsn, application_name=self._app)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._repl is not None:
            with contextlib.suppress(Exception):
                await self._repl.close()
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()

    async def page(self, schema: TableSchema, after: tuple[Any, ...] | None, limit: int) -> Page:
        """Up to `limit` rows with keys after `after` (from the start: None), in key order."""
        keys = sql.SQL(", ").join(map(sql.Identifier, schema.key))
        params: list[Any] = []
        where: sql.Composable = sql.SQL("")
        if after is not None:
            where = sql.SQL("WHERE ({}) > ({})").format(keys, await self._typed(schema))
            params.extend(after)
        query = sql.SQL("SELECT {} FROM {} {} ORDER BY {} LIMIT %s").format(
            select_list(schema, self._decode), _table(schema), where, keys
        )
        cp, rows = await self._read(schema, query, [*params, limit])
        last = tuple(rows[-1][c] for c in schema.key) if len(rows) == limit else None
        return Page(cp, rows, last)

    async def rows_of(self, schema: TableSchema, keys: Sequence[tuple[Any, ...]]) -> Page:
        """The current rows of `keys` (a key without a row is gone)."""
        typed = await self._typed(schema)
        values = sql.SQL(", ").join(sql.SQL("({})").format(typed) for _ in keys)
        query = sql.SQL("SELECT {} FROM {} WHERE ({}) IN (VALUES {})").format(
            select_list(schema, self._decode),
            _table(schema),
            sql.SQL(", ").join(map(sql.Identifier, schema.key)),
            values,
        )
        cp, rows = await self._read(schema, query, [v for key in keys for v in key])
        return Page(cp, rows, None)

    async def _read(
        self, schema: TableSchema, query: sql.Composed, params: Sequence[Any]
    ) -> tuple[Lsn, list[Row]]:
        assert self._conn is not None
        assert self._repl is not None
        name = f"{self._prefix}{uuid.uuid4().hex[:12]}"
        info = await self._create(name)
        assert info.snapshot_name is not None
        try:
            async with self._conn.transaction():
                await self._conn.execute(
                    sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(info.snapshot_name))
                )
                # Binary, as load_snapshot reads: the same Python values.
                async with self._conn.cursor(binary=True) as cur:
                    await cur.execute(query, params)
                    records = await cur.fetchall()
        finally:
            await drop_slot(self._repl, name)
        return info.consistent_point, [row_of(schema, r, self._decode) for r in records]

    async def _create(self, name: str) -> SlotInfo:
        """Create the page's slot. Creation waits for every transaction
        running at that moment to end; a long one stalls the load, so say
        which sessions it is waiting for."""
        assert self._repl is not None
        creating = asyncio.ensure_future(
            create_slot(self._repl, name, temporary=True, export_snapshot=True)
        )
        waited = 0
        try:
            while True:
                done, _ = await asyncio.wait({creating}, timeout=10)
                if done:
                    return creating.result()
                waited += 10
                log.warning(
                    "the next chunk has waited %d s for transactions that were open when its "
                    "slot was created to end",
                    waited,
                    extra={"sessions": await self._open_transactions()},
                )
        except BaseException:
            creating.cancel()
            raise

    async def _open_transactions(self) -> list[dict[str, Any]]:
        assert self._conn is not None
        async with self._conn.transaction(), self._conn.cursor() as cur:
            await cur.execute(
                "SELECT pid, state, (now() - xact_start)::text, left(query, 200)"
                " FROM pg_stat_activity WHERE backend_xid IS NOT NULL"
                " ORDER BY xact_start LIMIT 5"
            )
            found = await cur.fetchall()
        return [{"pid": p, "state": s, "open for": t, "query": q} for p, s, t, q in found]

    async def _typed(self, schema: TableSchema) -> sql.Composable:
        """Placeholders for one key, cast to the key columns' types."""
        types = self._key_types.get(schema.table)
        if types is None:
            types = self._key_types[schema.table] = await _key_types(self._conn, schema)
        return sql.SQL(", ").join(sql.SQL("%s::" + t) for t in types)


async def _key_types(conn: Conn | None, schema: TableSchema) -> tuple[str, ...]:
    assert conn is not None
    namespace, name = schema.table.split(".", 1)
    async with conn.transaction(), conn.cursor() as cur:
        await cur.execute(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)"
            " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
            (namespace, name),
        )
        found: dict[str, str] = {str(n): str(t) for n, t in await cur.fetchall()}
    return tuple(found[c] for c in schema.key)


def select_list(schema: TableSchema, decode: ValueDecoder) -> sql.Composable:
    """The table's columns, those the stream's decoder must read as text
    cast to text (json, jsonb, arrays, enums): a snapshot then gets exactly
    the values the stream gets for them."""
    types = schema.types or (0,) * len(schema.columns)
    return sql.SQL(", ").join(
        sql.SQL("{}::text").format(sql.Identifier(c))
        if t and decode.reads_as_text(t)
        else sql.Identifier(c)
        for c, t in zip(schema.columns, types, strict=True)
    )


def row_of(schema: TableSchema, record: Sequence[Any], decode: ValueDecoder) -> Row:
    types = schema.types or (0,) * len(schema.columns)
    return Row(
        (c, decode.text(t, v) if t and decode.reads_as_text(t) else v)
        for c, t, v in zip(schema.columns, types, record, strict=True)
    )


def _table(schema: TableSchema) -> sql.Composable:
    return sql.Identifier(*schema.table.split(".", 1))
