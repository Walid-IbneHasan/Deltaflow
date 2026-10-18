"""Source validation against the catalog, and the initial snapshot load.

Everything here runs before the first change is consumed, so a source the
engine cannot maintain correctly is rejected up front. In particular, the
engine rejects any configuration in which changes would silently not
arrive:

* a source table not in the publication;
* a publication that does not publish every operation;
* a publication with a row filter or column list.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg import sql

from deltaflow.core import Row
from deltaflow.planner import PlanError
from deltaflow.replication.lsn import Lsn
from deltaflow.runtime.engine import Engine
from deltaflow.state import TableSchema

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]


class UnsupportedSourceError(PlanError):
    """A source table cannot be maintained incrementally as configured."""


_DESCRIBE = """
SELECT c.relkind::text,
       c.relreplident::text,
       ARRAY(SELECT a.attname::text FROM pg_attribute a
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
    relkind, identity, columns, pk, replident = found
    if relkind != "r":
        raise UnsupportedSourceError(
            f"{table} is not an ordinary table (relkind {relkind!r}); views, "
            "partitioned and foreign tables are not supported as sources"
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
    return TableSchema(table, tuple(columns), tuple(key))


async def check_publication(conn: Conn, publication: str, schema: TableSchema) -> None:
    namespace, name = schema.table.split(".", 1)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate,
                   pt.rowfilter, pt.attnames::text[]
            FROM pg_publication p
            JOIN pg_publication_tables pt ON pt.pubname = p.pubname
            WHERE p.pubname = %s AND pt.schemaname = %s AND pt.tablename = %s
            """,
            (publication, namespace, name),
        )
        found = await cur.fetchone()
    if found is None:
        raise UnsupportedSourceError(f"publication {publication!r} does not include {schema.table}")
    all_ops, rowfilter, attnames = found
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
        for table in tables if tables is not None else engine.sources:
            schema = await describe_table(conn, table)
            await check_publication(conn, publication, schema)
            query = sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(", ").join(map(sql.Identifier, schema.columns)),
                sql.Identifier(*table.split(".", 1)),
            )
            # Binary, server-side cursor: the table is streamed into the
            # engine in chunks, never held in memory as a whole.
            loader = engine.begin_install(schema)
            async with conn.cursor(name="deltaflow_snapshot", binary=True) as cur:
                cur.itersize = 200
                await cur.execute(query)
                async for record in cur:
                    loader.add(Row(zip(schema.columns, record, strict=True)))
            loader.finish()
    engine.start(consistent_point)
