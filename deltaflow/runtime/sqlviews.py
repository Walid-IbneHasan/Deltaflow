"""Views from a SQL file, prepared against the database they read.

    deltaflow run views.sql --slot app

The file holds `CREATE [MATERIALIZED] VIEW schema.name AS SELECT ...;`
statements. Before anything streams:

1. **Types.** The source tables' column types are read from the catalog,
   so the views' expressions are typed as Postgres types them
   (planner/sqlexpr.py).
2. **Validation.** Each view is created as a temporary view, in a
   transaction that is rolled back. Postgres itself checks names, types
   and functions, with its own messages, and its output columns are
   compared with the translation's.
3. **Sink tables.** A missing sink table is created, with Postgres's
   column types and the view's key as its primary key:
   - the GROUP BY columns;
   - every column of a DISTINCT view;
   - or else the source tables' primary keys, which the SELECT list must
     then include.

   A sharded registry gets partial tables instead (`<table>__shards`, led
   by `shard`); prepare_cluster creates the merge views over them.

A primary key cannot be NULL. A view whose key column can be NULL (a
group of NULLs, a left join's right side) needs COALESCE in its SELECT
list. Its rows would otherwise fail to insert, loudly.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
from pglast import ast, parse_sql
from psycopg import sql

from deltaflow.planner import Deltaflow
from deltaflow.planner.sql import (
    aggregates_used,
    group_key_names,
    output_names,
    source_tables,
    sql_view,
    view_statement,
)
from deltaflow.planner.sqlexpr import SqlError, deparse
from deltaflow.sinks.postgres import SHARD_COLUMN

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]

#: catalog type name -> the kind the expression compiler reproduces. Types
#: missing here (char(n), real, ...) have no static kind: char(n)
#: comparisons ignore trailing blanks and real is single precision in
#: Postgres, which Python does not reproduce. An array type (typname
#: "_int4") is its element's kind with "[]".
_KINDS = {
    "int2": "int", "int4": "int", "int8": "int", "numeric": "numeric", "float8": "float",
    "text": "text", "varchar": "text", "name": "text", "bool": "bool", "date": "date",
    "timestamptz": "timestamptz", "timestamp": "timestamp", "json": "json", "jsonb": "jsonb",
}  # fmt: skip
_KINDS |= {f"_{t}": f"{k}[]" for t, k in _KINDS.items()}


@dataclass(frozen=True)
class SqlView:
    statement: str
    #: (schema, table) of the sink, from CREATE VIEW schema.table
    target: tuple[str, str]
    select: ast.SelectStmt

    @property
    def name(self) -> str:
        return self.target[1]

    @property
    def table(self) -> str:
        return f"{self.target[0]}.{self.target[1]}"


@dataclass
class Catalog:
    """What the views need to know about their source tables."""

    kinds: dict[str, dict[str, str]] = field(default_factory=dict)  # table -> column -> kind
    primary_keys: dict[str, tuple[str, ...]] = field(default_factory=dict)


def read_sql_file(path: str | Path) -> list[SqlView]:
    """The views of a .sql file, one per statement."""
    text = Path(path).read_text(encoding="utf-8")
    raw = text.encode("utf-8")  # the parser's locations are byte offsets
    views = []
    for stmt in parse_sql(text):
        start = stmt.stmt_location or 0
        end = start + stmt.stmt_len if stmt.stmt_len else len(raw)
        statement = raw[start:end].decode("utf-8").strip()
        target, select = view_statement(statement)
        if target is None:
            raise SqlError(
                f"{path}: every statement must be CREATE [MATERIALIZED] VIEW schema.name AS "
                f"SELECT ..., which names the sink table; found {statement[:60]!r}"
            )
        views.append(SqlView(statement, target, select))
    if not views:
        raise SqlError(f"{path} defines no views")
    names = [v.name for v in views]
    if len(set(names)) != len(names):
        raise SqlError(f"{path}: two views would be named alike: {sorted(names)}")
    return views


async def inspect(conn: Conn, views: Sequence[SqlView]) -> Catalog:
    """Column types and primary keys of every source table."""
    catalog = Catalog()
    tables = {t for v in views for _, t, _ in source_tables(v.select)}
    async with conn.cursor() as cur:
        for table in sorted(tables):
            await cur.execute(
                "SELECT a.attname::text, t.typname::text FROM pg_attribute a"
                " JOIN pg_type t ON t.oid = a.atttypid"
                " WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped",
                (table,),
            )
            columns = list(await cur.fetchall())
            if not columns:
                raise SqlError(f"source table {table} does not exist")
            catalog.kinds[table] = {c: _KINDS[t] for c, t in columns if t in _KINDS}
            await cur.execute(
                "SELECT ARRAY(SELECT a.attname::text FROM pg_index i"
                " CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, ord)"
                " JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum"
                " WHERE i.indrelid = to_regclass(%s) AND i.indisprimary ORDER BY k.ord)",
                (table,),
            )
            row = await cur.fetchone()
            catalog.primary_keys[table] = tuple(row[0]) if row else ()
    return catalog


def build(
    views: Sequence[SqlView],
    catalog: Catalog,
    *,
    shards: int | None = None,
    shard_by: Mapping[str, str] | None = None,
) -> Deltaflow:
    """A fresh registry of the views (a cluster builds one per shard)."""
    df = Deltaflow(shards=shards, shard_by=shard_by)
    for view in views:
        sql_view(df, view.statement, column_kinds=catalog.kinds)
    return df


async def validate(conn: Conn, views: Sequence[SqlView]) -> dict[str, list[tuple[str, str]]]:
    """Postgres's own check of every view, and its output columns
    (name, type), per sink table; nothing is left behind."""
    columns: dict[str, list[tuple[str, str]]] = {}
    async with conn.transaction(force_rollback=True), conn.cursor() as cur:
        for i, view in enumerate(views):
            probe = f"deltaflow_check_{i}"
            try:
                await cur.execute(
                    sql.SQL("CREATE TEMP VIEW {} AS ").format(sql.Identifier(probe))
                    + sql.SQL(deparse(view.select))  # Postgres's own deparse of the view
                )
            except psycopg.Error as exc:
                raise SqlError(f"view {view.table}: Postgres rejects it: {exc}") from None
            await cur.execute(
                "SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute"
                " WHERE attrelid = to_regclass(%s) AND attnum > 0 ORDER BY attnum",
                (f"pg_temp.{probe}",),
            )
            found = [(str(n), str(t)) for n, t in await cur.fetchall()]
            ours = output_names(view.select)
            if [n for n, _ in found] != ours:
                raise SqlError(
                    f"view {view.table}: Postgres names its columns {[n for n, _ in found]}, "
                    f"the translation {ours}"
                )
            columns[view.table] = found
    return columns


def view_key(view: SqlView, catalog: Catalog) -> tuple[str, ...]:
    """The output columns that identify a row of the view: its sink table's
    primary key."""
    select = view.select
    names = output_names(select)
    if aggregates_used(select):
        keys = group_key_names(select)
        if keys is None:
            raise SqlError(
                f"view {view.table}: every GROUP BY key must be in the SELECT list, or its rows "
                "have no key"
            )
        return keys
    if select.distinctClause:
        return tuple(names)
    selected = _selected_columns(select)
    key: list[str] = []
    for alias, table, how in source_tables(select):
        pk = catalog.primary_keys.get(table, ())
        if not pk:
            raise SqlError(f"view {view.table}: source {table} has no primary key")
        if how == "left":
            raise SqlError(
                f"view {view.table}: the right side of a left join can be NULL, and a primary "
                "key cannot; aggregate the view, or COALESCE the right table's key, and create "
                "the sink table yourself"
            )
        for column in pk:
            name = selected.get((alias, column))
            if name is None:
                raise SqlError(
                    f"view {view.table}: select {alias}.{column} (the primary key of {table}) "
                    "so that the view's rows have a key"
                )
            key.append(name)
    return tuple(key)


async def create_sinks(
    conn: Conn,
    views: Sequence[SqlView],
    catalog: Catalog,
    columns: Mapping[str, list[tuple[str, str]]],
    *,
    sharded: bool,
) -> list[str]:
    """Create the sink tables that are missing; returns those created."""
    created = []
    for view in views:
        schema, name = view.target
        table = f"{name}__shards" if sharded else name
        async with conn.cursor() as cur:
            await cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{schema}.{table}",))
            row = await cur.fetchone()
        if row and row[0]:
            continue
        key = view_key(view, catalog)
        cols = [
            sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in columns[view.table]
        ]
        key_cols = [sql.Identifier(k) for k in key]
        if sharded:
            cols.insert(0, sql.SQL("{} int NOT NULL").format(sql.Identifier(SHARD_COLUMN)))
            key_cols.insert(0, sql.Identifier(SHARD_COLUMN))
        if key_cols:
            cols.append(sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(key_cols)))
        await conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        await conn.execute(
            sql.SQL("CREATE TABLE {} ({})").format(
                sql.Identifier(schema, table), sql.SQL(", ").join(cols)
            )
        )
        created.append(f"{schema}.{table}")
    return created


async def prepare(
    dsn: str,
    views: Sequence[SqlView],
    *,
    create: bool = True,
    shards: int | None = None,
    shard_by: Mapping[str, str] | None = None,
) -> Catalog:
    """Inspect, validate and (if `create`) create sink tables; returns the
    catalog `build` needs."""
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        catalog = await inspect(conn, views)
        build(views, catalog, shards=shards, shard_by=shard_by)  # the plan is refused early
        columns = await validate(conn, views)
        if create:
            await create_sinks(conn, views, catalog, columns, sharded=shards is not None)
    return catalog


# --- parse-tree helpers ---------------------------------------------------------------


def _selected_columns(select: ast.SelectStmt) -> dict[tuple[str, str], str]:
    """(alias, column) -> output name, for plain column references."""
    aliases = [alias for alias, _, _ in source_tables(select)]
    out: dict[tuple[str, str], str] = {}
    for target, name in zip(select.targetList or (), output_names(select), strict=True):
        node = target.val
        if not isinstance(node, ast.ColumnRef):
            continue
        fields = [f.sval for f in node.fields or () if isinstance(f, ast.String)]
        if len(fields) >= 2:
            out[(str(fields[-2]), str(fields[-1]))] = name
        elif len(fields) == 1 and len(aliases) == 1:
            out[(aliases[0], str(fields[0]))] = name
    return out
