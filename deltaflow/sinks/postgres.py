"""The Postgres sink: exactly-once upserts with a watermark in the same transaction.

The user creates the sink table. Its primary key is the view's key, and its
columns are the view's output columns. A table without a primary key may
hold at most one row (a global aggregate). Deltaflow keeps one row per view
in `_deltaflow.watermarks`: the LSN the table is written through.

The sink buffers deltas until their transaction is complete (frontier),
then folds each complete transaction into *net writes per key*: the key's
final row (upsert), or its absence (delete). A flush applies the writes and
moves every view's watermark to the frontier, **in one Postgres
transaction**. The table and its watermark therefore always agree.

On restart the engine replays from its last state checkpoint, which is
never ahead of the watermark. Transactions at or below the watermark come
through again, and the sink drops them, because the table already reflects
them. Neither duplicates nor gaps. The full argument is in
docs/exactly-once.md.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar

import psycopg
from psycopg import sql

from deltaflow.core import (
    Delta,
    Frontier,
    FrontierViolationError,
    MutableFrontier,
    Row,
    consolidate,
)
from deltaflow.faults import crash_point
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks.base import Sink
from deltaflow.state.codec import encode_key

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]

WATERMARKS = "_deltaflow.watermarks"


class SinkError(RuntimeError):
    """The view's output cannot be written to its table as declared."""


@dataclass(slots=True)
class SinkWrites:
    #: rows whose key the table must not have yet
    inserts: list[Row] = field(default_factory=list)
    #: rows whose key the table must already have
    updates: list[Row] = field(default_factory=list)
    #: key values of rows the table must have, to delete
    deletes: list[tuple[Any, ...]] = field(default_factory=list)

    @property
    def upserts(self) -> list[Row]:
        return [*self.inserts, *self.updates]


@dataclass(slots=True)
class _KeyState:
    key: tuple[Any, ...]
    #: did the table have this key before the current flush?
    existed: bool
    row: Row | None


class PostgresSink(Sink):
    durable: ClassVar[bool] = True

    def __init__(self, table: str) -> None:
        schema, dot, name = table.partition(".")
        if not dot or not schema or not name or "." in name:
            raise ValueError(f"sink table {table!r} must be schema-qualified, e.g. 'analytics.t'")
        self.table = table
        self.view: str | None = None
        self.columns: tuple[str, ...] = ()
        self.key: tuple[str, ...] = ()
        self.watermark: Lsn | None = None
        self._frontier = MutableFrontier(Frontier.at(Lsn(0)))
        self._pending: list[Delta[Row]] = []
        self._writes: dict[bytes, _KeyState] = {}
        self._bound = False

    def bind(
        self, *, view: str, columns: tuple[str, ...], key: tuple[str, ...], watermark: Lsn | None
    ) -> None:
        """Called by the runner with the table's shape and stored watermark."""
        self.view, self.columns, self.key, self.watermark = view, columns, key, watermark
        self._bound = True

    @property
    def frontier(self) -> Frontier:
        return self._frontier.current

    def accept(self, batch: Sequence[Delta[Row]]) -> None:
        if not self._bound:
            raise SinkError(f"sink {self.table} is not bound (the runner binds it at start)")
        for d in batch:
            if not self.frontier.admits(d.time):
                raise FrontierViolationError(
                    f"sink {self.table} received a delta at {d.time}, behind {self.frontier}"
                )
        self._pending.extend(batch)

    def advance(self, frontier: Frontier) -> None:
        self._frontier.advance(frontier)
        ready = [d for d in self._pending if frontier.is_complete(d.time)]
        if not ready:
            return
        self._pending = [d for d in self._pending if not frontier.is_complete(d.time)]
        if (
            self.watermark is not None
            and frontier.lower is not None
            and frontier.lower <= self.watermark
        ):
            return  # replayed transactions: the table already has them
        by_time: dict[Lsn, list[Delta[Row]]] = {}
        for d in consolidate(ready):
            by_time.setdefault(d.time, []).append(d)
        for t in sorted(by_time):
            self._fold(by_time[t])

    def _fold(self, batch: list[Delta[Row]]) -> None:
        """One complete transaction. Per key it may retract the row the table
        has, add the row the table will have, or both. Anything else means
        the view is not unique on the table's key."""
        added: dict[bytes, Row] = {}
        removed: set[bytes] = set()
        keys: dict[bytes, tuple[Any, ...]] = {}
        for d in batch:
            self._check_columns(d.row)
            key = tuple(d.row[c] for c in self.key)
            kb = encode_key(key)
            keys[kb] = key
            if abs(d.diff) > 1:
                raise SinkError(
                    f"{self.table}: row {d.row!r} has multiplicity {d.diff}; the view is not "
                    f"unique on the table's key {self.key}"
                )
            if d.diff < 0:
                if kb in removed:
                    raise self._not_unique(key, "retracts two rows")
                removed.add(kb)
            elif kb in added:
                raise self._not_unique(key, "has two rows")
            else:
                added[kb] = d.row
        for kb, key in keys.items():
            state = self._writes.get(kb)
            had = (state.row is not None) if state is not None else (kb in removed)
            if kb in removed and not had:
                raise self._not_unique(key, "retracts a row the table does not have")
            if kb in added and had and kb not in removed:
                raise self._not_unique(key, "would be a second row")
            if state is None:
                state = self._writes[kb] = _KeyState(key, existed=kb in removed, row=None)
            state.row = added.get(kb)

    def _not_unique(self, key: tuple[Any, ...], what: str) -> SinkError:
        return SinkError(
            f"{self.table}: a transaction {what} with key {key!r}; the view is not unique on "
            f"the table's key {self.key}"
        )

    def _check_columns(self, row: Row) -> None:
        if set(row) != set(self.columns):
            missing = sorted(set(self.columns) - set(row))
            extra = sorted(set(row) - set(self.columns))
            raise SinkError(
                f"{self.table}: view rows must have exactly the table's columns; "
                f"missing {missing}, extra {extra}"
            )

    def take(self) -> SinkWrites:
        """The net writes since the last take, and forget them."""
        writes = SinkWrites()
        for state in self._writes.values():
            if state.row is None:
                if state.existed:
                    writes.deletes.append(state.key)
            elif state.existed:
                writes.updates.append(state.row)
            else:
                writes.inserts.append(state.row)
        self._writes = {}
        return writes


# --- database side ---------------------------------------------------------


async def ensure_watermark_table(conn: Conn) -> None:
    await conn.execute("CREATE SCHEMA IF NOT EXISTS _deltaflow")
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS _deltaflow.watermarks ("
        " view text PRIMARY KEY, lsn pg_lsn NOT NULL, updated_at timestamptz NOT NULL)"
    )


async def describe_sink_table(conn: Conn, table: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """(columns, primary key columns) of an existing sink table."""
    schema, name = table.split(".", 1)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT ARRAY(SELECT a.attname::text FROM pg_attribute a
                         WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum),
                   ARRAY(SELECT a.attname::text FROM pg_index i
                         CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, ord)
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                         WHERE i.indrelid = c.oid AND i.indisprimary ORDER BY k.ord)
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'
            """,
            (schema, name),
        )
        found = await cur.fetchone()
    if found is None:
        raise SinkError(
            f"sink table {table} does not exist; create it (its primary key is the view's key)"
        )
    return tuple(found[0]), tuple(found[1])


async def load_watermark(conn: Conn, view: str) -> Lsn | None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT lsn::text FROM _deltaflow.watermarks WHERE view = %s", (view,))
        row = await cur.fetchone()
    return None if row is None else Lsn.parse(str(row[0]))


async def flush(
    conn: Conn, sinks: Sequence[PostgresSink], lsn: Lsn, *, replace: bool = False
) -> int:
    """Write every sink's net changes and move its watermark to `lsn`, all in
    one transaction. With `replace`, each table is emptied first (the initial
    load of a fresh snapshot).

    Every write is checked against the table. An insert must find no row, an
    update or delete exactly one. A mismatch means the table and the view
    have drifted apart (or the view is not unique on the key): it raises and
    the transaction rolls back. Returns the number of rows written."""
    for sink in sinks:
        if sink.watermark is not None and lsn < sink.watermark:
            raise SinkError(
                f"{sink.table}: refusing to move the watermark back from {sink.watermark} to {lsn}"
            )
    written = 0
    async with conn.transaction(), conn.cursor() as cur:
        for sink in sinks:
            writes = sink.take()
            table = sql.Identifier(*sink.table.split(".", 1))
            if replace:
                await cur.execute(sql.SQL("DELETE FROM {}").format(table))
            if sink.key:
                await _write_keyed(cur, sink, table, writes)
            else:
                await _write_single_row(cur, sink, table, writes)
            written += len(writes.upserts) + len(writes.deletes)
            await cur.execute(
                "INSERT INTO _deltaflow.watermarks (view, lsn, updated_at)"
                " VALUES (%s, %s::pg_lsn, now()) ON CONFLICT (view)"
                " DO UPDATE SET lsn = EXCLUDED.lsn, updated_at = EXCLUDED.updated_at",
                (sink.view, str(lsn)),
            )
        crash_point("flush:before_commit")
    for sink in sinks:
        sink.watermark = lsn
    crash_point("flush:after_commit")
    return written


async def _write_keyed(
    cur: psycopg.AsyncCursor[tuple[Any, ...]],
    sink: PostgresSink,
    table: sql.Identifier,
    writes: SinkWrites,
) -> None:
    if writes.deletes:
        where = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(k)) for k in sink.key
        )
        await cur.executemany(
            sql.SQL("DELETE FROM {} WHERE {} RETURNING 1").format(table, where),
            writes.deletes,
            returning=True,
        )
        _expect(sink, "delete", writes.deletes, await _results(cur), lambda r: r is not None)
    upserts = writes.upserts
    if not upserts:
        return
    cols = sql.SQL(", ").join(map(sql.Identifier, sink.columns))
    rest = [c for c in sink.columns if c not in sink.key]
    assignment = (
        sql.SQL("UPDATE SET {}").format(
            sql.SQL(", ").join(
                sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in rest
            )
        )
        if rest
        else sql.SQL("UPDATE SET {0} = EXCLUDED.{0}").format(sql.Identifier(sink.key[0]))
    )
    # (xmax = 0) is true for a freshly inserted row, false for an updated one.
    query = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO {} RETURNING (xmax = 0)"
    ).format(
        table,
        cols,
        sql.SQL(", ").join(sql.Placeholder() * len(sink.columns)),
        sql.SQL(", ").join(map(sql.Identifier, sink.key)),
        assignment,
    )
    await cur.executemany(query, [[r[c] for c in sink.columns] for r in upserts], returning=True)
    inserted = await _results(cur)
    n = len(writes.inserts)
    _expect(sink, "insert", writes.inserts, inserted[:n], lambda r: r is not None and r[0] is True)
    _expect(sink, "update", writes.updates, inserted[n:], lambda r: r is not None and r[0] is False)


async def _write_single_row(
    cur: psycopg.AsyncCursor[tuple[Any, ...]],
    sink: PostgresSink,
    table: sql.Identifier,
    writes: SinkWrites,
) -> None:
    """A table without a key holds the one row of a global aggregate."""
    if not (writes.upserts or writes.deletes):
        return
    await cur.execute(sql.SQL("DELETE FROM {}").format(table))
    expected = 0 if writes.inserts else 1
    if cur.rowcount != expected:
        raise SinkError(
            f"{sink.table}: expected {expected} existing row(s), found {cur.rowcount}; "
            "the table and the view have drifted apart"
        )
    for row in writes.upserts:
        cols = sql.SQL(", ").join(map(sql.Identifier, sink.columns))
        await cur.execute(
            sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                table, cols, sql.SQL(", ").join(sql.Placeholder() * len(sink.columns))
            ),
            [row[c] for c in sink.columns],
        )


async def _results(cur: psycopg.AsyncCursor[tuple[Any, ...]]) -> list[tuple[Any, ...] | None]:
    out = []
    while True:
        out.append(await cur.fetchone())
        if not cur.nextset():
            return out


def _expect(
    sink: PostgresSink,
    what: str,
    items: Sequence[object],
    results: Sequence[tuple[Any, ...] | None],
    ok: Any,
) -> None:
    for item, result in zip(items, results, strict=True):
        if not ok(result):
            raise SinkError(
                f"{sink.table}: {what} of {item!r} did not match the table (got {result!r}); "
                "the table and the view have drifted apart, or the view is not unique on the key"
            )
