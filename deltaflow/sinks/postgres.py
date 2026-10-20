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

**Memory.** Bound with a `spill` keyspace, the sink keeps at most
`spill_after` deltas and `spill_after` keys' net writes in memory. Beyond
that, deltas move into the keyspace ordered by (time, key) and are folded a
key at a time, and the net writes move there too and are written in chunks
(`take_chunks`). A ten-million-row transaction, or a bootstrap's whole view,
then never sits in memory. All of it still commits with the watermark in one
transaction.
"""

from __future__ import annotations

import pickle
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from itertools import groupby, takewhile
from typing import Any, ClassVar

import psycopg
from psycopg import sql
from psycopg.adapt import Dumper

from deltaflow.core import (
    Delta,
    Frontier,
    FrontierViolationError,
    MutableFrontier,
    Row,
    consolidate,
)
from deltaflow.core.values import Json, Jsonb, to_array, to_param
from deltaflow.faults import crash_point
from deltaflow.replication.changes import register_value_loaders
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks.base import Sink
from deltaflow.state.codec import decode_row, encode_key, encode_row
from deltaflow.state.store import Keyspace

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]

WATERMARKS = "_deltaflow.watermarks"
SHARD_COLUMN = "shard"
#: past this many buffered deltas or keys, a bound sink spills (`bind`)
SPILL_AFTER = 100_000
#: rows per chunk of a flush's writes (`take_chunks`)
WRITE_CHUNK = 10_000

# The spill keyspace: pending deltas under _PENDING + time + key + seq,
# net writes under _WRITES + key.
_PENDING, _WRITES = b"p", b"w"


class SinkError(RuntimeError):
    """The view's output cannot be written to its table as declared."""


class FencedError(SinkError):
    """This runner's lease on its shard was taken over; it must not write."""


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
        #: sharded (M7): this sink writes one shard's partial rows into
        #: `write_table`, with the shard number in its first key column
        self.shard: int | None = None
        self.write_table = table
        self._frontier = MutableFrontier(Frontier.at(Lsn(0)))
        self._pending: list[Delta[Row]] = []
        self._writes: dict[bytes, _KeyState] = {}
        self._bound = False
        self._spill: Keyspace | None = None
        self._spill_after = SPILL_AFTER
        #: some pending deltas, or the net writes, live in the spill keyspace
        self._pending_spilled = False
        self._writes_spilled = False
        self._seq = 0

    def bind(
        self,
        *,
        view: str,
        columns: tuple[str, ...],
        key: tuple[str, ...],
        watermark: Lsn | None,
        shard: int | None = None,
        write_table: str | None = None,
        spill: Keyspace | None = None,
        spill_after: int = SPILL_AFTER,
    ) -> None:
        """Called by the runner with the table's shape and stored watermark.
        `columns` and `key` are the view's, without the shard column. With
        `spill` (a state store keyspace this sink owns), buffers beyond
        `spill_after` move there.

        Binding starts a session: whatever an earlier session buffered and
        never flushed is dropped, and the frontier starts over. A runner
        restarting in-process replays from its checkpoint, and must not
        apply the failed session's leftovers on top of the replay."""
        self.view, self.columns, self.key, self.watermark = view, columns, key, watermark
        self.shard = shard
        self.write_table = write_table or self.table
        self._frontier = MutableFrontier(Frontier.at(Lsn(0)))
        self._pending = []
        self._writes = {}
        self._bound = True
        if spill_after < 1:
            raise ValueError("spill_after must be at least 1")
        self._spill, self._spill_after = spill, spill_after
        if spill is not None:
            spill.clear()
        self._pending_spilled = self._writes_spilled = False

    @property
    def db_columns(self) -> tuple[str, ...]:
        return (SHARD_COLUMN, *self.columns) if self.shard is not None else self.columns

    @property
    def db_key(self) -> tuple[str, ...]:
        return (SHARD_COLUMN, *self.key) if self.shard is not None else self.key

    def db_values(self, row: Row) -> list[Any]:
        values = [to_param(row[c]) for c in self.columns]
        return [self.shard, *values] if self.shard is not None else values

    def db_key_values(self, key: tuple[Any, ...]) -> tuple[Any, ...]:
        key = tuple(to_param(v) for v in key)
        return (self.shard, *key) if self.shard is not None else key

    @property
    def frontier(self) -> Frontier:
        return self._frontier.current

    @property
    def buffered(self) -> int:
        """Deltas and keys held in memory (not spilled)."""
        return len(self._pending) + len(self._writes)

    def accept(self, batch: Sequence[Delta[Row]]) -> None:
        if not self._bound:
            raise SinkError(f"sink {self.table} is not bound (the runner binds it at start)")
        for d in batch:
            if not self.frontier.admits(d.time):
                raise FrontierViolationError(
                    f"sink {self.table} received a delta at {d.time}, behind {self.frontier}"
                )
        self._pending.extend(batch)
        if self._spill is not None and len(self._pending) >= self._spill_after:
            self._spill_pending(self._pending)
            self._pending = []

    def advance(self, frontier: Frontier) -> None:
        self._frontier.advance(frontier)
        ready = [d for d in self._pending if frontier.is_complete(d.time)]
        if not ready and not self._spilled_before(frontier):
            return
        self._pending = [d for d in self._pending if not frontier.is_complete(d.time)]
        replay = (
            self.watermark is not None
            and frontier.lower is not None
            and frontier.lower <= self.watermark
        )  # replayed transactions: the table already has them
        if self._pending_spilled:
            # Every complete delta goes through the keyspace, so a time's
            # deltas are folded together, a key at a time, in order.
            self._spill_pending(ready)
            self._fold_spilled(frontier, drop=replay)
            return
        if replay:
            return
        by_time: dict[Lsn, list[Delta[Row]]] = {}
        for d in consolidate(ready):
            by_time.setdefault(d.time, []).append(d)
        for t in sorted(by_time):
            self._fold(by_time[t])

    def _fold(self, batch: list[Delta[Row]]) -> None:
        """One complete transaction, a key at a time."""
        by_key: dict[bytes, list[Delta[Row]]] = {}
        for d in batch:
            self._check_columns(d.row)
            by_key.setdefault(encode_key(tuple(d.row[c] for c in self.key)), []).append(d)
        for kb, deltas in by_key.items():
            self._fold_key(kb, deltas)

    def _fold_key(self, kb: bytes, deltas: Iterable[Delta[Row]]) -> None:
        """One key's deltas of one complete transaction. It may retract the
        row the table has, add the row the table will have, or both.
        Anything else means the view is not unique on the table's key."""
        added: Row | None = None
        removed = False
        key: tuple[Any, ...] = ()
        for d in deltas:
            key = tuple(d.row[c] for c in self.key)
            if abs(d.diff) > 1:
                raise SinkError(
                    f"{self.table}: row {d.row!r} has multiplicity {d.diff}; the view is not "
                    f"unique on the table's key {self.key}"
                )
            if d.diff < 0:
                if removed:
                    raise self._not_unique(key, "retracts two rows")
                removed = True
            elif added is not None:
                raise self._not_unique(key, "has two rows")
            else:
                added = d.row
        state = self._state_of(kb)
        had = (state.row is not None) if state is not None else removed
        if removed and not had:
            raise self._not_unique(key, "retracts a row the table does not have")
        if added is not None and had and not removed:
            raise self._not_unique(key, "would be a second row")
        if state is None:
            state = _KeyState(key, existed=removed, row=None)
        state.row = added
        self._set_state(kb, state)

    # --- spilling ----------------------------------------------------------------

    def _spill_pending(self, deltas: Iterable[Delta[Row]]) -> None:
        assert self._spill is not None
        for d in deltas:
            self._check_columns(d.row)
            self._seq += 1
            self._spill.put(
                _PENDING
                + int(d.time).to_bytes(8, "big")
                + encode_key(tuple(d.row[c] for c in self.key))
                + self._seq.to_bytes(8, "big"),
                pickle.dumps((encode_row(d.row), d.diff), protocol=5),
            )
            self._pending_spilled = True

    def _spilled_before(self, frontier: Frontier) -> bool:
        if not self._pending_spilled:
            return False
        assert self._spill is not None
        first = self._spill.first(_PENDING)
        return first is not None and frontier.is_complete(_time_of(first[0]))

    def _fold_spilled(self, frontier: Frontier, *, drop: bool) -> None:
        """Fold (or, replayed, drop) the spilled deltas of complete times:
        in (time, key) order, one key's deltas of one time at a time."""
        assert self._spill is not None
        done: bytes | None = None
        complete = takewhile(
            lambda kv: frontier.is_complete(_time_of(kv[0])), self._spill.scan(_PENDING)
        )
        for (_time, kb), group in groupby(complete, key=lambda kv: (kv[0][1:9], kv[0][9:-8])):
            batch = []
            for k, raw in group:
                done = k
                row, diff = pickle.loads(raw)
                batch.append(Delta(decode_row(row), _time_of(k), diff))
            # A row retracted and re-added in one transaction cancels out:
            # no change to the key at all.
            if not drop and (deltas := consolidate(batch)):
                self._fold_key(kb, deltas)
        if done is not None:
            self._spill.clear_range(_PENDING, done + bytes(1))
        self._pending_spilled = self._spill.first(_PENDING) is not None

    def _state_of(self, kb: bytes) -> _KeyState | None:
        if not self._writes_spilled:
            return self._writes.get(kb)
        assert self._spill is not None
        raw = self._spill.get(_WRITES + kb)
        return None if raw is None else _load_state(raw)

    def _set_state(self, kb: bytes, state: _KeyState) -> None:
        if self._writes_spilled:
            assert self._spill is not None
            self._spill.put(_WRITES + kb, _dump_state(state))
            return
        self._writes[kb] = state
        if self._spill is not None and len(self._writes) >= self._spill_after:
            for k, s in self._writes.items():
                self._spill.put(_WRITES + k, _dump_state(s))
            self._writes = {}
            self._writes_spilled = True

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
        for chunk in self.take_chunks():
            writes.inserts.extend(chunk.inserts)
            writes.updates.extend(chunk.updates)
            writes.deletes.extend(chunk.deletes)
        return writes

    def take_chunks(self, rows: int = WRITE_CHUNK) -> Iterator[SinkWrites]:
        """The net writes since the last take, `rows` keys at a time, and
        forget them."""
        if self._writes_spilled:
            assert self._spill is not None
            states: Iterable[_KeyState] = (_load_state(raw) for _, raw in self._spill.scan(_WRITES))
        else:
            states = list(self._writes.values())
            self._writes = {}
        chunk, n = SinkWrites(), 0
        for state in states:
            if state.row is None:
                if not state.existed:
                    continue
                chunk.deletes.append(state.key)
            elif state.existed:
                chunk.updates.append(state.row)
            else:
                chunk.inserts.append(state.row)
            n += 1
            if n >= rows:
                yield chunk
                chunk, n = SinkWrites(), 0
        if n:
            yield chunk
        if self._writes_spilled:
            assert self._spill is not None
            self._spill.clear_prefix(_WRITES)
            self._writes_spilled = False


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
    conn: Conn,
    sinks: Sequence[PostgresSink],
    lsn: Lsn,
    *,
    replace: bool = False,
    fence: tuple[int, int] | None = None,
) -> int:
    """Write every sink's net changes and move its watermark to `lsn`, all in
    one transaction. With `replace`, each table is emptied first (the initial
    load of a fresh snapshot).

    Every write is checked against the table. An insert must find no row, an
    update or delete exactly one. A mismatch means the table and the view
    have drifted apart (or the view is not unique on the key): it raises and
    the transaction rolls back. Returns the number of rows written.

    With `fence=(shard, epoch)` the transaction first share-locks that lease
    row and requires the epoch to still be ours. A takeover bumps the epoch
    under an exclusive lock on the same row, so it is ordered against every
    flush: after it commits, an old owner's flush finds the new epoch and
    raises FencedError. An old owner can never write behind a new one."""
    for sink in sinks:
        if sink.watermark is not None and lsn < sink.watermark:
            raise SinkError(
                f"{sink.table}: refusing to move the watermark back from {sink.watermark} to {lsn}"
            )
    _register_dumpers(conn)
    written = 0
    async with conn.transaction(), conn.cursor() as cur:
        if fence is not None:
            await cur.execute(
                "SELECT 1 FROM _deltaflow.shard_leases WHERE shard = %s AND epoch = %s FOR SHARE",
                fence,
            )
            if await cur.fetchone() is None:
                raise FencedError(f"lease on shard {fence[0]} (epoch {fence[1]}) was taken over")
        for sink in sinks:
            table = sql.Identifier(*sink.write_table.split(".", 1))
            if replace and sink.shard is not None:
                await cur.execute(
                    sql.SQL("DELETE FROM {} WHERE {} = %s").format(
                        table, sql.Identifier(SHARD_COLUMN)
                    ),
                    (sink.shard,),
                )
            elif replace:
                await cur.execute(sql.SQL("DELETE FROM {}").format(table))
            if sink.db_key:
                for chunk in sink.take_chunks():
                    await _write_keyed(cur, sink, table, chunk)
                    written += len(chunk.upserts) + len(chunk.deletes)
            else:
                writes = sink.take()
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


#: Postgres allows at most 65535 bind parameters per statement.
_MAX_PARAMS = 65_535


def _chunks[T](items: Sequence[T], per_item: int) -> list[Sequence[T]]:
    size = max(1, _MAX_PARAMS // max(1, per_item))
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _write_keyed(
    cur: psycopg.AsyncCursor[tuple[Any, ...]],
    sink: PostgresSink,
    table: sql.Identifier,
    writes: SinkWrites,
) -> None:
    """One multi-row statement per kind of write, not one statement per row.
    Per-row statements, even pipelined with executemany, cost about 45 ms
    per batch on the benchmark machine whatever the batch size; a single
    statement is one round trip. RETURNING gives the keys Postgres actually
    touched, matched against what the sink expected (by canonical key, since
    RETURNING order is unspecified)."""
    keys = sql.SQL(", ").join(map(sql.Identifier, sink.db_key))
    nkey = len(sink.db_key)
    for chunk in _chunks(writes.deletes, nkey):
        values = sql.SQL(", ").join(
            sql.SQL("({})").format(sql.SQL(", ").join(sql.Placeholder() * nkey)) for _ in chunk
        )
        await cur.execute(
            sql.SQL("DELETE FROM {} WHERE ({}) IN (VALUES {}) RETURNING {}").format(
                table, keys, values, keys
            ),
            [v for key in chunk for v in sink.db_key_values(key)],
        )
        deleted = {encode_key(map(to_array, r)) for r in await cur.fetchall()}
        for key in chunk:
            if encode_key(sink.db_key_values(key)) not in deleted:
                raise _drift(sink, "delete", key, "no such row")
    if not writes.upserts:
        return

    def db_key(r: Row) -> bytes:
        return encode_key(sink.db_key_values(tuple(r[c] for c in sink.key)))

    expected = {db_key(r): True for r in writes.inserts}
    expected |= {db_key(r): False for r in writes.updates}
    cols = sql.SQL(", ").join(map(sql.Identifier, sink.db_columns))
    rest = [c for c in sink.db_columns if c not in sink.db_key]
    # With every column in the key, "update" sets a key column to itself, so
    # that RETURNING still reports the row.
    assigned = rest or [sink.db_key[0]]
    assignment = sql.SQL(", ").join(
        sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in assigned
    )
    width = len(sink.db_columns)
    for rows in _chunks(writes.upserts, width):
        values = sql.SQL(", ").join(
            sql.SQL("({})").format(sql.SQL(", ").join(sql.Placeholder() * width)) for _ in rows
        )
        # (xmax = 0) is true for a freshly inserted row, false for an updated one.
        await cur.execute(
            sql.SQL(
                "INSERT INTO {} ({}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {} "
                "RETURNING {}, (xmax = 0)"
            ).format(table, cols, values, keys, assignment, keys),
            [v for r in rows for v in sink.db_values(r)],
        )
        for returned in await cur.fetchall():
            kb = encode_key(map(to_array, returned[:nkey]))
            inserted = returned[nkey]
            if kb not in expected or expected[kb] != inserted:
                what = "insert" if expected.get(kb, inserted) else "update"
                raise _drift(
                    sink, what, tuple(returned[:nkey]), "it was " + ("new" if inserted else "there")
                )


class _JsonDumper(Dumper):
    oid = 114

    def dump(self, obj: Json) -> bytes:
        return obj.text.encode()


class _JsonbDumper(Dumper):
    oid = 3802

    def dump(self, obj: Jsonb) -> bytes:
        return obj.text.encode()


def _register_dumpers(conn: Conn) -> None:
    """json and jsonb values written as their text, with their types, and
    read back (RETURNING keys) as the engine has them."""
    conn.adapters.register_dumper(Json, _JsonDumper)
    conn.adapters.register_dumper(Jsonb, _JsonbDumper)
    register_value_loaders(conn.adapters)


def _time_of(spilled_key: bytes) -> Lsn:
    return Lsn(int.from_bytes(spilled_key[1:9], "big"))


def _dump_state(state: _KeyState) -> bytes:
    row = None if state.row is None else encode_row(state.row)
    return pickle.dumps((state.key, state.existed, row), protocol=5)


def _load_state(raw: bytes) -> _KeyState:
    key, existed, row = pickle.loads(raw)
    return _KeyState(key, existed, None if row is None else decode_row(row))


def _drift(sink: PostgresSink, what: str, key: object, found: str) -> SinkError:
    return SinkError(
        f"{sink.table}: {what} of key {key!r} did not match the table ({found}); the table and "
        "the view have drifted apart, or the view is not unique on the key"
    )


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
            [to_param(row[c]) for c in sink.columns],
        )
