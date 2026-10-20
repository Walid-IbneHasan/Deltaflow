"""Source arrangements: Deltaflow's own keyed copy of each source table.

With the default replica identity, pgoutput sends:

* for an UPDATE, the new row with every out-of-line (TOASTed) column the
  statement did not touch replaced by the `UNCHANGED_TOAST` placeholder,
  plus the old *key* only if the key changed;
* for a DELETE, only the old key.

A view has to retract the whole old row (an UPDATE is (old, -1), (new, +1)),
so the arrangement keeps every current row keyed by its replica identity
and fills in what the stream leaves out. The same lookup that supplies the
old row also supplies the unchanged TOAST values for the new one. See
docs/decisions/0003-source-arrangements.md for why this beats `REPLICA
IDENTITY FULL`.

The arrangement is also a consistency check on the stream. An INSERT of an
existing key, or an UPDATE or DELETE of a missing one, means a change was
lost or duplicated upstream, so it raises instead of guessing.

Rows live in the state store (M3), keyed by the canonical encoding of the
key columns, so an arrangement may be far larger than memory.

**Loading in chunks (decision 0009).** With `marks`, the arrangement is
being loaded while the stream runs: chunks read at different consistent
points, changes from the stream, and per key the newest writer wins.
`finish_loading` turns it into an ordinary arrangement.

**Sharded (M7).** With `owns`, the arrangement keeps only the rows its shard
owns. A change whose shard key moves a row between shards becomes a delete
here and an insert there. The consistency checks are necessarily weaker in
this mode: an UPDATE or DELETE of a key this shard does not have is someone
else's row, not a lost change. One case cannot be handled. A row that moves
*in* with an unchanged-TOAST placeholder has its value on the shard it came
from, so it raises.
"""

from __future__ import annotations

import contextlib
import pickle
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from deltaflow.core import Row
from deltaflow.replication.changes import Row as Values
from deltaflow.replication.changes import RowDelete, RowInsert, RowUpdate
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Relation
from deltaflow.state.codec import decode_row, encode_key, encode_row
from deltaflow.state.store import Keyspace


class SourceStateError(RuntimeError):
    """The change stream contradicts the arrangement: a change was lost,
    duplicated or applied twice upstream."""


class SchemaChangedError(SourceStateError):
    """A source table's columns changed while views were running on it."""


@dataclass(frozen=True, slots=True)
class TableSchema:
    #: schema-qualified, as `Relation.qualified_name`
    table: str
    #: in table order, as pgoutput sends them
    columns: tuple[str, ...]
    #: the columns that identify a row (the replica identity, or the
    #: primary key for REPLICA IDENTITY FULL)
    key: tuple[str, ...]


type Key = tuple[Any, ...]


class TableArrangement:
    def __init__(
        self,
        schema: TableSchema,
        keyspace: Keyspace,
        *,
        count: int = 0,
        owns: Callable[[Mapping[str, Any]], bool] | None = None,
        marks: Keyspace | None = None,
        rereads: Keyspace | None = None,
    ) -> None:
        """`count` is the number of rows already in `keyspace` (on resume).
        `owns` says whether a row belongs to this arrangement's shard.
        With `marks` and `rereads` (both or neither), it is loading: they
        hold each key's newest writer and the keys to read again."""
        if (marks is None) != (rereads is None):
            raise ValueError("a loading arrangement needs both marks and rereads")
        self.schema = schema
        self._rows = keyspace
        self._count = count
        self._owns = owns
        self._marks = marks
        self._rereads = rereads
        self._checked: Relation | None = None
        self.counters: Counter[str] = Counter()

    def __len__(self) -> int:
        return self._count

    def rows(self) -> Iterator[Row]:
        return (decode_row(v) for _, v in self._rows.scan())

    def load(self, row: Row) -> bool:
        """Add a row from the initial snapshot; False if another shard owns it."""
        self._not_loading()
        if self._owns is not None and not self._owns(row):
            return False
        key = self._key(row)
        if self._get(key) is not None:
            raise SourceStateError(f"{self.schema.table}: duplicate key {key!r} in snapshot")
        self._put(key, row)
        return True

    def apply(self, change: RowInsert | RowUpdate | RowDelete) -> list[tuple[Row, int]]:
        """Apply one change; return it as whole-row (row, diff) pairs."""
        self._not_loading()
        self._check(change.relation)
        match change:
            case RowInsert():
                if self._owns is not None and not self._owns(change.new):
                    return []
                return [(self._insert(change.new), 1)]
            case RowUpdate():
                return self._update(change)
            case RowDelete():
                if self._owns is not None and self._get(self._key(change.old)) is None:
                    return []  # another shard's row
                old = self._take(self._key(change.old), "delete")
                if change.old_kind == "full":
                    self._cross_check(old, change.old)
                return [(old, -1)]

    def truncate(self) -> list[tuple[Row, int]]:
        self._not_loading()
        out = [(r, -1) for r in self.rows()]
        self._rows.clear()
        self._count = 0
        return out

    def _insert(self, values: Values) -> Row:
        if any(v is UNCHANGED_TOAST for v in values.values()):
            raise SourceStateError(
                f"{self.schema.table}: unchanged-TOAST placeholder in an INSERT {values!r}"
            )
        row = Row(values)
        key = self._key(row)
        if self._get(key) is not None:
            raise SourceStateError(f"{self.schema.table}: insert of existing key {key!r}")
        self._put(key, row)
        return row

    def _update(self, change: RowUpdate) -> list[tuple[Row, int]]:
        # The old key is in `old` when the key changed (or for FULL);
        # otherwise it is unchanged and can be read from `new`.
        old_key = self._key(change.old if change.old is not None else change.new)
        if self._owns is not None and self._get(old_key) is None:
            return self._move_in(change)
        old = self._take(old_key, "update")
        if change.old_kind == "full" and change.old is not None:
            self._cross_check(old, change.old)
        resolved = 0
        values: dict[str, Any] = {}
        for col, v in change.new.items():
            if v is UNCHANGED_TOAST:
                values[col] = old[col]
                resolved += 1
            else:
                values[col] = v
        self.counters["unchanged_toast_resolved"] += resolved
        new = Row(values)
        new_key = self._key(new)
        if encode_key(new_key) != encode_key(old_key) and self._get(new_key) is not None:
            self._put(old_key, old)  # leave the arrangement as it was
            raise SourceStateError(
                f"{self.schema.table}: update of {old_key!r} moves onto existing key {new_key!r}"
            )
        if self._owns is not None and not self._owns(new):
            self.counters["moved_out"] += 1
            return [(old, -1)]  # the row now lives on another shard
        self._put(new_key, new)
        return [(old, -1), (new, 1)]

    def _move_in(self, change: RowUpdate) -> list[tuple[Row, int]]:
        """An UPDATE of a row this shard does not have: either another shard's
        row that stays there, or one moving in."""
        assert self._owns is not None
        if any(v is UNCHANGED_TOAST for v in change.new.values()):
            # Is it moving in? Only decidable if the shard key itself was sent.
            try:
                moving_in = self._owns(change.new)
            except (KeyError, TypeError):
                moving_in = True
            if not moving_in:
                return []
            raise SourceStateError(
                f"{self.schema.table}: a row moved into shard with an unchanged-TOAST "
                "placeholder; its value is on the shard it came from. Shard by a column "
                "that does not change, or keep large columns out of sharded sources"
            )
        if not self._owns(change.new):
            return []
        self.counters["moved_in"] += 1
        return [(self._insert(change.new), 1)]

    def _get(self, key: Key) -> Row | None:
        raw = self._rows.get(encode_key(key))
        return None if raw is None else decode_row(raw)

    def _put(self, key: Key, row: Row) -> None:
        self._rows.put(encode_key(key), encode_row(row))
        self._count += 1

    def _take(self, key: Key, what: str) -> Row:
        row = self._get(key)
        if row is None:
            raise SourceStateError(f"{self.schema.table}: {what} of unknown key {key!r}")
        self._rows.delete(encode_key(key))
        self._count -= 1
        return row

    def _key(self, values: Values | Row) -> Key:
        key = tuple(values[c] for c in self.schema.key)
        if any(v is UNCHANGED_TOAST for v in key):
            raise SourceStateError(
                f"{self.schema.table}: key column sent as unchanged-TOAST placeholder"
            )
        return key

    def _cross_check(self, stored: Row, sent: Values) -> None:
        # REPLICA IDENTITY FULL sends the old row too; it must match ours
        # (except where it is a placeholder itself).
        for col, v in sent.items():
            if v is not UNCHANGED_TOAST and stored[col] != v:
                raise SourceStateError(
                    f"{self.schema.table}: old row from the server disagrees with the stored "
                    f"row in column {col!r}: {v!r} != {stored[col]!r}"
                )

    # --- loading in chunks (decision 0009) -------------------------------------

    @property
    def loading(self) -> bool:
        return self._marks is not None

    def load_chunk(self, rows: Iterable[Row], cp: Lsn) -> int:
        """Merge rows read with an exported snapshot at consistent point `cp`.
        Every change applied so far commits before `cp`, so the chunk is the
        newest writer of every key it returns. Returns the rows kept (this
        shard's)."""
        marks = self._loading_marks()
        kept = 0
        for row in rows:
            key = self._key(row)
            kb = encode_key(key)
            marks.put(kb, _lsn_bytes(cp))
            if self._owns is not None and not self._owns(row):
                self._discard(kb)
                continue
            self._set(kb, row)
            kept += 1
        return kept

    def apply_loading(self, change: RowInsert | RowUpdate | RowDelete, lsn: Lsn) -> None:
        """Apply a change committed at `lsn` while loading: per key, the newest
        writer wins (decision 0009, fork 2). Produces no deltas."""
        self._check(change.relation)
        match change:
            case RowInsert():
                self._insert_loading(change.new, lsn)
            case RowUpdate():
                self._update_loading(change, lsn)
            case RowDelete():
                key = self._key(change.old)
                kb = encode_key(key)
                if self._writer(kb, lsn) is not _MARKED:
                    return  # a newer chunk has the key, or no chunk read it yet
                stored = self._get_enc(kb)
                if stored is None:
                    if self._owns is None:
                        raise SourceStateError(
                            f"{self.schema.table}: delete of unknown key {key!r}"
                        )
                    return  # another shard's row
                self._discard(kb)
                if change.old_kind == "full":
                    self._cross_check(stored, change.old)

    def truncate_loading(self, lsn: Lsn) -> None:
        """Remove every row whose newest writer is not newer than the
        TRUNCATE; rows from chunks read after it stay."""
        marks = self._loading_marks()
        for kb, _ in self._rows.scan():  # the iterator reads a snapshot
            mark = marks.get(kb)
            if mark is None or _lsn_of(mark) <= lsn:
                self._discard(kb)

    def rereads(self) -> list[Key]:
        """Keys whose row the loader must read again (`reread`)."""
        assert self._rereads is not None
        return [pickle.loads(v) for _, v in self._rereads.scan()]

    def reread(self, keys: Sequence[Key], rows: Iterable[Row], cp: Lsn) -> None:
        """Merge the rows of `keys` read again at `cp`; a key without a row is gone."""
        marks = self._loading_marks()
        assert self._rereads is not None
        found = {encode_key(self._key(r)): r for r in rows}
        for key in keys:
            kb = encode_key(key)
            marks.put(kb, _lsn_bytes(cp))
            self._rereads.delete(kb)
            row = found.get(kb)
            if row is None or (self._owns is not None and not self._owns(row)):
                self._discard(kb)
            else:
                self._set(kb, row)

    def finish_loading(self) -> None:
        """Every chunk is in, no re-read is pending, and the stream is past
        every chunk's consistent point: the rows are the table. From here
        on, changes are applied live (`apply`)."""
        marks = self._loading_marks()
        assert self._rereads is not None
        if self._rereads.first() is not None:
            raise RuntimeError(f"{self.schema.table}: re-reads still pending")
        marks.clear()
        self._marks = self._rereads = None

    def _loading_marks(self) -> Keyspace:
        if self._marks is None:
            raise RuntimeError(f"{self.schema.table} is not loading")
        return self._marks

    def _writer(self, kb: bytes, lsn: Lsn) -> object:
        """How a change at `lsn` relates to the key's newest writer."""
        mark = self._loading_marks().get(kb)
        if mark is None:
            return _UNREAD
        return _STALE if lsn < _lsn_of(mark) else _MARKED

    def _mark(self, kb: bytes, lsn: Lsn) -> None:
        self._loading_marks().put(kb, _lsn_bytes(lsn))

    def _insert_loading(self, values: Values, lsn: Lsn) -> None:
        key = self._key(values)
        kb = encode_key(key)
        writer = self._writer(kb, lsn)
        if writer is _STALE:
            return
        if any(v is UNCHANGED_TOAST for v in values.values()):
            raise SourceStateError(
                f"{self.schema.table}: unchanged-TOAST placeholder in an INSERT {values!r}"
            )
        row = Row(values)
        if self._get_enc(kb) is not None:
            raise SourceStateError(f"{self.schema.table}: insert of existing key {key!r}")
        # Unread: the key may lie in a page already read, which no chunk
        # will read again; the insert is its writer.
        self._mark(kb, lsn)
        if self._owns is None or self._owns(row):
            self._set(kb, row)

    def _update_loading(self, change: RowUpdate, lsn: Lsn) -> None:
        old_key = self._key(change.old if change.old is not None else change.new)
        new_key = self._key(change.new)
        old_kb, new_kb = encode_key(old_key), encode_key(new_key)
        rekey = new_kb != old_kb
        old_writer = self._writer(old_kb, lsn)
        if not rekey and old_writer is not _MARKED:
            return  # a newer chunk has the key, or a later one will read it
        old = self._take_loading(change, old_key, old_kb) if old_writer is _MARKED else None
        # An unread old key lies in a page still ahead; its new position
        # may not, so the new row is written like an insert.
        if rekey and self._writer(new_kb, lsn) is _STALE:
            return
        if old is None and any(v is UNCHANGED_TOAST for v in change.new.values()):
            # The old row is not here: another shard's, or never read.
            if self._owns is not None and old_writer is _MARKED:
                with contextlib.suppress(KeyError, TypeError):
                    if not self._owns(change.new):
                        self._mark(new_kb, lsn)
                        return  # stays on (or moves to) another shard
            self._queue_reread(new_key, new_kb)
            return
        new = Row(
            {c: old[c] if v is UNCHANGED_TOAST and old is not None else v
             for c, v in change.new.items()}
        )  # fmt: skip
        if rekey and self._get_enc(new_kb) is not None:
            raise SourceStateError(
                f"{self.schema.table}: update of {old_key!r} moves onto existing key {new_key!r}"
            )
        self._mark(new_kb, lsn)
        if self._owns is None or self._owns(new):
            self._set(new_kb, new)

    def _take_loading(self, change: RowUpdate, key: Key, kb: bytes) -> Row | None:
        """Remove the row an update replaces, for a key with a mark (so held
        exactly). None: another shard's row."""
        old = self._get_enc(kb)
        if old is None:
            if self._owns is None:
                raise SourceStateError(f"{self.schema.table}: update of unknown key {key!r}")
            return None
        self._discard(kb)
        if change.old_kind == "full" and change.old is not None:
            self._cross_check(old, change.old)
        return old

    def _queue_reread(self, key: Key, kb: bytes) -> None:
        assert self._rereads is not None
        if self._get_enc(kb) is not None:
            raise SourceStateError(
                f"{self.schema.table}: an update moves onto existing key {key!r}"
            )
        self._rereads.put(kb, pickle.dumps(key, protocol=5))
        self.counters["rereads_queued"] += 1

    def _get_enc(self, kb: bytes) -> Row | None:
        raw = self._rows.get(kb)
        return None if raw is None else decode_row(raw)

    def _set(self, kb: bytes, row: Row) -> None:
        """Write a row, replacing the key's row if it has one."""
        if self._rows.get(kb) is None:
            self._count += 1
        self._rows.put(kb, encode_row(row))

    def _discard(self, kb: bytes) -> None:
        if self._rows.get(kb) is not None:
            self._rows.delete(kb)
            self._count -= 1

    def _not_loading(self) -> None:
        if self._marks is not None:
            raise RuntimeError(f"{self.schema.table} is still loading")

    def _check(self, relation: Relation) -> None:
        if relation is self._checked:
            return
        columns = tuple(c.name for c in relation.columns)
        if columns != self.schema.columns:
            raise SchemaChangedError(
                f"{self.schema.table}: columns changed from ({', '.join(self.schema.columns)}) "
                f"to ({', '.join(columns)}); re-register the views on it"
            )
        self._checked = relation


_UNREAD, _STALE, _MARKED = object(), object(), object()


def _lsn_bytes(lsn: Lsn) -> bytes:
    return int(lsn).to_bytes(8, "big")


def _lsn_of(raw: bytes) -> Lsn:
    return Lsn(int.from_bytes(raw, "big"))
