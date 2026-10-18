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
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from deltaflow.core import Row
from deltaflow.replication.changes import Row as Values
from deltaflow.replication.changes import RowDelete, RowInsert, RowUpdate
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
    def __init__(self, schema: TableSchema, keyspace: Keyspace, *, count: int = 0) -> None:
        """`count` is the number of rows already in `keyspace` (on resume)."""
        self.schema = schema
        self._rows = keyspace
        self._count = count
        self._checked: Relation | None = None
        self.counters: Counter[str] = Counter()

    def __len__(self) -> int:
        return self._count

    def rows(self) -> Iterator[Row]:
        return (decode_row(v) for _, v in self._rows.scan())

    def load(self, row: Row) -> None:
        """Add a row from the initial snapshot."""
        key = self._key(row)
        if self._get(key) is not None:
            raise SourceStateError(f"{self.schema.table}: duplicate key {key!r} in snapshot")
        self._put(key, row)

    def apply(self, change: RowInsert | RowUpdate | RowDelete) -> list[tuple[Row, int]]:
        """Apply one change; return it as whole-row (row, diff) pairs."""
        self._check(change.relation)
        match change:
            case RowInsert():
                return [(self._insert(change.new), 1)]
            case RowUpdate():
                return self._update(change)
            case RowDelete():
                old = self._take(self._key(change.old), "delete")
                if change.old_kind == "full":
                    self._cross_check(old, change.old)
                return [(old, -1)]

    def truncate(self) -> list[tuple[Row, int]]:
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
        self._put(new_key, new)
        return [(old, -1), (new, 1)]

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
