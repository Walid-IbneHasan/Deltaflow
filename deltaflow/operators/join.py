"""Incremental inner equi-join.

Each side keeps an index of its accumulated input, keyed by join key, in the
state store. A batch arriving on one side:

1. probes the *other* side's index for each delta's key, and emits the
   product with each match, at the delta's time, with diff = delta.diff ×
   the match's multiplicity;
2. then folds itself into its *own* index.

The engine delivers a step's left batch before its right batch. The right
batch therefore probes a left index that already includes ΔL, and the
total is

    ΔL ⋈ R  +  (L + ΔL) ⋈ ΔR  =  (L + ΔL) ⋈ (R + ΔR) − L ⋈ R,

exactly the change in the join. The indices store accumulated state without
times. That is valid only because every delta is at or after every time
already folded in, which `process` checks. See docs/join.md.

SQL semantics: a NULL in any key column matches nothing (NULL = NULL is not
true), so such rows produce no output and are not indexed.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from deltaflow.core import Delta, FrontierViolationError, Row
from deltaflow.operators.base import Operator
from deltaflow.replication.lsn import Lsn
from deltaflow.state.codec import decode_row, encode_key, encode_row, encode_row_key
from deltaflow.state.store import Keyspace, StateStore

_COUNT = 8  # bytes of signed multiplicity in front of each index value


class JoinStateError(RuntimeError):
    """A join input retracted more copies of a row than it ever added."""


class Join(Operator):
    def __init__(
        self,
        left: Operator,
        right: Operator,
        *,
        left_on: tuple[str, ...],
        right_on: tuple[str, ...],
        left_prefix: str | None,
        right_prefix: str,
        store: StateStore,
        name: str,
    ) -> None:
        """`left_prefix` qualifies left column names (`orders.amount`); None
        means the left rows are already qualified (the output of an earlier
        join). Right columns are always qualified with `right_prefix`."""
        super().__init__(left, right)
        if len(left_on) != len(right_on) or not left_on:
            raise ValueError("join needs the same, non-zero number of key columns on each side")
        self.name = name
        self._on = (left_on, right_on)
        self._prefixes = (left_prefix, right_prefix)
        self._store = store
        self._index = (store.keyspace(f"join:{name}:left"), store.keyspace(f"join:{name}:right"))
        self._latest = Lsn(0)

    def process(self, port: int, batch: Sequence[Delta[Row]]) -> list[Delta[Row]]:
        own, other = self._index[port], self._index[1 - port]
        columns = self._on[port]
        probes: dict[bytes, list[tuple[Row, int]]] = {}
        pending: dict[bytes, tuple[int, Row]] = {}
        out: list[Delta[Row]] = []
        for d in batch:
            if d.time < self._latest:
                raise FrontierViolationError(
                    f"join {self.name}: delta at {d.time} arrived after state at "
                    f"{self._latest} was folded in"
                )
            self._latest = d.time
            key = tuple(d.row[c] for c in columns)
            if any(v is None for v in key):
                continue
            kb = encode_key(key)
            matches = probes.get(kb)
            if matches is None:
                matches = probes[kb] = [_decode(v) for _, v in other.scan(kb)]
            for match, n in matches:
                out.append(Delta(self._combine(port, d.row, match), d.time, d.diff * n))
            entry = kb + encode_row_key(d.row)
            seen, _ = pending.get(entry, (0, d.row))
            pending[entry] = (seen + d.diff, d.row)
        self._fold(own, pending)
        return out

    def _fold(self, own: Keyspace, pending: dict[bytes, tuple[int, Row]]) -> None:
        with self._store.batch() as batch:
            for entry, (diff, row) in pending.items():
                if diff == 0:
                    continue
                current = own.get(entry)
                n = diff + (_decode(current)[1] if current is not None else 0)
                if n < 0:
                    raise JoinStateError(
                        f"join {self.name}: {row!r} would have multiplicity {n} "
                        "(a retraction of a row this side never contained)"
                    )
                if n == 0:
                    batch.delete(own, entry)
                else:
                    batch.put(own, entry, n.to_bytes(_COUNT, "big", signed=True) + encode_row(row))

    def _combine(self, port: int, delta_row: Row, match: Row) -> Row:
        left, right = (delta_row, match) if port == 0 else (match, delta_row)
        merged: dict[str, Any] = {}
        for prefix, row in zip(self._prefixes, (left, right), strict=True):
            if prefix is None:
                merged.update(row)
            else:
                merged.update((f"{prefix}.{c}", v) for c, v in row.items())
        return Row(merged)


def _decode(value: bytes) -> tuple[Row, int]:
    return decode_row(value[_COUNT:]), int.from_bytes(value[:_COUNT], "big", signed=True)
