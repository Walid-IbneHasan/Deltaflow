"""group_by + aggregates, and distinct.

Each group's state lives in the store, under the canonical encoding of the
group key:

- the number of rows;
- one state per aggregate;
- the output row last emitted.

A batch that touches a group updates the state and emits the change, the
old output with −1 and the new with +1, or nothing if they are equal. The
old output is the one stored, so it is retracted exactly as it was
emitted.

* **Count** keeps an integer.
* **Sum** keeps the count of non-NULL values and an exact total. Integer
  sums are exact anyway. Decimal sums run in a 1000-digit context that
  traps rounding, so a sum that cannot be held exactly raises instead of
  drifting. Floats and bools are refused: float addition is not
  associative, so an incremental sum could differ from SQL's recompute.
* **Min/Max** keep, per group, a multiset of the non-NULL values in their
  own keyspace, keyed by the order-preserving encoding of the value. The
  extreme is the first or last entry under the group's prefix. Deleting
  the current extreme removes its entry (or decrements its count), and the
  next entry *is* the new extreme. Nothing is recomputed from the source,
  at the price of storing every value. See docs/aggregates.md.
* **CountDistinct** keeps the same multiset; the count changes when a
  value's multiplicity crosses zero. Numerically equal values (1.5, 1.50)
  are one entry, as SQL compares them.
* **Avg** keeps Sum's count and exact total, plus a multiset of the values'
  scales: Postgres's avg is numeric_div(sum, count), whose result scale
  depends on the sum's, which is the largest scale among the values summed
  (`numeric_avg`).

SQL semantics:
- NULL group keys form one group;
- aggregates ignore NULL values;
- Sum, Min and Max of no non-NULL values are NULL;
- a group with no rows disappears;
- a global aggregate (no group keys) always has exactly one row.
"""

from __future__ import annotations

import decimal
import pickle
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Any

from deltaflow.core import Delta, FrontierViolationError, Row
from deltaflow.operators.base import Operator
from deltaflow.replication.lsn import Lsn
from deltaflow.state.codec import (
    decode_row,
    encode_key,
    encode_ordered,
    encode_row,
    encode_row_key,
)
from deltaflow.state.store import Keyspace, StateStore

_COUNT = 8
_EXACT = decimal.Context(
    prec=1000, traps=[decimal.Inexact, decimal.Overflow, decimal.InvalidOperation]
)


class AggregateError(RuntimeError):
    """An aggregate cannot be maintained exactly, or its input went negative."""


@dataclass(frozen=True, slots=True)
class Count:
    """COUNT(*) without a column, COUNT(column) (non-NULL values) with one."""

    column: str | None = None


@dataclass(frozen=True, slots=True)
class Sum:
    column: str


@dataclass(frozen=True, slots=True)
class Min:
    column: str


@dataclass(frozen=True, slots=True)
class Max:
    column: str


@dataclass(frozen=True, slots=True)
class Avg:
    """AVG(column) of int and Decimal values, exactly as Postgres computes it."""

    column: str


@dataclass(frozen=True, slots=True)
class CountDistinct:
    """COUNT(DISTINCT column): the number of distinct non-NULL values."""

    column: str


type Aggregate = Count | Sum | Min | Max | Avg | CountDistinct

#: from Postgres's numeric.c: significant digits a division keeps at least,
#: decimal digits per base-10000 digit, and the largest display scale
_MIN_SIG_DIGITS, _DEC_DIGITS, _MAX_DISPLAY_SCALE = 16, 4, 1000


def scale_of(value: int | Decimal) -> int:
    """A number's display scale: digits after the decimal point."""
    if isinstance(value, int):
        return 0
    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int), f"not a finite number: {value!r}"
    return max(0, -exponent)


def _weight_and_first(value: int | Decimal) -> tuple[int, int]:
    """Postgres's numeric layout of |value| in base 10000: the weight of its
    first non-zero digit, and that digit (0, 0 for zero)."""
    if value == 0:
        return 0, 0
    magnitude = abs(Fraction(value))
    # copy_abs, not abs(): abs rounds to the context's 28 digits
    weight = Decimal(value).copy_abs().adjusted() // _DEC_DIGITS
    return weight, int(magnitude / Fraction(10) ** (_DEC_DIGITS * weight))


def numeric_avg(total: int | Decimal, n: int, scale: int) -> Decimal:
    """`total / n` as Postgres's avg returns it: numeric_div(sum, count).

    The result scale is select_div_scale's: enough decimals for 16
    significant digits (from an estimate of the quotient's weight), but no
    fewer than the sum's scale (`scale`) and at most 1000. The quotient is
    exact and rounded half away from zero (div_var with round = true)."""
    weight1, first1 = _weight_and_first(total)
    weight2, first2 = _weight_and_first(n)
    qweight = weight1 - weight2 - (1 if first1 <= first2 else 0)
    rscale = min(max(_MIN_SIG_DIGITS - qweight * _DEC_DIGITS, scale, 0), _MAX_DISPLAY_SCALE)
    scaled = Fraction(total) / n * 10**rscale
    magnitude = (2 * abs(scaled.numerator) + scaled.denominator) // (2 * scaled.denominator)
    sign = 1 if scaled < 0 and magnitude else 0
    return Decimal((sign, tuple(int(c) for c in str(magnitude)), -rscale))


@dataclass(slots=True)
class _Group:
    rows: int
    #: per aggregate: Count -> non-NULL count; Sum -> (non-NULL count, total);
    #: Avg -> (non-NULL count, total, {scale: count of values});
    #: Min/Max -> non-NULL count; CountDistinct -> distinct values (the values
    #: themselves are in the keyspace)
    states: list[Any]
    out: dict[str, Any] | None


def _check_time(name: str, latest: Lsn, t: Lsn) -> None:
    if t < latest:
        raise FrontierViolationError(
            f"{name}: delta at {t} arrived after state at {latest} was folded in"
        )


def _by_time(batch: Sequence[Delta[Row]]) -> Iterator[tuple[Lsn, list[Delta[Row]]]]:
    times: dict[Lsn, list[Delta[Row]]] = {}
    for d in batch:
        times.setdefault(d.time, []).append(d)
    for t in sorted(times):
        yield t, times[t]


class GroupBy(Operator):
    def __init__(
        self,
        input: Operator,
        *,
        keys: tuple[str, ...],
        aggregates: dict[str, Aggregate],
        store: StateStore,
        name: str,
        names: tuple[str, ...] | None = None,
    ) -> None:
        """`names` are the output column names of `keys` (default: the same)."""
        super().__init__(input)
        self.name = name
        self.keys = keys
        self.names = names or keys
        self.aggregates = aggregates
        self._groups = store.keyspace(f"agg:{name}:groups")
        self._values = store.keyspace(f"agg:{name}:values")
        self._latest = Lsn(0)

    def initial(self) -> list[Delta[Row]]:
        """A global aggregate has a row before any input arrives."""
        if self.keys:
            return []
        group = self._new_group()
        group.out = dict(self._output((), b"", group))
        self._save(b"", group)
        return [Delta(Row(group.out), Lsn(0), 1)]

    def process(self, port: int, batch: Sequence[Delta[Row]]) -> list[Delta[Row]]:
        out: list[Delta[Row]] = []
        for t, deltas in _by_time(batch):
            _check_time(self.name, self._latest, t)
            self._latest = t
            groups: dict[bytes, tuple[tuple[Any, ...], list[Delta[Row]]]] = {}
            for d in deltas:
                key = tuple(d.row[k] for k in self.keys)
                groups.setdefault(encode_key(key), (key, []))[1].append(d)
            for kb, (key, changes) in groups.items():
                out.extend(self._update(kb, key, changes, t))
        return out

    def _update(
        self, kb: bytes, key: tuple[Any, ...], changes: list[Delta[Row]], t: Lsn
    ) -> list[Delta[Row]]:
        raw = self._groups.get(kb)
        group = self._new_group() if raw is None else _Group(*pickle.loads(raw))
        old = group.out
        group.rows += sum(d.diff for d in changes)
        if group.rows < 0:
            raise AggregateError(f"{self.name}: group {key!r} would have a negative row count")
        for i, agg in enumerate(self.aggregates.values()):
            group.states[i] = self._fold(i, agg, kb, group.states[i], changes)
        if group.rows == 0 and self.keys:
            self._groups.delete(kb)
            new = None
        else:
            new = dict(self._output(key, kb, group))
            group.out = new
            self._save(kb, group)
        if old == new:
            return []
        emitted = [] if old is None else [Delta(Row(old), t, -1)]
        return emitted if new is None else [*emitted, Delta(Row(new), t, 1)]

    def _fold(
        self, i: int, agg: Aggregate, kb: bytes, state: Any, changes: list[Delta[Row]]
    ) -> Any:
        if isinstance(agg, Count):
            if agg.column is None:
                return state  # COUNT(*) is the group's row count
            column = agg.column
            return state + sum(d.diff for d in changes if d.row[column] is not None)
        if isinstance(agg, Sum):
            return self._fold_sum(agg.column, state, changes)
        if isinstance(agg, Avg):
            n, total, scales = state
            n, total = self._fold_sum(agg.column, (n, total), changes, what="Avg")
            for d in changes:
                v = d.row[agg.column]
                if v is not None:
                    s = scale_of(v)
                    scales[s] = scales.get(s, 0) + d.diff
                    if not scales[s]:
                        del scales[s]
            return (n, total, scales)
        if isinstance(agg, CountDistinct):
            return self._fold_distinct(i, agg.column, kb, state, changes)
        return self._fold_extreme(i, agg.column, kb, state, changes)

    def _fold_sum(
        self,
        column: str,
        state: tuple[int, int | Decimal],
        changes: list[Delta[Row]],
        what: str = "Sum",
    ) -> tuple[int, int | Decimal]:
        n, total = state
        for d in changes:
            v = d.row[column]
            if v is None:
                continue
            if isinstance(v, bool) or not isinstance(v, int | Decimal):
                raise AggregateError(
                    f"{self.name}: {what}({column!r}) got {type(v).__name__} {v!r}; only "
                    "int and Decimal are summed (float addition is not associative, "
                    "so an incremental sum could differ from a recompute)"
                )
            n += d.diff
            if isinstance(v, int) and isinstance(total, int):
                total += v * d.diff  # Python ints are exact
                continue
            try:
                total = _EXACT.add(total, _EXACT.multiply(v, d.diff))
            except decimal.DecimalException:
                raise AggregateError(
                    f"{self.name}: Sum({column!r}) cannot be held exactly in 1000 digits"
                ) from None
        return (n, total)

    def _fold_extreme(
        self, i: int, column: str, kb: bytes, n: int, changes: list[Delta[Row]]
    ) -> int:
        """Min/Max: the non-NULL count stays in the group; every value goes
        into the ordered keyspace with its multiplicity."""
        pending: dict[bytes, tuple[int, Any]] = {}
        for d in changes:
            v = d.row[column]
            if v is None:
                continue
            n += d.diff
            entry = bytes([i]) + kb + encode_ordered(v)
            seen, _ = pending.get(entry, (0, v))
            pending[entry] = (seen + d.diff, v)
        for entry, (diff, v) in pending.items():
            if diff:
                self._bump(entry, diff, v)
        return n

    def _fold_distinct(
        self, i: int, column: str, kb: bytes, distinct: int, changes: list[Delta[Row]]
    ) -> int:
        """CountDistinct: the values go into the keyspace as for Min/Max;
        the count moves when a value's multiplicity crosses zero."""
        pending: dict[bytes, tuple[int, Any]] = {}
        for d in changes:
            v = d.row[column]
            if v is not None:
                entry = bytes([i]) + kb + encode_ordered(v)
                seen, _ = pending.get(entry, (0, v))
                pending[entry] = (seen + d.diff, v)
        for entry, (diff, v) in pending.items():
            if diff:
                before, after = self._bump(entry, diff, v)
                distinct += (after > 0) - (before > 0)
        return distinct

    def _bump(self, entry: bytes, diff: int, value: Any) -> tuple[int, int]:
        current = self._values.get(entry)
        before = 0 if current is None else int.from_bytes(current[:_COUNT], "big", signed=True)
        n = before + diff
        if n < 0:
            raise AggregateError(f"{self.name}: value {value!r} would have a negative count")
        if n == 0:
            self._values.delete(entry)
        else:
            blob = current[_COUNT:] if current is not None else pickle.dumps(value, protocol=5)
            self._values.put(entry, n.to_bytes(_COUNT, "big", signed=True) + blob)
        return before, n

    def _output(self, key: tuple[Any, ...], kb: bytes, group: _Group) -> Iterator[tuple[str, Any]]:
        yield from zip(self.names, key, strict=True)
        for i, (name, agg) in enumerate(self.aggregates.items()):
            state = group.states[i]
            match agg:
                case Count(column=None):
                    yield name, group.rows
                case Count():
                    yield name, state
                case Sum():
                    n, total = state
                    yield name, total if n else None
                case Avg():
                    n, total, scales = state
                    yield name, numeric_avg(total, n, max(scales)) if n else None
                case CountDistinct():
                    yield name, state
                case Min() | Max():
                    if not state:
                        yield name, None
                        continue
                    prefix = bytes([i]) + kb
                    found = (
                        self._values.first(prefix)
                        if isinstance(agg, Min)
                        else self._values.last(prefix)
                    )
                    assert found is not None, f"{self.name}: {name} has values but no entries"
                    yield name, pickle.loads(found[1][_COUNT:])

    def _new_group(self) -> _Group:
        states: list[Any] = [_initial(a) for a in self.aggregates.values()]
        return _Group(0, states, None)

    def _save(self, kb: bytes, group: _Group) -> None:
        self._groups.put(kb, pickle.dumps((group.rows, group.states, group.out), protocol=5))


def _initial(agg: Aggregate) -> Any:
    if isinstance(agg, Sum):
        return (0, 0)
    if isinstance(agg, Avg):
        return (0, 0, {})
    return 0


class Distinct(Operator):
    """Each distinct row once, as long as at least one copy is present."""

    def __init__(self, input: Operator, *, store: StateStore, name: str) -> None:
        super().__init__(input)
        self.name = name
        self._counts: Keyspace = store.keyspace(f"distinct:{name}")
        self._latest = Lsn(0)

    def process(self, port: int, batch: Sequence[Delta[Row]]) -> list[Delta[Row]]:
        out: list[Delta[Row]] = []
        for t, deltas in _by_time(batch):
            _check_time(self.name, self._latest, t)
            self._latest = t
            pending: dict[bytes, tuple[int, Row]] = {}
            for d in deltas:
                rk = encode_row_key(d.row)
                seen, _ = pending.get(rk, (0, d.row))
                pending[rk] = (seen + d.diff, d.row)
            for rk, (diff, row) in pending.items():
                if not diff:
                    continue
                current = self._counts.get(rk)
                before = (
                    0 if current is None else int.from_bytes(current[:_COUNT], "big", signed=True)
                )
                after = before + diff
                if after < 0:
                    raise AggregateError(f"{self.name}: {row!r} would have a negative count")
                if after == 0:
                    self._counts.delete(rk)
                    out.append(Delta(decode_row(current[_COUNT:]) if current else row, t, -1))
                    continue
                stored = current[_COUNT:] if current is not None else encode_row(row)
                self._counts.put(rk, after.to_bytes(_COUNT, "big", signed=True) + stored)
                if before == 0:
                    out.append(Delta(row, t, 1))
        return out
