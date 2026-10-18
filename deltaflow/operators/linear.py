"""Stateless (linear) operators: map and filter.

Both act on each delta independently and keep its time and diff, so they
commute with + and with negation. An UPDATE arrives as (old, -1), (new, +1).
Mapping both halves yields the retraction of exactly what the old row
produced earlier, *provided the function is deterministic*: retracting
f(old) only cancels the earlier +f(old) if f returns the same row both
times. A non-deterministic function (one that reads the clock, a random
number generator or mutable globals) breaks that. Sinks detect the symptom,
a row whose multiplicity goes negative, and raise rather than publish it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from deltaflow.core import Delta, Row
from deltaflow.operators.base import Operator

type Predicate = Callable[[Row], bool]
type MapFunction = Callable[[Row], Mapping[str, Any]]


class Filter(Operator):
    def __init__(self, input: Operator, predicate: Predicate) -> None:
        super().__init__(input)
        self.predicate = predicate

    def process(self, port: int, batch: Sequence[Delta[Row]]) -> list[Delta[Row]]:
        out = []
        for d in batch:
            keep = self.predicate(d.row)
            if not isinstance(keep, bool):
                # None in particular: Python has no SQL "unknown", and treating
                # it as False would hide a NULL-handling bug in the predicate.
                raise TypeError(
                    f"filter predicate {_name(self.predicate)} returned "
                    f"{type(keep).__name__} for {d.row!r}; it must return bool"
                )
            if keep:
                out.append(d)
        return out


class Map(Operator):
    def __init__(self, input: Operator, fn: MapFunction) -> None:
        super().__init__(input)
        self.fn = fn

    def process(self, port: int, batch: Sequence[Delta[Row]]) -> list[Delta[Row]]:
        out = []
        for d in batch:
            result = self.fn(d.row)
            if not isinstance(result, Mapping):
                raise TypeError(
                    f"map function {_name(self.fn)} returned {type(result).__name__} "
                    f"for {d.row!r}; it must return a mapping of column name to value"
                )
            out.append(Delta(result if isinstance(result, Row) else Row(result), d.time, d.diff))
        return out


def _name(fn: Callable[..., object]) -> str:
    return getattr(fn, "__qualname__", repr(fn))
