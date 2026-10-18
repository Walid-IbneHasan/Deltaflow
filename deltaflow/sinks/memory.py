"""In-memory sink: the view's result as a multiset, as of its frontier."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from deltaflow.core import (
    Delta,
    Frontier,
    FrontierViolationError,
    Multiset,
    MutableFrontier,
    Row,
    consolidate,
)
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks.base import Sink

type OnChange = Callable[[Frontier, list[Delta[Row]]], None]


class NegativeMultiplicityError(RuntimeError):
    """A view retracted a row it never contained.

    Either a source change was lost or duplicated, or a map/filter function
    is not deterministic. Publishing the result would be publishing a wrong
    answer.
    """


class MemorySink(Sink):
    """Holds deltas until the frontier passes their time, then folds them
    into `result`. `result` is therefore always the exact view contents as
    of `frontier`: never a mix of complete and partial transactions."""

    def __init__(self, on_change: OnChange | None = None) -> None:
        self.result: Multiset[Row] = Multiset()
        self._frontier = MutableFrontier(Frontier.at(Lsn(0)))
        self._pending: list[Delta[Row]] = []
        self._on_change = on_change

    @property
    def frontier(self) -> Frontier:
        return self._frontier.current

    def rows(self) -> list[Row]:
        """The result with each row repeated by its multiplicity."""
        return [r for r, n in self.result.items() for _ in range(n)]

    def accept(self, batch: Sequence[Delta[Row]]) -> None:
        for d in batch:
            if not self.frontier.admits(d.time):
                raise FrontierViolationError(
                    f"sink received a delta at {d.time}, behind its frontier {self.frontier}"
                )
        self._pending.extend(batch)

    def advance(self, frontier: Frontier) -> None:
        self._frontier.advance(frontier)
        ready = [d for d in self._pending if frontier.is_complete(d.time)]
        if not ready:
            return
        self._pending = [d for d in self._pending if not frontier.is_complete(d.time)]
        batch = consolidate(ready)
        for d in batch:
            self.result.add(d.row, d.diff)
            if (n := self.result.count(d.row)) < 0:
                raise NegativeMultiplicityError(
                    f"{d.row!r} has multiplicity {n} at {frontier}: a retraction of a row the "
                    "view never contained (lost or duplicated change, or a "
                    "non-deterministic map/filter function)"
                )
        if batch and self._on_change is not None:
            self._on_change(frontier, batch)
