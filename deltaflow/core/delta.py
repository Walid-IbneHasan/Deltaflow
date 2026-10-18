"""Deltas `(row, time, diff)`, consolidation and accumulation."""

from __future__ import annotations

from collections.abc import Hashable, Iterable
from dataclasses import dataclass

from deltaflow.core.frontier import Frontier
from deltaflow.core.multiset import Multiset
from deltaflow.replication.lsn import Lsn


@dataclass(frozen=True, slots=True)
class Delta[T: Hashable]:
    """`diff` copies of `row` were added (diff > 0) or removed (diff < 0) at `time`.

    An UPDATE is two deltas at the same time: the old row with -1 and the new
    row with +1.
    """

    row: T
    time: Lsn
    diff: int


def consolidate[T: Hashable](deltas: Iterable[Delta[T]]) -> list[Delta[T]]:
    """Merge deltas with equal (row, time) by summing diffs, and drop zeros.

    The result is canonical: one delta per (row, time), none with diff 0,
    ordered by time (and by first appearance within a time). It accumulates
    to the same collection as the input at every frontier.
    """
    sums: dict[tuple[T, Lsn], int] = {}
    for d in deltas:
        key = (d.row, d.time)
        sums[key] = sums.get(key, 0) + d.diff
    out = [Delta(row, time, n) for (row, time), n in sums.items() if n]
    out.sort(key=lambda d: d.time)  # stable: keeps first-appearance order per time
    return out


def accumulate[T: Hashable](
    deltas: Iterable[Delta[T]], frontier: Frontier = Frontier.CLOSED
) -> Multiset[T]:
    """The collection the deltas describe as of `frontier`: the sum of every
    delta whose time is complete there. With the default CLOSED frontier,
    every delta counts."""
    m: Multiset[T] = Multiset()
    for d in deltas:
        if frontier.is_complete(d.time):
            m.add(d.row, d.diff)
    return m
