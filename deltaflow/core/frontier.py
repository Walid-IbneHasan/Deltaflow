"""Frontiers: how far a stream of deltas is known to be complete.

A frontier is a lower bound on the timestamps of every delta that may still
arrive. A time `t` is *complete* once `t < frontier`: no delta at `t` can
arrive any more, so the accumulated result at `t` is final and may be shown.

Timestamps are commit LSNs, which are totally ordered, so a frontier is a
single LSN. (In general an antichain of a partially ordered time domain; for
a total order the antichain has at most one element.) The empty antichain is
`Frontier.CLOSED`: nothing more will ever arrive, and every time is complete.
"""

from __future__ import annotations

from typing import ClassVar

from deltaflow.replication.lsn import Lsn


class FrontierRegressionError(RuntimeError):
    """A frontier moved backwards, which would un-complete published results."""


class FrontierViolationError(RuntimeError):
    """A delta arrived at a time its frontier had already declared complete."""


class Frontier:
    __slots__ = ("lower",)

    CLOSED: ClassVar[Frontier]

    #: Least timestamp that may still arrive; None when closed.
    lower: Lsn | None

    def __init__(self, lower: Lsn | None) -> None:
        self.lower = lower

    @classmethod
    def at(cls, lower: Lsn) -> Frontier:
        return cls(lower)

    @staticmethod
    def meet(*frontiers: Frontier) -> Frontier:
        """Greatest lower bound: a result built from several inputs is complete
        only where all of them are. The meet of nothing is CLOSED."""
        lows = [f.lower for f in frontiers if f.lower is not None]
        return Frontier(min(lows)) if lows else Frontier.CLOSED

    def is_complete(self, t: Lsn) -> bool:
        return self.lower is None or t < self.lower

    def admits(self, t: Lsn) -> bool:
        """May a delta at time `t` still arrive?"""
        return not self.is_complete(t)

    def __le__(self, other: Frontier) -> bool:
        if other.lower is None:
            return True
        return self.lower is not None and self.lower <= other.lower

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Frontier):
            return NotImplemented
        return self.lower == other.lower

    def __hash__(self) -> int:
        return hash(self.lower)

    def __repr__(self) -> str:
        return "Frontier.CLOSED" if self.lower is None else f"Frontier.at({self.lower!r})"

    def __str__(self) -> str:
        return "closed" if self.lower is None else str(self.lower)


Frontier.CLOSED = Frontier(None)


class MutableFrontier:
    """A frontier that may only move forward."""

    __slots__ = ("current",)

    def __init__(self, initial: Frontier) -> None:
        self.current = initial

    def advance(self, to: Frontier) -> None:
        if not self.current <= to:
            raise FrontierRegressionError(f"frontier would move back from {self.current} to {to}")
        self.current = to
