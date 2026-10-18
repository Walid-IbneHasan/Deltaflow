"""Operator graph nodes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from deltaflow.core import Delta, Frontier, FrontierViolationError, MutableFrontier, Row
from deltaflow.replication.lsn import Lsn

_LSN_ZERO = Lsn(0)


class Operator(ABC):
    """A node of the dataflow graph.

    It consumes batches of deltas from its inputs and returns the deltas it
    produces. Its frontier is derived: an operator cannot be further along
    than the least advanced of its inputs. Operators are synchronous. They do
    CPU work on data already in memory, and the runtime is what awaits I/O.
    """

    def __init__(self, *inputs: Operator) -> None:
        self.inputs: tuple[Operator, ...] = inputs

    @property
    def frontier(self) -> Frontier:
        return Frontier.meet(*(i.frontier for i in self.inputs))

    @abstractmethod
    def process(self, port: int, batch: Sequence[Delta[Row]]) -> list[Delta[Row]]:
        """Deltas produced in response to `batch` arriving on input `port`."""


class Source(Operator):
    """Entry point for one source table's deltas; owns its frontier."""

    def __init__(self, table: str) -> None:
        super().__init__()
        self.table = table
        self._frontier = MutableFrontier(Frontier.at(_LSN_ZERO))

    @property
    def frontier(self) -> Frontier:
        return self._frontier.current

    def advance(self, to: Frontier) -> None:
        self._frontier.advance(to)

    def process(self, port: int, batch: Sequence[Delta[Row]]) -> list[Delta[Row]]:
        for d in batch:
            if not self.frontier.admits(d.time):
                raise FrontierViolationError(
                    f"{self.table}: delta at {d.time} arrived after the frontier "
                    f"passed it ({self.frontier})"
                )
        return list(batch)

    def __repr__(self) -> str:
        return f"Source({self.table!r})"
