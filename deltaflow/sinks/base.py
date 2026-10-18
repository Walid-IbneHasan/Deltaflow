from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from deltaflow.core import Delta, Frontier, Row


class Sink(ABC):
    """Receives a view's output deltas, and frontier advances that say which
    times are complete. Only complete times may be made visible."""

    @abstractmethod
    def accept(self, batch: Sequence[Delta[Row]]) -> None: ...

    @abstractmethod
    def advance(self, frontier: Frontier) -> None: ...
