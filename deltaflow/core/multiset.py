"""Multisets with signed multiplicities (a free abelian group over the rows)."""

from __future__ import annotations

from collections.abc import Hashable, Iterable, Iterator


class Multiset[T: Hashable]:
    """Element -> non-zero integer multiplicity.

    Multiplicities may be negative: a batch of deltas accumulates to a
    multiset that can retract rows. A *collection* (a table, a view result)
    is a multiset whose multiplicities are all positive. `negatives()` is how
    sinks check that invariant.
    """

    __slots__ = ("_counts",)

    def __init__(self) -> None:
        self._counts: dict[T, int] = {}

    @classmethod
    def of(cls, elements: Iterable[T]) -> Multiset[T]:
        """Each element once per occurrence."""
        return cls.from_pairs((e, 1) for e in elements)

    @classmethod
    def from_pairs(cls, pairs: Iterable[tuple[T, int]]) -> Multiset[T]:
        m: Multiset[T] = cls()
        for element, n in pairs:
            m.add(element, n)
        return m

    def add(self, element: T, n: int = 1) -> None:
        total = self._counts.get(element, 0) + n
        if total:
            self._counts[element] = total
        else:
            self._counts.pop(element, None)

    def count(self, element: T) -> int:
        return self._counts.get(element, 0)

    def items(self) -> Iterator[tuple[T, int]]:
        return iter(self._counts.items())

    def size(self) -> int:
        """Sum of all multiplicities (for a collection: the number of rows)."""
        return sum(self._counts.values())

    def negatives(self) -> dict[T, int]:
        return {e: n for e, n in self._counts.items() if n < 0}

    def copy(self) -> Multiset[T]:
        m: Multiset[T] = Multiset()
        m._counts = dict(self._counts)
        return m

    def __len__(self) -> int:
        """Number of distinct elements."""
        return len(self._counts)

    def __bool__(self) -> bool:
        return bool(self._counts)

    def __add__(self, other: Multiset[T]) -> Multiset[T]:
        m = self.copy()
        for e, n in other.items():
            m.add(e, n)
        return m

    def __neg__(self) -> Multiset[T]:
        m: Multiset[T] = Multiset()
        m._counts = {e: -n for e, n in self._counts.items()}
        return m

    def __sub__(self, other: Multiset[T]) -> Multiset[T]:
        return self + (-other)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Multiset):
            return NotImplemented
        return self._counts == other._counts

    __hash__ = None  # type: ignore[assignment]  # mutable

    def __repr__(self) -> str:
        return f"Multiset({self._counts!r})"
