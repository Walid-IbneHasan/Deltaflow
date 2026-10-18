"""Immutable, hashable rows."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any


class Row(Mapping[str, Any]):
    """Column name -> value; immutable and hashable, so it can be a multiset element.

    Equality and hashing ignore column order, as a `dict` does. Every value
    must be hashable. That is checked here, when the row is built, so an
    unsupported column type (an array arrives as `list`, jsonb as `dict`)
    fails loudly at the source instead of somewhere inside an operator.

    Rows are immutable for a second reason too: the source arrangement hands
    its stored rows to user functions. A function that mutated its input
    would silently corrupt the state that later retractions are computed
    from.
    """

    __slots__ = ("_hash", "_values")

    _values: dict[str, Any]
    _hash: int

    def __init__(self, values: Mapping[str, Any] | Iterable[tuple[str, Any]] = (), /) -> None:
        self._values = dict(values)
        try:
            self._hash = hash(frozenset(self._values.items()))
        except TypeError:
            bad = next(k for k, v in self._values.items() if not _hashable(v))
            raise TypeError(
                f"column {bad!r} has unhashable value of type "
                f"{type(self._values[bad]).__name__}; rows must hold hashable values"
            ) from None

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Row):
            return self._hash == other._hash and self._values == other._values
        if isinstance(other, Mapping):
            return self._values == dict(other)
        return NotImplemented

    def __repr__(self) -> str:
        return f"Row({self._values!r})"


def _hashable(value: object) -> bool:
    try:
        hash(value)
    except TypeError:
        return False
    return True
