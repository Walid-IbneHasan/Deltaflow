"""Immutable, hashable rows."""

from __future__ import annotations

from collections.abc import ItemsView, Iterable, Iterator, KeysView, Mapping, ValuesView
from typing import Any

#: (a row's column names, an unqualified name) -> the qualified column it
#: names, or None when ambiguous or absent: rows of one shape (one join's
#: output) resolve a name once, not by scanning their columns each time.
_RESOLVED: dict[tuple[tuple[str, ...], str], str | None] = {}
_RESOLVED_MAX = 4096


class Row(Mapping[str, Any]):
    """Column name -> value; immutable and hashable, so it can be a multiset element.

    Equality and hashing ignore column order, as a `dict` does. Every value
    must be hashable. That is checked here, when the row is built, so an
    unsupported column type (an array arrives as `list`, jsonb as `dict`)
    fails loudly at the source instead of somewhere inside an operator.

    Columns of a joined row are qualified (`orders.status`); `row["status"]`
    finds it as long as exactly one column has that unqualified name.

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
        self._hash_values()

    @classmethod
    def of_dict(cls, values: dict[str, Any]) -> Row:
        """A row that takes `values` over, without copying: the caller must
        not touch the dict again."""
        row = cls.__new__(cls)
        row._values = values
        row._hash_values()
        return row

    def _hash_values(self) -> None:
        try:
            self._hash = hash(frozenset(self._values.items()))
        except TypeError:
            bad = next(k for k, v in self._values.items() if not _hashable(v))
            raise TypeError(
                f"column {bad!r} has unhashable value of type "
                f"{type(self._values[bad]).__name__}; rows must hold hashable values"
            ) from None

    def __getitem__(self, key: str) -> Any:
        try:
            return self._values[key]
        except KeyError:
            if "." in key:
                raise
        # Joined rows have qualified names ("orders.status"). An unqualified
        # name resolves to the one column it qualifies, if exactly one does.
        shape = (tuple(self._values), key)
        qualified = _RESOLVED.get(shape, "")
        if qualified == "":
            matches = [k for k in self._values if k.endswith("." + key)]
            if len(matches) > 1:
                raise KeyError(f"{key!r} is ambiguous: {', '.join(matches)}")
            qualified = matches[0] if matches else None
            if len(_RESOLVED) >= _RESOLVED_MAX:
                _RESOLVED.clear()
            _RESOLVED[shape] = qualified
        if qualified is None:
            raise KeyError(key)
        return self._values[qualified]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    # The dict's own views: Mapping's would look every key up again.
    def items(self) -> ItemsView[str, Any]:
        return self._values.items()

    def keys(self) -> KeysView[str]:
        return self._values.keys()

    def values(self) -> ValuesView[Any]:
        return self._values.values()

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
