"""Values of Postgres types that have no exact Python counterpart: json,
jsonb and arrays.

psycopg's defaults are not exact enough for incremental views. It loads
jsonb numbers as floats (2.50 becomes 2.5), json with duplicate keys as a
dict that keeps one of them, and an array with a lower bound other than 1
as a list without it. It also loads them as dicts and lists, which are not
hashable, and every value in a row must be (core/row.py). So:

* `Json` is a json value's text, exactly as stored. Postgres has no
  equality for json; two Json are equal when their texts are.
* `Jsonb` is a jsonb value's canonical text (jsonb_out). Equality is
  jsonb's: numbers compare by value, so 1.0 = 1, as in Postgres.
* `PgArray` is a tuple of elements, nested for each further dimension.
  An array whose lower bound is not 1 is refused when decoded rather than
  silently re-based (`check_bounds`).

`get`, `get_text`, `get_path` and `get_path_text` are Postgres's `->`,
`->>`, `#>` and `#>>` on both json types.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from deltaflow.core.jsontext import JsonError, Node, Unescapable, jsonb_out, jsonb_pairs, parse


class Json:
    """A json value: its text, exactly as stored."""

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Json) and other.text == self.text

    def __hash__(self) -> int:
        return hash(("json", self.text))

    def __reduce__(self) -> tuple[type[Json], tuple[str]]:
        return (Json, (self.text,))

    def __repr__(self) -> str:
        return f"Json({self.text!r})"

    def __str__(self) -> str:
        return self.text


class Jsonb:
    """A jsonb value: its canonical text, with jsonb's equality."""

    __slots__ = ("_key", "text")

    def __init__(self, text: str) -> None:
        """`text` must be canonical (jsonb_out's), as Postgres sends it."""
        self.text = text
        self._key: tuple[Any, ...] | None = None

    @property
    def key(self) -> tuple[Any, ...]:
        """The value as nested tuples, numbers as Decimal: equal exactly
        when Postgres says the jsonb values are."""
        if self._key is None:
            self._key = _key(parse(self.text))
        return self._key

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Jsonb):
            return False
        return other.text == self.text or other.key == self.key

    def __hash__(self) -> int:
        return hash(self.key)

    def __reduce__(self) -> tuple[type[Jsonb], tuple[str]]:
        return (Jsonb, (self.text,))

    def __repr__(self) -> str:
        return f"Jsonb({self.text!r})"

    def __str__(self) -> str:
        return self.text


def _key(node: Node) -> tuple[Any, ...]:
    match node.kind:
        case "object":
            return ("o", tuple((k, _key(v)) for k, v in jsonb_pairs(node)))
        case "array":
            return ("a", tuple(_key(v) for v in node.value))
        case "string":
            return ("s", node.value)
        case "number":
            return ("n", node.value)
        case "boolean":
            return ("b", node.value)
        case _:
            return ("z",)


class PgArray(tuple[Any, ...]):
    """An array's elements (a PgArray per element for each further
    dimension); every dimension's lower bound is 1."""

    __slots__ = ()

    def __repr__(self) -> str:
        return f"PgArray({tuple(self)!r})"


class UnsupportedValueError(ValueError):
    """A value Deltaflow cannot hold exactly."""


def check_bounds(text: str) -> None:
    """An array's text form starts with its bounds when one is not 1
    ("[0:1]={1,2}"); its elements would be re-based silently, so refuse."""
    if text.startswith("["):
        raise UnsupportedValueError(
            f"array {text[:40]!r} has a lower bound other than 1, which Deltaflow does not "
            "support (its subscripts would differ from Postgres's)"
        )


def to_array(value: Any) -> Any:
    """psycopg's nested lists as PgArray, all the way down."""
    if isinstance(value, list):
        return PgArray(to_array(v) for v in value)
    return value


def to_param(value: Any) -> Any:
    """A value as psycopg dumps it: arrays as lists (a PgArray is a tuple,
    which psycopg would dump as a record)."""
    if isinstance(value, PgArray):
        return [to_param(v) for v in value]
    return value


# --- -> ->> #> #>> ------------------------------------------------------------


def get(doc: Json | Jsonb, key: str | int) -> Json | Jsonb | None:
    """`doc -> key`: an object's member (by text key) or an array's element
    (by integer index, negative from the end); otherwise NULL."""
    node = _member(_top(doc, key), key, json=isinstance(doc, Json))
    return None if node is None else _same(doc, node)


def get_text(doc: Json | Jsonb, key: str | int) -> str | None:
    """`doc ->> key`: as `->`, as text; a JSON null is SQL NULL."""
    node = _member(_top(doc, key), key, json=isinstance(doc, Json))
    return None if node is None else _as_text(doc, node)


def get_path(doc: Json | Jsonb, path: Sequence[str | None]) -> Json | Jsonb | None:
    """`doc #> path`: follow keys and indexes; NULL if any step is missing."""
    node = _walk(doc, path)
    return None if node is None else _same(doc, node)


def get_path_text(doc: Json | Jsonb, path: Sequence[str | None]) -> str | None:
    node = _walk(doc, path)
    return None if node is None else _as_text(doc, node)


def _tree(doc: Json | Jsonb) -> Node:
    """The parsed document. json's operators de-escape every string while
    they parse, so a string anywhere that cannot become text fails them
    all, not only one on the path taken."""
    node = parse(doc.text)
    if isinstance(doc, Json):
        _unescape_all(node)
    return node


def _unescape_all(node: Node) -> None:
    match node.kind:
        case "object":
            for key, value in node.value:
                _unescapable(key)
                _unescape_all(value)
        case "array":
            for item in node.value:
                _unescape_all(item)
        case "string":
            _unescapable(node.value)
        case _:
            pass


def _unescapable(s: str) -> None:
    if isinstance(s, Unescapable):
        raise JsonError(s.error)
    if "\x00" in s:
        raise JsonError("unsupported Unicode escape sequence")


def _top(doc: Json | Jsonb, step: str | int | None) -> Node:
    """The document's root, as `->` and `->>` see it. jsonb stores a scalar
    at the top as an array of one element ("raw scalar"), so an index
    reaches it there: '"x"'::jsonb ->> 0 is 'x'."""
    node = _tree(doc)
    scalar = node.kind not in ("object", "array")
    if isinstance(doc, Jsonb) and scalar and not isinstance(step, str):
        return Node("array", [node], node.start, node.end)
    return node


def _walk(doc: Json | Jsonb, path: Sequence[str | None]) -> Node | None:
    # No raw-scalar step here: '"x"'::jsonb #> '{0}' is NULL.
    node: Node | None = _tree(doc)
    for step in path:
        if node is None or step is None:
            return None
        if node.kind == "array":
            index = _index(step)
            if index is None:
                return None
            node = _member(node, index, json=isinstance(doc, Json))
        else:
            node = _member(node, step, json=isinstance(doc, Json))
    return node


def _index(step: str) -> int | None:
    """A path step as an array index: an integer, as int4 reads one."""
    text = step.strip()
    body = text[1:] if text[:1] in "+-" else text
    if not body.isdigit():
        return None
    return int(text)


def _member(node: Node, key: str | int, *, json: bool) -> Node | None:
    if isinstance(key, str):
        if node.kind != "object":
            return None
        pairs = node.value if json else jsonb_pairs(node)
        found = None
        for k, v in pairs:
            if isinstance(k, Unescapable):
                raise JsonError(k.error)  # keys are compared as text
            if k == key:
                found = v
                if not json:
                    break
        return found
    if node.kind != "array":
        return None
    items = node.value
    if key < 0:
        key += len(items)
    return items[key] if 0 <= key < len(items) else None


def _same(doc: Json | Jsonb, node: Node) -> Json | Jsonb:
    if isinstance(doc, Json):
        return Json(doc.text[node.start : node.end])
    return Jsonb(jsonb_out(node))


def _as_text(doc: Json | Jsonb, node: Node) -> str | None:
    if node.kind == "null":
        return None
    if node.kind == "string":
        if isinstance(node.value, Unescapable):
            raise JsonError(node.value.error)
        if "\x00" in node.value:
            raise JsonError("unsupported Unicode escape sequence")
        return str(node.value)
    if isinstance(doc, Json):
        return doc.text[node.start : node.end]
    return jsonb_out(node)
