"""JSON text as Postgres reads and writes it, exactly.

Two types, two jobs:

* `json` keeps its input text as is: whitespace, key order, duplicate keys.
  Its operators return slices of that text (`spans`).
* `jsonb` is a canonical value. Its text (`jsonb_out`) puts object keys in
  jsonb's own order (shorter first, then bytewise), keeps only the last of
  duplicate keys, writes numbers as numeric_out does (scale kept, never an
  exponent) and separates with ", " and ": ".

The scanner accepts what Postgres's JSON lexer accepts and rejects what it
rejects (leading zeros, control characters in strings, lone surrogates),
raising `JsonError` with Postgres's message where one matters. Numbers are
`Decimal`, never float: a jsonb number is a numeric.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

#: JSON whitespace, as Postgres's lexer defines it
_SPACE = " \t\n\r"
_LITERALS: tuple[tuple[str, str, bool | None], ...] = (
    ("true", "boolean", True),
    ("false", "boolean", False),
    ("null", "null", None),
)
_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
_ESCAPES = {'"': '"', "\\": "\\", "/": "/", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}


class JsonError(ValueError):
    """Text Postgres would reject as json or jsonb."""


class Unescapable(str):
    """A json string with a bad escape (a lone surrogate). json accepts it
    on input and keeps it; turning it into text fails, with `error`."""

    __slots__ = ("error",)
    error: str


@dataclass(frozen=True, slots=True)
class Node:
    """A parsed value: its kind, its value, and its span in the text.

    kind is "object" (value: list of (key, Node), in text order, with any
    duplicates), "array" (value: list of Node), "string" (str), "number"
    (Decimal), "boolean" (bool) or "null" (None)."""

    kind: str
    value: Any
    start: int
    end: int


def parse(text: str, *, jsonb: bool = False) -> Node:
    """One JSON value, with nothing but whitespace around it. json checks
    only syntax: a string with a bad escape is kept, as `Unescapable`.
    jsonb (`jsonb=True`) de-escapes everything on input, so it refuses."""
    p = _Parser(text, strict=jsonb)
    p.skip()
    node = p.value()
    p.skip()
    if p.i != len(text):
        raise JsonError(f'invalid input syntax for type json: unexpected "{text[p.i]}"')
    return node


class _Parser:
    def __init__(self, text: str, *, strict: bool) -> None:
        self.text = text
        self.i = 0
        self.strict = strict

    def skip(self) -> None:
        while self.i < len(self.text) and self.text[self.i] in _SPACE:
            self.i += 1

    def value(self) -> Node:
        t, i = self.text, self.i
        if i >= len(t):
            raise JsonError("invalid input syntax for type json: the input ends unexpectedly")
        c = t[i]
        if c == "{":
            return self.object()
        if c == "[":
            return self.array()
        if c == '"':
            start = i
            string = self.string()
            return Node("string", string, start, self.i)
        if c == "-" or c.isdigit():
            return self.number()
        for word, kind, value in _LITERALS:
            if t.startswith(word, i):
                end = i + len(word)
                if end < len(t) and (t[end].isalnum() or t[end] == "_"):
                    break
                self.i = end
                return Node(kind, value, i, end)
        raise JsonError(f'invalid input syntax for type json: unexpected "{c}"')

    def object(self) -> Node:
        start = self.i
        self.i += 1
        pairs: list[tuple[str, Node]] = []
        self.skip()
        if self.peek() == "}":
            self.i += 1
            return Node("object", pairs, start, self.i)
        while True:
            self.skip()
            if self.peek() != '"':
                raise JsonError("invalid input syntax for type json: expected a string key")
            key = self.string()
            self.skip()
            if self.peek() != ":":
                raise JsonError('invalid input syntax for type json: expected ":"')
            self.i += 1
            self.skip()
            pairs.append((key, self.value()))
            self.skip()
            c = self.peek()
            self.i += 1
            if c == "}":
                return Node("object", pairs, start, self.i)
            if c != ",":
                raise JsonError('invalid input syntax for type json: expected "," or "}"')

    def array(self) -> Node:
        start = self.i
        self.i += 1
        items: list[Node] = []
        self.skip()
        if self.peek() == "]":
            self.i += 1
            return Node("array", items, start, self.i)
        while True:
            self.skip()
            items.append(self.value())
            self.skip()
            c = self.peek()
            self.i += 1
            if c == "]":
                return Node("array", items, start, self.i)
            if c != ",":
                raise JsonError('invalid input syntax for type json: expected "," or "]"')

    def peek(self) -> str:
        if self.i >= len(self.text):
            raise JsonError("invalid input syntax for type json: the input ends unexpectedly")
        return self.text[self.i]

    def string(self) -> str:
        t = self.text
        self.i += 1
        out: list[str] = []
        bad: str | None = None  # the first bad escape (json only)
        while True:
            if self.i >= len(t):
                raise JsonError("invalid input syntax for type json: unterminated string")
            c = t[self.i]
            if c == '"':
                self.i += 1
                if bad is None:
                    return "".join(out)
                kept = Unescapable("".join(out))
                kept.error = bad
                return kept
            if ord(c) < 0x20:
                raise JsonError(f"Character with value 0x{ord(c):02x} must be escaped.")
            if c != "\\":
                out.append(c)
                self.i += 1
                continue
            self.i += 1
            e = t[self.i : self.i + 1]
            if e in _ESCAPES:
                out.append(_ESCAPES[e])
                self.i += 1
            elif e == "u":
                char, error = self.unicode()
                out.append(char)
                bad = bad or error
            else:
                raise JsonError(f'Escape sequence "\\{e}" is invalid.')

    def unicode(self) -> tuple[str, str | None]:
        """The character of a \\u escape, and what is wrong with it if json
        keeps it anyway."""
        code = self.hex4()
        if 0xD800 <= code <= 0xDBFF:
            if self.text[self.i : self.i + 2] != "\\u":
                return self.surrogate()
            back = self.i
            self.i += 1
            low = self.hex4()
            if not 0xDC00 <= low <= 0xDFFF:
                self.i = back  # the next escape is read on its own
                return self.surrogate()
            return chr(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00)), None
        if 0xDC00 <= code <= 0xDFFF:
            return self.surrogate()
        if code == 0:
            # Accepted by json's input; text cannot hold it (unescaping fails).
            return "\x00", None
        return chr(code), None

    def surrogate(self) -> tuple[str, str]:
        """A lone surrogate: jsonb refuses it now, json when it is unescaped."""
        error = "Unicode low surrogate must follow a high surrogate."
        if self.strict:
            raise JsonError(error)
        return "\N{REPLACEMENT CHARACTER}", error

    def hex4(self) -> int:
        digits = self.text[self.i + 1 : self.i + 5]
        if len(digits) != 4 or any(d not in "0123456789abcdefABCDEF" for d in digits):
            raise JsonError('"\\u" must be followed by four hexadecimal digits.')
        self.i += 5
        return int(digits, 16)

    def number(self) -> Node:
        t, start = self.text, self.i
        found = _NUMBER.match(t, start)
        i = start if found is None else found.end()
        # A number must end where a token can: "01", "1.", "1e", "1x" do not.
        if found is None or (i < len(t) and (t[i].isalnum() or t[i] in "._")):
            raise JsonError("invalid input syntax for type json")
        self.i = i
        return Node("number", Decimal(t[start:i]), start, i)


# --- jsonb ----------------------------------------------------------------------


def jsonb_out(node: Node) -> str:
    """jsonb's canonical text of a parsed value."""
    out: list[str] = []
    _jsonb(node, out)
    return "".join(out)


def _jsonb(node: Node, out: list[str]) -> None:
    match node.kind:
        case "object":
            out.append("{")
            for n, (key, value) in enumerate(jsonb_pairs(node)):
                if n:
                    out.append(", ")
                out.append(escape(key))
                out.append(": ")
                _jsonb(value, out)
            out.append("}")
        case "array":
            out.append("[")
            for n, item in enumerate(node.value):
                if n:
                    out.append(", ")
                _jsonb(item, out)
            out.append("]")
        case "string":
            out.append(escape(node.value))
        case "number":
            out.append(numeric_out(node.value))
        case "boolean":
            out.append("true" if node.value else "false")
        case _:
            out.append("null")


def jsonb_pairs(node: Node) -> list[tuple[str, Node]]:
    """An object's pairs as jsonb keeps them: the last of each duplicate
    key, in jsonb's key order (shorter first, then bytewise)."""
    last: dict[str, Node] = {}
    for key, value in node.value:
        last[key] = value
    return sorted(last.items(), key=lambda kv: (len(kv[0].encode()), kv[0].encode()))


def escape(s: str) -> str:
    """A string literal as Postgres writes one (escape_json)."""
    out = ['"']
    for c in s:
        match c:
            case '"':
                out.append('\\"')
            case "\\":
                out.append("\\\\")
            case "\b":
                out.append("\\b")
            case "\f":
                out.append("\\f")
            case "\n":
                out.append("\\n")
            case "\r":
                out.append("\\r")
            case "\t":
                out.append("\\t")
            case _ if ord(c) < 0x20:
                out.append(f"\\u{ord(c):04x}")
            case _:
                out.append(c)
    out.append('"')
    return "".join(out)


def numeric_out(d: Decimal) -> str:
    """A numeric's text: every digit of its scale, never an exponent; zero
    has no sign."""
    text = format(d, "f")
    if d.is_zero() and text.startswith("-"):
        text = text[1:]
    return text


def jsonb_in(text: str) -> str:
    """Text cast to jsonb: its canonical text. jsonb cannot hold \\u0000."""
    node = parse(text, jsonb=True)
    if _has_nul(node):
        raise JsonError("unsupported Unicode escape sequence")
    return jsonb_out(node)


def _has_nul(node: Node) -> bool:
    match node.kind:
        case "object":
            return any("\x00" in k or _has_nul(v) for k, v in node.value)
        case "array":
            return any(_has_nul(v) for v in node.value)
        case "string":
            return "\x00" in node.value
        case _:
            return False
