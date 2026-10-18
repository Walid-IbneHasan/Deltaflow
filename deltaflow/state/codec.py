"""Byte encodings for the state store.

Two kinds of encoding with different jobs:

* **Keys** (`encode_key`, `encode_row_key`) are *canonical*: two values
  encode to the same bytes exactly when they are equal under Python `==`.
  That is the equality the multisets use, so it is what a retraction has to
  match. Naive encodings break this in ways the data really produces:
  `Decimal("1.0")` vs `Decimal("1.00")`; the same instant in two time zones,
  since the snapshot reads `timestamptz` in the session zone and the stream
  in UTC; `1 == 1.0 == Decimal(1) == True`. Keys are never decoded. Every
  element is self-delimiting, so a tuple of fixed arity is prefix-free:
  the encoding of key K is never a prefix of another key's, which makes
  prefix scans exact.

* **Values** (`encode_row`, `decode_row`) round-trip exactly, including
  `Decimal` scale, `tzinfo` and column order. They use pickle (protocol
  5): the store is private to this process, written and read only by
  Deltaflow, so pickle's trust problem (loading attacker-controlled bytes)
  does not arise.
"""

from __future__ import annotations

import pickle
import uuid
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from deltaflow.core import Row

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_MICRO = timedelta(microseconds=1)


def _varint(n: int) -> bytes:
    """LEB128 of a non-negative int."""
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _signed(n: int) -> bytes:
    return _varint(n * 2 if n >= 0 else -n * 2 - 1)  # zigzag


def _blob(tag: bytes, data: bytes) -> bytes:
    return tag + _varint(len(data)) + data


def _number(sign: int, digits: str, exp: int) -> bytes:
    """Canonical (sign, significant digits, exponent) of an exact number."""
    stripped = digits.rstrip("0")
    if not stripped:  # zero; -0 == 0
        return b"z"
    exp += len(digits) - len(stripped)
    return _blob(b"N", f"{'-' if sign else '+'}{stripped}e{exp}".encode())


def _encode(value: Any) -> bytes:  # noqa: PLR0911, PLR0912 - ordered type dispatch
    # bool before int (bool is an int, and True == 1); datetime before date.
    if value is None:
        return b"0"
    if isinstance(value, int):
        return _number(value < 0, str(abs(value)), 0)
    if isinstance(value, str):
        return _blob(b"S", value.encode("utf-8", "surrogatepass"))
    if isinstance(value, float | Decimal):
        d = Decimal(value)  # exact, also for floats
        if d.is_nan():  # NaN is not equal to itself, so it has no canonical key
            raise TypeError(f"NaN ({value!r}) cannot be used as a key: it is not equal to itself")
        if d.is_infinite():
            return b"i" if d < 0 else b"I"
        sign, digits, exp = d.as_tuple()
        assert isinstance(exp, int)
        return _number(sign, "".join(map(str, digits)), exp)
    if isinstance(value, datetime):
        if value.utcoffset() is not None:
            return b"Z" + _signed((value - _EPOCH_UTC) // _MICRO)
        return b"L" + _signed((value - _EPOCH_NAIVE) // _MICRO)
    if isinstance(value, date):
        return b"A" + _signed(value.toordinal())
    if isinstance(value, time) and value.utcoffset() is None:
        return b"H" + _varint(
            ((value.hour * 60 + value.minute) * 60 + value.second) * 10**6 + value.microsecond
        )
    if isinstance(value, timedelta):
        return b"T" + _signed(value // _MICRO)
    if isinstance(value, bytes | bytearray | memoryview):
        return _blob(b"B", bytes(value))
    if isinstance(value, uuid.UUID):
        return b"U" + value.bytes
    raise TypeError(
        f"a value of type {type(value).__name__} ({value!r:.60}) cannot be used as a key"
    )


def encode_key(values: Iterable[Any]) -> bytes:
    return b"".join(_encode(v) for v in values)


def encode_row_key(row: Row) -> bytes:
    """Canonical bytes of a whole row: column names sorted, since row
    equality ignores column order."""
    return b"".join(_encode(name) + _encode(row[name]) for name in sorted(row))


def encode_row(row: Row) -> bytes:
    return pickle.dumps(dict(row), protocol=5)


def decode_row(data: bytes) -> Row:
    return Row(pickle.loads(data))
