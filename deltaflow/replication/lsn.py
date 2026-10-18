"""Log sequence numbers."""

from __future__ import annotations

_MAX = 2**64


class Lsn(int):
    """A WAL position: an unsigned 64-bit byte offset into the WAL.

    Formatted the way Postgres formats `pg_lsn`, as two hex halves `HI/LO`.
    """

    __slots__ = ()

    def __new__(cls, value: int) -> Lsn:
        if not 0 <= value < _MAX:
            raise ValueError(f"LSN out of range: {value}")
        return super().__new__(cls, value)

    @classmethod
    def parse(cls, text: str) -> Lsn:
        hi, sep, lo = text.partition("/")
        if not sep:
            raise ValueError(f"malformed LSN: {text!r}")
        return cls((int(hi, 16) << 32) | int(lo, 16))

    def __str__(self) -> str:
        return f"{self >> 32:X}/{self & 0xFFFFFFFF:X}"

    def __repr__(self) -> str:
        return f"Lsn('{self}')"
