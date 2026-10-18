"""Bounds-checked big-endian reader over a protocol message."""

from __future__ import annotations

import struct


class BufferUnderrun(ValueError):
    pass


_U8 = struct.Struct(">B")
_I16 = struct.Struct(">h")
_I32 = struct.Struct(">i")
_I64 = struct.Struct(">q")
_U64 = struct.Struct(">Q")


class ByteReader:
    __slots__ = ("_buf", "_pos")

    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self._pos = 0

    def _unpack(self, s: struct.Struct) -> int:
        if self._pos + s.size > len(self._buf):
            raise BufferUnderrun(
                f"need {s.size} bytes at offset {self._pos}, have {self.remaining}"
            )
        (value,) = s.unpack_from(self._buf, self._pos)
        self._pos += s.size
        return int(value)

    def u8(self) -> int:
        return self._unpack(_U8)

    def i16(self) -> int:
        return self._unpack(_I16)

    def i32(self) -> int:
        return self._unpack(_I32)

    def i64(self) -> int:
        return self._unpack(_I64)

    def u64(self) -> int:
        return self._unpack(_U64)

    def byte(self) -> bytes:
        return bytes((self.u8(),))

    def take(self, n: int) -> bytes:
        if n < 0 or self._pos + n > len(self._buf):
            raise BufferUnderrun(f"need {n} bytes at offset {self._pos}, have {self.remaining}")
        out = self._buf[self._pos : self._pos + n]
        self._pos += n
        return out

    def cstring(self) -> str:
        end = self._buf.find(b"\0", self._pos)
        if end < 0:
            raise BufferUnderrun(f"unterminated string at offset {self._pos}")
        out = self._buf[self._pos : end].decode()
        self._pos = end + 1
        return out

    def rest(self) -> bytes:
        return self.take(self.remaining)

    @property
    def remaining(self) -> int:
        return len(self._buf) - self._pos
