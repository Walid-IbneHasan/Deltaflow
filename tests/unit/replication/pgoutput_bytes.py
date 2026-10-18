"""Hand-built pgoutput (protocol version 1) messages, written from the layout in the
PostgreSQL docs ("Logical Replication Message Formats"), not from our decoder."""

from __future__ import annotations

import struct
from collections.abc import Sequence


def cstr(s: str) -> bytes:
    return s.encode() + b"\0"


def begin(final_lsn: int, ts: int, xid: int) -> bytes:
    return b"B" + struct.pack(">Qqi", final_lsn, ts, xid)


def commit(commit_lsn: int, end_lsn: int, ts: int) -> bytes:
    return b"C" + struct.pack(">BQQq", 0, commit_lsn, end_lsn, ts)


def origin(lsn: int, name: str) -> bytes:
    return b"O" + struct.pack(">Q", lsn) + cstr(name)


def relation(
    oid: int, namespace: str, name: str, identity: str, columns: Sequence[tuple[int, str, int, int]]
) -> bytes:
    out = b"R" + struct.pack(">i", oid) + cstr(namespace) + cstr(name) + identity.encode()
    out += struct.pack(">h", len(columns))
    for flags, col, type_oid, typmod in columns:
        out += struct.pack(">B", flags) + cstr(col) + struct.pack(">ii", type_oid, typmod)
    return out


def type_(oid: int, namespace: str, name: str) -> bytes:
    return b"Y" + struct.pack(">i", oid) + cstr(namespace) + cstr(name)


# A tuple column is None (SQL NULL), "u" (unchanged TOAST), str (text) or bytes (binary).
Col = str | bytes | None


def tuple_data(cols: Sequence[Col]) -> bytes:
    out = struct.pack(">h", len(cols))
    for c in cols:
        if c is None:
            out += b"n"
        elif c == "u":
            out += b"u"
        elif isinstance(c, str):
            raw = c.encode()
            out += b"t" + struct.pack(">i", len(raw)) + raw
        else:
            out += b"b" + struct.pack(">i", len(c)) + c
    return out


def insert(rel: int, new: Sequence[Col]) -> bytes:
    return b"I" + struct.pack(">i", rel) + b"N" + tuple_data(new)


def update(
    rel: int, new: Sequence[Col], old: Sequence[Col] | None = None, old_kind: bytes = b"K"
) -> bytes:
    out = b"U" + struct.pack(">i", rel)
    if old is not None:
        out += old_kind + tuple_data(old)
    return out + b"N" + tuple_data(new)


def delete(rel: int, old: Sequence[Col], old_kind: bytes = b"K") -> bytes:
    return b"D" + struct.pack(">i", rel) + old_kind + tuple_data(old)


def truncate(rels: Sequence[int], options: int) -> bytes:
    return (
        b"T" + struct.pack(">iB", len(rels), options) + b"".join(struct.pack(">i", r) for r in rels)
    )


def message(transactional: bool, lsn: int, prefix: str, content: bytes) -> bytes:
    return (
        b"M"
        + struct.pack(">BQ", int(transactional), lsn)
        + cstr(prefix)
        + struct.pack(">i", len(content))
        + content
    )
