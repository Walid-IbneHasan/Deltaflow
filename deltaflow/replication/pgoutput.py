"""Decoder for the `pgoutput` logical replication message format, protocol version 1.

Written against "Logical Replication Message Formats" in the PostgreSQL docs.
We request protocol version 1 without streaming or two-phase options, so the
streaming (S/E/c/A) and two-phase (b/P/K/r/p) messages, and the xid fields
that only exist inside streamed transactions, never appear. If they do, that
is a protocol violation and decoding fails rather than guessing.

Column values are left as raw bytes here. Typing them is the job of
`deltaflow.replication.changes`, which knows the relation's column types.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Final, Literal

from deltaflow.replication._buffer import BufferUnderrun, ByteReader
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.protocol import from_pg_timestamp


class ProtocolError(ValueError):
    """The server sent something that does not follow the protocol we negotiated."""


class Toast(Enum):
    UNCHANGED = "u"

    def __repr__(self) -> str:
        return "UNCHANGED_TOAST"


#: Placeholder for an out-of-line (TOASTed) value the UPDATE did not modify.
#: The server does not resend it; the previous value still holds.
UNCHANGED_TOAST: Final = Toast.UNCHANGED


@dataclass(frozen=True, slots=True)
class Binary:
    """A column value in binary format (only sent if `binary` is requested)."""

    data: bytes


#: One column of a TupleData: NULL, unchanged TOAST, text-format bytes, or binary.
type RawValue = bytes | Binary | Toast | None
type OldKind = Literal["key", "full"]


@dataclass(frozen=True, slots=True)
class Begin:
    final_lsn: Lsn
    commit_time: datetime
    xid: int


@dataclass(frozen=True, slots=True)
class Commit:
    flags: int
    commit_lsn: Lsn
    end_lsn: Lsn
    commit_time: datetime


@dataclass(frozen=True, slots=True)
class Origin:
    commit_lsn: Lsn
    name: str


@dataclass(frozen=True, slots=True)
class Column:
    is_key: bool
    name: str
    type_oid: int
    type_modifier: int


@dataclass(frozen=True, slots=True)
class Relation:
    oid: int
    namespace: str
    name: str
    #: d = default (primary key), n = nothing, f = full, i = index
    replica_identity: str
    columns: tuple[Column, ...]

    @property
    def qualified_name(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass(frozen=True, slots=True)
class TypeInfo:
    oid: int
    namespace: str
    name: str


@dataclass(frozen=True, slots=True)
class Insert:
    relation_oid: int
    new: tuple[RawValue, ...]


@dataclass(frozen=True, slots=True)
class Update:
    relation_oid: int
    #: "key" when the replica identity columns changed (or identity is an
    #: index), "full" under REPLICA IDENTITY FULL, None when not sent.
    old_kind: OldKind | None
    old: tuple[RawValue, ...] | None
    new: tuple[RawValue, ...]


@dataclass(frozen=True, slots=True)
class Delete:
    relation_oid: int
    old_kind: OldKind
    old: tuple[RawValue, ...]


@dataclass(frozen=True, slots=True)
class Truncate:
    relation_oids: tuple[int, ...]
    cascade: bool
    restart_identity: bool


@dataclass(frozen=True, slots=True)
class Message:
    """A `pg_logical_emit_message()` record (requires the `messages` option)."""

    transactional: bool
    lsn: Lsn
    prefix: str
    content: bytes


type PgOutputMessage = (
    Begin | Commit | Origin | Relation | TypeInfo | Insert | Update | Delete | Truncate | Message
)

_OLD_KINDS: dict[bytes, OldKind] = {b"K": "key", b"O": "full"}


def decode_message(payload: bytes) -> PgOutputMessage:
    r = ByteReader(payload)
    try:
        tag = r.byte()
        msg = _decode(tag, r)
    except BufferUnderrun as exc:
        raise ProtocolError(f"truncated pgoutput message: {exc}") from exc
    if r.remaining:
        raise ProtocolError(f"{r.remaining} trailing bytes after {type(msg).__name__} message")
    return msg


def _decode(tag: bytes, r: ByteReader) -> PgOutputMessage:  # noqa: PLR0911 - one arm per message
    match tag:
        case b"B":
            return Begin(Lsn(r.u64()), from_pg_timestamp(r.i64()), r.i32())
        case b"C":
            return Commit(r.u8(), Lsn(r.u64()), Lsn(r.u64()), from_pg_timestamp(r.i64()))
        case b"O":
            return Origin(Lsn(r.u64()), r.cstring())
        case b"R":
            return _relation(r)
        case b"Y":
            return TypeInfo(r.i32(), r.cstring(), r.cstring())
        case b"I":
            oid = r.i32()
            _expect(r, b"N", "INSERT")
            return Insert(oid, _tuple(r))
        case b"U":
            return _update(r)
        case b"D":
            oid = r.i32()
            kind = r.byte()
            if kind not in _OLD_KINDS:
                raise ProtocolError(f"DELETE without old tuple marker, got {kind!r}")
            return Delete(oid, _OLD_KINDS[kind], _tuple(r))
        case b"T":
            n = r.i32()
            options = r.u8()
            oids = tuple(r.i32() for _ in range(n))
            return Truncate(oids, bool(options & 1), bool(options & 2))
        case b"M":
            flags = r.u8()
            lsn = Lsn(r.u64())
            prefix = r.cstring()
            content = r.take(r.i32())
            return Message(bool(flags & 1), lsn, prefix, content)
        case _:
            raise ProtocolError(f"unexpected pgoutput message type {tag!r}")


def _relation(r: ByteReader) -> Relation:
    oid = r.i32()
    # An empty namespace means pg_catalog.
    namespace = r.cstring() or "pg_catalog"
    name = r.cstring()
    identity = r.byte().decode()
    columns = tuple(Column(bool(r.u8() & 1), r.cstring(), r.i32(), r.i32()) for _ in range(r.i16()))
    return Relation(oid, namespace, name, identity, columns)


def _update(r: ByteReader) -> Update:
    oid = r.i32()
    marker = r.byte()
    old_kind: OldKind | None = None
    old: tuple[RawValue, ...] | None = None
    if marker in _OLD_KINDS:
        old_kind = _OLD_KINDS[marker]
        old = _tuple(r)
        marker = r.byte()
    if marker != b"N":
        raise ProtocolError(f"UPDATE expected new tuple marker 'N', got {marker!r}")
    return Update(oid, old_kind, old, _tuple(r))


def _expect(r: ByteReader, marker: bytes, what: str) -> None:
    got = r.byte()
    if got != marker:
        raise ProtocolError(f"{what} expected {marker!r}, got {got!r}")


def _tuple(r: ByteReader) -> tuple[RawValue, ...]:
    values: list[RawValue] = []
    for _ in range(r.i16()):
        kind = r.byte()
        if kind == b"n":
            values.append(None)
        elif kind == b"u":
            values.append(UNCHANGED_TOAST)
        elif kind == b"t":
            values.append(r.take(r.i32()))
        elif kind == b"b":
            values.append(Binary(r.take(r.i32())))
        else:
            raise ProtocolError(f"unknown tuple column kind {kind!r}")
    return tuple(values)
