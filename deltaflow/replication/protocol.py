"""Streaming replication framing inside CopyData: XLogData, keepalives, status updates.

See "Streaming Replication Protocol" in the PostgreSQL docs.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from deltaflow.replication._buffer import ByteReader
from deltaflow.replication.lsn import Lsn

PG_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)
_STATUS = struct.Struct(">cQQQqB")


def from_pg_timestamp(micros: int) -> datetime:
    """Server timestamps are microseconds since 2000-01-01 00:00 UTC."""
    return PG_EPOCH + timedelta(microseconds=micros)


def to_pg_timestamp(moment: datetime) -> int:
    return (moment - PG_EPOCH) // timedelta(microseconds=1)


@dataclass(frozen=True, slots=True)
class XLogData:
    """A chunk of decoded WAL: one pgoutput message, starting at `wal_start`."""

    wal_start: Lsn
    wal_end: Lsn
    send_time: datetime
    payload: bytes


@dataclass(frozen=True, slots=True)
class PrimaryKeepalive:
    wal_end: Lsn
    send_time: datetime
    reply_requested: bool


def parse_copy_data(data: bytes) -> XLogData | PrimaryKeepalive:
    r = ByteReader(data)
    tag = r.byte()
    if tag == b"w":
        start, end, sent = Lsn(r.u64()), Lsn(r.u64()), from_pg_timestamp(r.i64())
        return XLogData(start, end, sent, r.rest())
    if tag == b"k":
        end, sent = Lsn(r.u64()), from_pg_timestamp(r.i64())
        return PrimaryKeepalive(end, sent, r.u8() == 1)
    raise ValueError(f"unexpected replication CopyData message {tag!r}")


def standby_status_update(
    write: Lsn, flush: Lsn, apply: Lsn, now: datetime, *, reply_requested: bool = False
) -> bytes:
    """`flush` is what the server records as the slot's confirmed_flush_lsn."""
    return _STATUS.pack(b"r", write, flush, apply, to_pg_timestamp(now), int(reply_requested))
