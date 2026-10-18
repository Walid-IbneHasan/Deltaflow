"""LSN formatting and the replication CopyData framing (XLogData, keepalive, status update)."""

from __future__ import annotations

import struct
from datetime import UTC, datetime

import pytest

from deltaflow.replication.lsn import Lsn
from deltaflow.replication.protocol import (
    PrimaryKeepalive,
    XLogData,
    from_pg_timestamp,
    parse_copy_data,
    standby_status_update,
    to_pg_timestamp,
)


def test_lsn_prints_like_postgres() -> None:
    assert str(Lsn(0)) == "0/0"
    assert str(Lsn(0x16_B3748D0)) == "1/6B3748D0"
    assert repr(Lsn(0x16_B3748D0)) == "Lsn('1/6B3748D0')"


def test_lsn_parse_round_trips() -> None:
    for text in ("0/0", "1/6B3748D0", "FFFFFFFF/FFFFFFFF", "A/1"):
        assert str(Lsn.parse(text)) == text


def test_lsn_rejects_out_of_range_and_garbage() -> None:
    with pytest.raises(ValueError, match="LSN"):
        Lsn(-1)
    with pytest.raises(ValueError, match="LSN"):
        Lsn(2**64)
    with pytest.raises(ValueError, match="LSN"):
        Lsn.parse("16B3748D0")


def test_lsn_orders_numerically() -> None:
    assert Lsn.parse("0/FFFFFFFF") < Lsn.parse("1/0")


def test_pg_timestamps_are_microseconds_since_2000() -> None:
    assert from_pg_timestamp(0) == datetime(2000, 1, 1, tzinfo=UTC)
    moment = datetime(2026, 10, 18, 12, 30, 1, 123456, tzinfo=UTC)
    assert from_pg_timestamp(to_pg_timestamp(moment)) == moment


def test_parse_xlogdata() -> None:
    payload = b"B\x00\x01"
    data = b"w" + struct.pack(">QQq", 0x100, 0x200, 0) + payload
    msg = parse_copy_data(data)
    assert msg == XLogData(Lsn(0x100), Lsn(0x200), from_pg_timestamp(0), payload)


@pytest.mark.parametrize("flag", [0, 1])
def test_parse_keepalive(flag: int) -> None:
    data = b"k" + struct.pack(">Qq", 0x300, 5) + bytes([flag])
    assert parse_copy_data(data) == PrimaryKeepalive(Lsn(0x300), from_pg_timestamp(5), bool(flag))


def test_unknown_copy_data_is_rejected() -> None:
    with pytest.raises(ValueError, match="CopyData"):
        parse_copy_data(b"z" + bytes(16))


def test_standby_status_update_layout() -> None:
    now = datetime(2000, 1, 1, 0, 0, 1, tzinfo=UTC)
    raw = standby_status_update(Lsn(3), Lsn(2), Lsn(1), now, reply_requested=True)
    assert raw == b"r" + struct.pack(">QQQqB", 3, 2, 1, 1_000_000, 1)
