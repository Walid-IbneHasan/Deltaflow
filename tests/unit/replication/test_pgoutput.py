"""Byte-level decoding of every pgoutput v1 message we request."""

from __future__ import annotations

import pytest

from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import (
    UNCHANGED_TOAST,
    Begin,
    Binary,
    Column,
    Commit,
    Delete,
    Insert,
    Message,
    OldKind,
    Origin,
    ProtocolError,
    Relation,
    Truncate,
    TypeInfo,
    Update,
    decode_message,
)
from deltaflow.replication.protocol import from_pg_timestamp

from . import pgoutput_bytes as pb


def test_begin() -> None:
    assert decode_message(pb.begin(0xA0, 42, 777)) == Begin(
        final_lsn=Lsn(0xA0), commit_time=from_pg_timestamp(42), xid=777
    )


def test_commit() -> None:
    assert decode_message(pb.commit(0xA0, 0xB0, 42)) == Commit(
        flags=0, commit_lsn=Lsn(0xA0), end_lsn=Lsn(0xB0), commit_time=from_pg_timestamp(42)
    )


def test_origin() -> None:
    assert decode_message(pb.origin(0x10, "upstream")) == Origin(Lsn(0x10), "upstream")


def test_relation_with_key_flags_and_typmods() -> None:
    raw = pb.relation(
        16384, "public", "orders", "d", [(1, "id", 20, -1), (0, "amount", 1700, 786438)]
    )
    assert decode_message(raw) == Relation(
        oid=16384,
        namespace="public",
        name="orders",
        replica_identity="d",
        columns=(Column(True, "id", 20, -1), Column(False, "amount", 1700, 786438)),
    )


def test_relation_in_pg_catalog_has_empty_namespace_mapped() -> None:
    rel = decode_message(pb.relation(1, "", "t", "f", []))
    assert isinstance(rel, Relation)
    assert rel.namespace == "pg_catalog"


def test_type() -> None:
    assert decode_message(pb.type_(90001, "public", "mood")) == TypeInfo(90001, "public", "mood")


def test_insert_with_null_text_and_binary_columns() -> None:
    msg = decode_message(pb.insert(5, ["1", None, b"\x00\xff", ""]))
    assert msg == Insert(5, (b"1", None, Binary(b"\x00\xff"), b""))


def test_update_without_old_tuple_keeps_unchanged_toast_placeholder() -> None:
    msg = decode_message(pb.update(5, ["1", "u", "x"]))
    assert msg == Update(5, old_kind=None, old=None, new=(b"1", UNCHANGED_TOAST, b"x"))


@pytest.mark.parametrize(("kind", "label"), [(b"K", "key"), (b"O", "full")])
def test_update_with_old_tuple(kind: bytes, label: OldKind) -> None:
    msg = decode_message(pb.update(5, ["2", "b"], old=["1", None], old_kind=kind))
    assert msg == Update(5, old_kind=label, old=(b"1", None), new=(b"2", b"b"))


@pytest.mark.parametrize(("kind", "label"), [(b"K", "key"), (b"O", "full")])
def test_delete(kind: bytes, label: OldKind) -> None:
    assert decode_message(pb.delete(5, ["9", None], old_kind=kind)) == Delete(
        5, old_kind=label, old=(b"9", None)
    )


@pytest.mark.parametrize(
    ("options", "cascade", "restart"), [(0, False, False), (1, True, False), (3, True, True)]
)
def test_truncate(options: int, cascade: bool, restart: bool) -> None:
    assert decode_message(pb.truncate([5, 6], options)) == Truncate((5, 6), cascade, restart)


def test_logical_message() -> None:
    assert decode_message(pb.message(True, 0x55, "deltaflow", b"\x01end")) == Message(
        transactional=True, lsn=Lsn(0x55), prefix="deltaflow", content=b"\x01end"
    )


def test_text_values_are_not_decoded_as_cstrings() -> None:
    # A text value may legally contain bytes that look like protocol framing.
    msg = decode_message(pb.insert(5, ["a\nb\tc'\"\\"]))
    assert msg == Insert(5, (b"a\nb\tc'\"\\",))


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"B" + bytes(3),  # truncated
        pb.begin(1, 2, 3) + b"x",  # trailing garbage
        b"I" + bytes(4) + b"X" + bytes(2),  # bad tuple marker
        pb.insert(5, ["1"])[:-1],  # truncated text value
        b"S" + bytes(10),  # streaming messages are never requested
    ],
)
def test_malformed_or_unrequested_messages_raise(raw: bytes) -> None:
    with pytest.raises(ProtocolError):
        decode_message(raw)


def test_bad_column_kind_raises() -> None:
    raw = b"I" + (5).to_bytes(4, "big") + b"N" + (1).to_bytes(2, "big") + b"q"
    with pytest.raises(ProtocolError, match="column kind"):
        decode_message(raw)
