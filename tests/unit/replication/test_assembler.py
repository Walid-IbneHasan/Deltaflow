"""Turning decoded pgoutput messages into typed, commit-bounded transactions."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from deltaflow.replication.changes import (
    RowDelete,
    RowInsert,
    RowUpdate,
    TableTruncate,
    Transaction,
    TransactionAssembler,
    TransactionPart,
)
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Message, ProtocolError, decode_message

from . import pgoutput_bytes as pb

INT8, TEXT, NUMERIC, TIMESTAMPTZ, BOOL = 20, 25, 1700, 1184, 16

ORDERS = pb.relation(
    100,
    "public",
    "orders",
    "d",
    [(1, "id", INT8, -1), (0, "amount", NUMERIC, -1), (0, "at", TIMESTAMPTZ, -1)],
)
CUSTOMERS = pb.relation(
    200,
    "public",
    "customers",
    "d",
    [(1, "id", INT8, -1), (0, "bio", TEXT, -1), (0, "vip", BOOL, -1)],
)


def feed(asm: TransactionAssembler, *raws: bytes) -> list[Transaction | TransactionPart | Message]:
    out: list[Transaction | TransactionPart | Message] = []
    for i, raw in enumerate(raws):
        event = asm.feed(Lsn(0x1000 + i), decode_message(raw))
        if event is not None:
            out.append(event)
    return out


def test_nothing_is_emitted_before_commit() -> None:
    asm = TransactionAssembler()
    assert feed(asm, ORDERS, pb.begin(0x2000, 0, 9), pb.insert(100, ["1", "2.50", None])) == []
    [txn] = feed(asm, pb.commit(0x2000, 0x2010, 0))
    assert isinstance(txn, Transaction)
    assert txn.xid == 9
    assert txn.commit_lsn == Lsn(0x2000)
    assert txn.end_lsn == Lsn(0x2010)


def test_values_are_typed_by_column_oid() -> None:
    asm = TransactionAssembler()
    [txn] = feed(
        asm,
        pb.begin(0x2000, 0, 9),
        ORDERS,
        pb.insert(100, ["7", "12.30", "2026-10-18 12:00:00.5+00"]),
        CUSTOMERS,
        pb.insert(200, ["3", None, "t"]),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(txn, Transaction)
    order, customer = txn.changes
    assert isinstance(order, RowInsert)
    assert order.relation.qualified_name == "public.orders"
    assert order.new == {
        "id": 7,
        "amount": Decimal("12.30"),
        "at": datetime(2026, 10, 18, 12, 0, 0, 500000, tzinfo=UTC),
    }
    assert str(order.new["amount"]) == "12.30"  # numeric scale is preserved
    assert isinstance(customer, RowInsert)
    assert customer.new == {"id": 3, "bio": None, "vip": True}


def test_change_lsns_come_from_the_wal_position_of_each_message() -> None:
    asm = TransactionAssembler()
    [txn] = feed(
        asm,
        ORDERS,
        pb.begin(0x2000, 0, 1),
        pb.insert(100, ["1", "1", None]),
        pb.insert(100, ["2", "1", None]),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(txn, Transaction)
    assert [c.lsn for c in txn.changes] == [Lsn(0x1002), Lsn(0x1003)]


def test_update_keeps_unchanged_toast_sentinel_and_key_only_old_row() -> None:
    asm = TransactionAssembler()
    [txn] = feed(
        asm,
        CUSTOMERS,
        pb.begin(0x2000, 0, 1),
        pb.update(200, ["3", "u", "f"]),
        pb.update(200, ["4", "u", "f"], old=["3", None, None], old_kind=b"K"),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(txn, Transaction)
    plain, rekey = txn.changes
    assert isinstance(plain, RowUpdate)
    assert plain.old is None
    assert plain.new == {"id": 3, "bio": UNCHANGED_TOAST, "vip": False}
    assert isinstance(rekey, RowUpdate)
    assert rekey.old_kind == "key"
    assert rekey.old == {"id": 3}  # only replica-identity columns, not fake NULLs


def test_full_old_row_is_kept_whole() -> None:
    asm = TransactionAssembler()
    [txn] = feed(
        asm,
        CUSTOMERS,
        pb.begin(0x2000, 0, 1),
        pb.delete(200, ["3", None, "t"], old_kind=b"O"),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(txn, Transaction)
    [change] = txn.changes
    assert isinstance(change, RowDelete)
    assert change.old_kind == "full"
    assert change.old == {"id": 3, "bio": None, "vip": True}


def test_key_of_update_and_delete() -> None:
    asm = TransactionAssembler()
    [txn] = feed(
        asm,
        CUSTOMERS,
        pb.begin(0x2000, 0, 1),
        pb.update(200, ["3", "u", "f"]),
        pb.update(200, ["4", "u", "f"], old=["3", None, None]),
        pb.delete(200, ["4", None, None]),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(txn, Transaction)
    keys = [c.key for c in txn.changes if isinstance(c, RowUpdate | RowDelete)]
    assert keys == [{"id": 3}, {"id": 3}, {"id": 4}]


def test_truncate_resolves_relations() -> None:
    asm = TransactionAssembler()
    [txn] = feed(
        asm,
        ORDERS,
        CUSTOMERS,
        pb.begin(0x2000, 0, 1),
        pb.truncate([100, 200], 1),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(txn, Transaction)
    [change] = txn.changes
    assert isinstance(change, TableTruncate)
    assert [r.name for r in change.relations] == ["orders", "customers"]
    assert change.cascade
    assert not change.restart_identity


def test_origin_is_attached() -> None:
    asm = TransactionAssembler()
    [txn] = feed(asm, pb.begin(0x2000, 0, 1), pb.origin(5, "up"), pb.commit(0x2000, 0x2010, 0))
    assert isinstance(txn, Transaction)
    assert txn.origin == "up"


def test_transactional_message_stays_in_order_and_plain_message_passes_through() -> None:
    asm = TransactionAssembler()
    events = feed(
        asm,
        pb.message(False, 0x10, "p", b"now"),
        ORDERS,
        pb.begin(0x2000, 0, 1),
        pb.insert(100, ["1", "1", None]),
        pb.message(True, 0x20, "p", b"marker"),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(events[0], Message)
    assert events[0].content == b"now"
    txn = events[1]
    assert isinstance(txn, Transaction)
    assert isinstance(txn.changes[0], RowInsert)
    assert isinstance(txn.changes[1], Message)


def test_relation_redefinition_applies_to_later_changes() -> None:
    asm = TransactionAssembler()
    widened = pb.relation(
        100,
        "public",
        "orders",
        "d",
        [
            (1, "id", INT8, -1),
            (0, "amount", NUMERIC, -1),
            (0, "at", TIMESTAMPTZ, -1),
            (0, "note", TEXT, -1),
        ],
    )
    [txn] = feed(
        asm,
        ORDERS,
        pb.begin(0x2000, 0, 1),
        pb.insert(100, ["1", "1", None]),
        widened,
        pb.insert(100, ["2", "1", None, "hi"]),
        pb.commit(0x2000, 0x2010, 0),
    )
    assert isinstance(txn, Transaction)
    first, second = txn.changes
    assert isinstance(first, RowInsert)
    assert isinstance(second, RowInsert)
    assert "note" not in first.new
    assert second.new["note"] == "hi"


@pytest.mark.parametrize(
    ("raws", "match"),
    [
        ([pb.insert(100, ["1", "1", None])], "outside a transaction"),
        ([pb.begin(1, 0, 1), pb.begin(2, 0, 2)], "nested BEGIN"),
        ([pb.commit(1, 2, 0)], "COMMIT without BEGIN"),
        ([pb.begin(1, 0, 1), pb.insert(999, ["1"])], "unknown relation 999"),
        ([ORDERS, pb.begin(1, 0, 1), pb.insert(100, ["1"])], "3 columns"),
        ([pb.begin(0x10, 0, 1), pb.commit(0x11, 0x12, 0)], "commit LSN"),
    ],
)
def test_protocol_violations_raise(raws: list[bytes], match: str) -> None:
    asm = TransactionAssembler()
    with pytest.raises(ProtocolError, match=match):
        feed(asm, *raws)


def test_a_large_transaction_comes_in_parts_before_its_commit() -> None:
    """pgoutput without streaming sends a transaction only once it has
    committed, so its changes can be applied before its end arrives; in
    parts, a large one never has to be in memory whole."""
    asm = TransactionAssembler(max_changes=3)
    inserts = [pb.insert(100, [str(i), "1", None]) for i in range(8)]
    parts = feed(asm, ORDERS, pb.begin(0x2000, 0, 9), *inserts)
    assert all(isinstance(p, TransactionPart) for p in parts)
    assert [len(p.changes) for p in parts if isinstance(p, TransactionPart)] == [3, 3]
    assert {(p.xid, p.commit_lsn) for p in parts if isinstance(p, TransactionPart)} == {
        (9, Lsn(0x2000))
    }
    [txn] = feed(asm, pb.commit(0x2000, 0x2010, 0))
    assert isinstance(txn, Transaction)
    assert txn.continued
    assert [c.new["id"] for c in txn.changes if isinstance(c, RowInsert)] == [6, 7]


def test_without_a_limit_a_transaction_comes_whole() -> None:
    asm = TransactionAssembler()
    inserts = [pb.insert(100, [str(i), "1", None]) for i in range(8)]
    assert feed(asm, ORDERS, pb.begin(0x2000, 0, 9), *inserts) == []
    [txn] = feed(asm, pb.commit(0x2000, 0x2010, 0))
    assert isinstance(txn, Transaction)
    assert not txn.continued
    assert len(txn.changes) == 8
