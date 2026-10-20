"""Loading an arrangement in chunks while the stream runs (decision 0009).

The model: a table's true history is a list of transactions. The stream
applies them in order. Meanwhile chunks are read, each showing the table as
of a consistent point CP that is never behind what the stream has applied
(CP is created after it), in primary-key pages. Keys the loader asks to
re-read are read again at a later CP. Once every page is read, no re-read
is pending, and the stream has passed every CP, the arrangement must equal
the table. From then on it is live: its deltas must keep it equal.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from deltaflow.core import Multiset, Row
from deltaflow.replication.changes import RowDelete, RowInsert, RowUpdate, TableTruncate
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Column, Relation
from deltaflow.state import SourceStateError, TableArrangement, TableSchema
from deltaflow.state.store import StateStore

SCHEMA = TableSchema("public.t", columns=("id", "a", "bio"), key=("id",))
REL = Relation(1, "public", "t", "d", tuple(Column(c == "id", c, 25, -1) for c in SCHEMA.columns))

type State = dict[int, dict[str, Any]]
type Change = RowInsert | RowUpdate | RowDelete | TableTruncate


@dataclass(frozen=True)
class Op:
    kind: Literal["insert", "update", "rekey", "delete", "truncate"]
    pick: int
    a: int
    bio: str
    set_a: bool
    set_bio: bool


ops = st.builds(
    Op,
    st.sampled_from(["insert", "insert", "update", "update", "rekey", "delete", "truncate"]),
    st.integers(0, 10_000),
    st.integers(0, 3),
    st.sampled_from(["", "x", "long"]),
    st.booleans(),
    st.booleans(),
)

#: keys live in 0..KEYS, so deleted keys come back and inserts land both in
#: pages already read and in pages still ahead
KEYS = 40


def changes_of(op: Op, state: State, lsn: Lsn, sharded: bool) -> list[Change]:
    """The changes pgoutput sends for `op` on `state`, and `state` updated.
    An UPDATE that leaves `bio` alone sends it as the TOAST placeholder;
    sharded, except when the row moves between shards, which live sharding
    refuses (arrangement.py) and this model therefore never does."""
    free = [k for k in range(KEYS) if k not in state]
    taken = sorted(state)
    match op.kind:
        case "insert" if free:
            k = free[op.pick % len(free)]
            state[k] = {"id": k, "a": op.a, "bio": op.bio}
            return [RowInsert(REL, lsn, dict(state[k]))]
        case "update" if taken:
            k = taken[op.pick % len(taken)]
            new = dict(state[k])
            if op.set_a:
                new["a"] = op.a
            if op.set_bio:
                new["bio"] = op.bio
            sent = dict(new)
            moves = sharded and by_a(new) != by_a(state[k])
            if new["bio"] == state[k]["bio"] and not moves:
                sent["bio"] = UNCHANGED_TOAST
            state[k] = new
            return [RowUpdate(REL, lsn, None, None, sent)]
        case "rekey" if taken and free:
            k, k2 = taken[op.pick % len(taken)], free[op.pick % len(free)]
            new = {**state.pop(k), "id": k2}
            sent = {**new, "bio": UNCHANGED_TOAST}
            state[k2] = new
            return [RowUpdate(REL, lsn, "key", {"id": k}, sent)]
        case "delete" if taken:
            k = taken[op.pick % len(taken)]
            del state[k]
            return [RowDelete(REL, lsn, "key", {"id": k})]
        case "truncate":
            state.clear()
            return [TableTruncate((REL,), lsn, False, False)]
    return []


@dataclass
class History:
    initial: State
    #: per transaction: (commit LSN, changes)
    txns: list[tuple[Lsn, list[Change]]]
    #: before[i]: the table just before transaction i commits (i == len: after all)
    before: list[State]


def history(initial_keys: list[int], txn_ops: list[list[Op]], sharded: bool) -> History:
    state: State = {k: {"id": k, "a": k % 4, "bio": "init"} for k in initial_keys}
    initial = {k: dict(v) for k, v in state.items()}
    txns, before = [], []
    for i, txn in enumerate(txn_ops):
        before.append({k: dict(v) for k, v in state.items()})
        lsn = Lsn(0x100 + 0x10 * i)
        txns.append((lsn, [c for op in txn for c in changes_of(op, state, lsn, sharded)]))
    before.append({k: dict(v) for k, v in state.items()})
    return History(initial, txns, before)


def cp_of(h: History, m: int) -> Lsn:
    """The consistent point just before transaction m commits."""
    return h.txns[m][0] if m < len(h.txns) else Lsn(0x100 + 0x10 * len(h.txns))


def owned(owns: Callable[[Mapping[str, Any]], bool] | None, state: State) -> Multiset[Row]:
    return Multiset.of(Row(r) for r in state.values() if owns is None or owns(r))


def live_apply(arr: TableArrangement, changes: list[Change], acc: Multiset[Row]) -> None:
    for c in changes:
        pairs = arr.truncate() if isinstance(c, TableTruncate) else arr.apply(c)
        for r, d in pairs:
            acc.add(r, d)


def by_a(row: Mapping[str, Any]) -> bool:
    return bool(row["a"] % 2 == 0)


def loading(
    store: StateStore, owns: Callable[[Mapping[str, Any]], bool] | None = None
) -> TableArrangement:
    space = uuid.uuid4().hex
    return TableArrangement(
        SCHEMA,
        store.keyspace(f"arr:{space}"),
        owns=owns,
        marks=store.keyspace(f"mark:{space}"),
        rereads=store.keyspace(f"reread:{space}"),
    )


def update(lsn: int, new: dict[str, Any], old: dict[str, Any] | None = None) -> RowUpdate:
    return RowUpdate(REL, Lsn(lsn), None if old is None else "key", old, new)


@pytest.mark.parametrize("owns", [None, by_a], ids=["unsharded", "sharded"])
@settings(max_examples=300)
@given(
    initial_keys=st.lists(st.integers(0, KEYS - 1), unique=True, max_size=25),
    txn_ops=st.lists(st.lists(ops, min_size=1, max_size=4), max_size=30),
    # page size; per step: transactions the stream applies, and how far
    # ahead of the stream the step's consistent point is
    plan=st.tuples(
        st.integers(1, 6), st.lists(st.tuples(st.integers(0, 3), st.integers(0, 4)), max_size=60)
    ),
)
def test_chunked_load_converges_to_the_table_and_stays_live(
    state_store: StateStore,
    owns: Callable[[Mapping[str, Any]], bool] | None,
    initial_keys: list[int],
    txn_ops: list[list[Op]],
    plan: tuple[int, list[tuple[int, int]]],
) -> None:
    page, schedule = plan
    h = history(initial_keys, txn_ops, sharded=owns is not None)
    arr = loading(state_store, owns)
    applied = 0  # transactions the stream has applied
    after: int | None = -1  # paging position: last key read; None once done
    max_cp = Lsn(0)
    steps = iter(schedule)

    def read(keys: list[int], m: int) -> tuple[Lsn, list[Row]]:
        nonlocal max_cp
        cp = cp_of(h, m)
        max_cp = max(max_cp, cp)
        return cp, [Row(h.before[m][k]) for k in keys if k in h.before[m]]

    def stream(n: int) -> None:
        nonlocal applied
        for lsn, changes in h.txns[applied : applied + n]:
            for c in changes:
                if isinstance(c, TableTruncate):
                    arr.truncate_loading(lsn)
                else:
                    arr.apply_loading(c, lsn)
            applied += 1

    def live() -> bool:
        # Every page read, no re-read pending, and the stream past every CP:
        # the next transaction to apply commits at or after the last CP.
        return after is None and not arr.rereads() and cp_of(h, applied) >= max_cp

    while not live():
        streamed, ahead = next(steps, (1, 0))
        m = min(applied + ahead, len(h.txns))  # a CP is never behind the stream
        if after is not None:
            keys = sorted(k for k in h.before[m] if k > after)[:page]
            cp, rows = read(keys, m)
            arr.load_chunk(rows, cp)
            after = keys[-1] if len(keys) == page else None
        elif pending := arr.rereads():
            cp, rows = read([k for (k,) in pending], m)
            arr.reread(pending, rows, cp)
        stream(streamed)

    arr.finish_loading()
    assert Multiset.of(arr.rows()) == owned(owns, h.before[applied])
    assert len(arr) == len(owned(owns, h.before[applied]))
    acc = Multiset.of(arr.rows())
    for _lsn, changes in h.txns[applied:]:
        live_apply(arr, changes, acc)
    assert acc == owned(owns, h.before[len(h.txns)])


def test_a_change_older_than_the_chunk_that_read_its_key_is_ignored(
    state_store: StateStore,
) -> None:
    arr = loading(state_store)
    arr.load_chunk([Row({"id": 1, "a": 2, "bio": "new"})], Lsn(0x200))
    # The stream catches up with an update the chunk already contains.
    arr.apply_loading(update(0x180, {"id": 1, "a": 1, "bio": "old"}), Lsn(0x180))
    assert list(arr.rows()) == [Row({"id": 1, "a": 2, "bio": "new"})]
    # A later one is applied, with its unchanged TOAST value filled in.
    arr.apply_loading(update(0x210, {"id": 1, "a": 3, "bio": UNCHANGED_TOAST}), Lsn(0x210))
    assert list(arr.rows()) == [Row({"id": 1, "a": 3, "bio": "new"})]


def test_a_key_with_a_mark_keeps_the_streams_consistency_checks(state_store: StateStore) -> None:
    arr = loading(state_store)
    arr.load_chunk([Row({"id": 1, "a": 2, "bio": "x"})], Lsn(0x200))
    arr.apply_loading(RowDelete(REL, Lsn(0x210), "key", {"id": 1}), Lsn(0x210))
    # The chunk read the key and the stream deleted it since: a second
    # delete means a change was duplicated upstream.
    with pytest.raises(SourceStateError, match="delete of unknown key"):
        arr.apply_loading(RowDelete(REL, Lsn(0x220), "key", {"id": 1}), Lsn(0x220))


def test_an_update_the_loader_cannot_complete_is_re_read(state_store: StateStore) -> None:
    """A key moves into a page already read, keeping its out-of-line value:
    the loader never held the old row, so the value is unknown until the
    key is read again."""
    arr = loading(state_store)
    arr.load_chunk([Row({"id": 1, "a": 0, "bio": "x"})], Lsn(0x200))
    moved = update(0x210, {"id": 2, "a": 0, "bio": UNCHANGED_TOAST}, old={"id": 9})
    arr.apply_loading(moved, Lsn(0x210))
    assert arr.rereads() == [(2,)]
    arr.reread([(2,)], [Row({"id": 2, "a": 0, "bio": "big"})], Lsn(0x300))
    assert arr.rereads() == []
    assert sorted(r["id"] for r in arr.rows()) == [1, 2]
