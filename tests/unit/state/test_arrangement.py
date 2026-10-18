"""The source arrangement: Deltaflow's keyed copy of a source table.

pgoutput (default replica identity) gives an UPDATE's new row, minus any
unchanged out-of-line value, and at most the old row's *key*. A DELETE gives
only the key. Views need whole rows on both sides of every change, so the
arrangement fills them in from its own copy. The property: after every
change, the accumulated deltas equal the table, and every retraction is
exactly the row that was there before.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Literal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Multiset, Row
from deltaflow.replication.changes import RowDelete, RowInsert, RowUpdate
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Column, Relation
from deltaflow.state import SchemaChangedError, SourceStateError, TableArrangement, TableSchema
from deltaflow.state.store import StateStore

SCHEMA = TableSchema("public.t", columns=("id", "a", "bio"), key=("id",))
LSN = Lsn(0x100)


def relation(
    identity: Literal["d", "f"] = "d", columns: tuple[str, ...] = SCHEMA.columns
) -> Relation:
    return Relation(
        1,
        "public",
        "t",
        identity,
        tuple(Column(identity == "f" or c == "id", c, 25, -1) for c in columns),
    )


DEFAULT, FULL = relation("d"), relation("f")


def fresh(store: StateStore, schema: TableSchema = SCHEMA) -> TableArrangement:
    return TableArrangement(schema, store.keyspace(f"arr:{uuid.uuid4().hex}"))


def row(id_: int, a: object, bio: object) -> dict[str, Any]:
    return {"id": id_, "a": a, "bio": bio}


@dataclass(frozen=True)
class Op:
    kind: Literal["insert", "update", "rekey", "delete", "truncate"]
    pick: int = 0
    a: int | None = None
    bio: str | None = None
    set_a: bool = True
    set_bio: bool = False
    #: Unchanged bio sent as the TOAST placeholder, as Postgres does for
    #: out-of-line values an UPDATE did not touch.
    toast: bool = True


ops = st.one_of(
    st.builds(
        Op, st.just("insert"), a=st.none() | st.integers(0, 3), bio=st.none() | st.text(max_size=3)
    ),
    st.builds(
        Op,
        st.just("update"),
        st.integers(0, 100),
        a=st.none() | st.integers(0, 3),
        bio=st.none() | st.text(max_size=3),
        set_a=st.booleans(),
        set_bio=st.booleans(),
        toast=st.booleans(),
    ),
    st.builds(Op, st.just("rekey"), st.integers(0, 100), toast=st.booleans()),
    st.builds(Op, st.just("delete"), st.integers(0, 100)),
    st.builds(Op, st.just("truncate")),
)


def execute(  # noqa: PLR0911 - one return per operation kind reads best
    arr: TableArrangement, model: dict[int, dict[str, Any]], op: Op, next_id: int, rel: Relation
) -> list[tuple[Row, int]]:
    """Apply `op` to the model and feed the arrangement what Postgres would send."""
    full = rel.replica_identity == "f"
    ids = list(model)
    if op.kind == "insert":
        model[next_id] = row(next_id, op.a, op.bio)
        return arr.apply(RowInsert(rel, LSN, dict(model[next_id])))
    if op.kind == "truncate":
        model.clear()
        return arr.truncate()
    if not ids:
        return []
    id_ = ids[op.pick % len(ids)]
    before = model[id_]
    old_full = {**before, "bio": UNCHANGED_TOAST} if op.toast else dict(before)
    if op.kind == "delete":
        del model[id_]
        old = old_full if full else {"id": id_}
        return arr.apply(RowDelete(rel, LSN, "full" if full else "key", old))
    if op.kind == "rekey":
        after = {**model.pop(id_), "id": next_id}
        model[next_id] = after
        new = {**after, "bio": UNCHANGED_TOAST} if op.toast else dict(after)
        old = old_full if full else {"id": id_}
        return arr.apply(RowUpdate(rel, LSN, "full" if full else "key", old, new))
    after = dict(before)
    if op.set_a:
        after["a"] = op.a
    if op.set_bio:
        after["bio"] = op.bio
    model[id_] = after
    sent = {**after, "bio": UNCHANGED_TOAST} if op.toast and not op.set_bio else dict(after)
    if full:
        return arr.apply(RowUpdate(rel, LSN, "full", old_full, sent))
    return arr.apply(RowUpdate(rel, LSN, None, None, sent))


@given(st.lists(ops, max_size=60), st.sampled_from(["d", "f"]))
def test_accumulated_deltas_equal_the_table_after_every_change(
    state_store: StateStore, script: list[Op], identity: Literal["d", "f"]
) -> None:
    rel = relation(identity)
    arr = fresh(state_store)
    model: dict[int, dict[str, Any]] = {i: row(i, i, f"bio{i}") for i in range(3)}
    for r in model.values():
        arr.load(Row(r))
    contents = Multiset.of(Row(r) for r in model.values())
    for n, op in enumerate(script):
        before = {i: Row(r) for i, r in model.items()}
        out = execute(arr, model, op, next_id=100 + n, rel=rel)
        for changed, diff in out:
            assert diff in (1, -1)
            if diff == -1:
                # A retraction is exactly the row that was there: whole, with
                # the real bio rather than the TOAST placeholder.
                assert changed in before.values()
            contents.add(changed, diff)
        assert contents == Multiset.of(Row(r) for r in model.values())
        assert sorted(map(dict, arr.rows()), key=lambda r: r["id"]) == sorted(
            model.values(), key=lambda r: r["id"]
        )
        assert len(arr) == len(model)


def test_update_resolves_unchanged_toast_from_the_stored_row(state_store: StateStore) -> None:
    arr = fresh(state_store)
    arr.load(Row(row(1, 10, "x" * 9000)))
    out = arr.apply(RowUpdate(DEFAULT, LSN, None, None, row(1, 11, UNCHANGED_TOAST)))
    assert out == [(Row(row(1, 10, "x" * 9000)), -1), (Row(row(1, 11, "x" * 9000)), 1)]
    assert arr.counters["unchanged_toast_resolved"] == 1


def test_delete_retracts_the_whole_stored_row(state_store: StateStore) -> None:
    arr = fresh(state_store)
    arr.load(Row(row(1, 10, "bio")))
    assert arr.apply(RowDelete(DEFAULT, LSN, "key", {"id": 1})) == [(Row(row(1, 10, "bio")), -1)]
    assert len(arr) == 0


def test_composite_keys(state_store: StateStore) -> None:
    schema = TableSchema("public.t", columns=("a", "b", "v"), key=("a", "b"))
    rel = Relation(
        1, "public", "t", "d", tuple(Column(c != "v", c, 25, -1) for c in schema.columns)
    )
    arr = fresh(state_store, schema)
    arr.load(Row({"a": 1, "b": 1, "v": "x"}))
    arr.load(Row({"a": 1, "b": 2, "v": "y"}))
    out = arr.apply(RowDelete(rel, LSN, "key", {"a": 1, "b": 2}))
    assert out == [(Row({"a": 1, "b": 2, "v": "y"}), -1)]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (RowInsert(DEFAULT, LSN, row(1, 0, "dup")), "insert of existing key"),
        (RowUpdate(DEFAULT, LSN, None, None, row(9, 0, "x")), "update of unknown key"),
        (RowDelete(DEFAULT, LSN, "key", {"id": 9}), "delete of unknown key"),
        (RowUpdate(DEFAULT, LSN, "key", {"id": 1}, row(2, 0, "x")), "moves onto existing key"),
        (
            RowInsert(DEFAULT, LSN, row(5, 0, UNCHANGED_TOAST)),
            "unchanged-TOAST placeholder in an INSERT",
        ),
        (
            RowUpdate(DEFAULT, LSN, None, None, {**row(1, 0, "x"), "id": UNCHANGED_TOAST}),
            "key column",
        ),
        (
            RowDelete(FULL, LSN, "full", row(1, 999, "a")),
            "old row from the server disagrees with the stored row",
        ),
    ],
)
def test_changes_that_contradict_the_arrangement_raise(
    state_store: StateStore, change: Any, message: str
) -> None:
    # Each of these means a change was lost, duplicated or misapplied
    # upstream. Carrying on would publish a wrong view, so stop instead.
    arr = fresh(state_store)
    arr.load(Row(row(1, 0, "a")))
    arr.load(Row(row(2, 0, "b")))
    with pytest.raises(SourceStateError, match=message):
        arr.apply(change)


def test_duplicate_snapshot_row_raises(state_store: StateStore) -> None:
    arr = fresh(state_store)
    arr.load(Row(row(1, 0, "a")))
    with pytest.raises(SourceStateError, match="duplicate key"):
        arr.load(Row(row(1, 0, "a")))


def test_schema_change_raises(state_store: StateStore) -> None:
    arr = fresh(state_store)
    widened = relation("d", columns=("id", "a", "bio", "extra"))
    with pytest.raises(
        SchemaChangedError, match=r"columns changed from \(id, a, bio\) to \(id, a, bio, extra\)"
    ):
        arr.apply(RowInsert(widened, LSN, {**row(1, 0, "x"), "extra": 1}))
