"""The incremental join is bilinear: Δ(L ⋈ R) = ΔL ⋈ R + L' ⋈ ΔR, where L'
already includes ΔL. Applying it batch by batch and accumulating must equal
joining the accumulated inputs, after every batch."""

from __future__ import annotations

import itertools
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Delta, FrontierViolationError, Multiset, Row, accumulate
from deltaflow.operators import Join, JoinStateError, Source
from deltaflow.replication.lsn import Lsn
from deltaflow.state.store import StateStore

_names = itertools.count()


def make_join(store: StateStore, left_prefix: str | None = "l") -> Join:
    return Join(
        Source("public.l"),
        Source("public.r"),
        left_on=("k",),
        right_on=("k",),
        left_prefix=left_prefix,
        right_prefix="r",
        store=store,
        name=f"j{next(_names)}",
    )


def naive_join(left: Multiset[Row], right: Multiset[Row]) -> Multiset[Row]:
    out: Multiset[Row] = Multiset()
    for l_row, nl in left.items():
        for r_row, nr in right.items():
            # SQL: NULL = NULL is not true, so NULL keys never match.
            if l_row["k"] is not None and l_row["k"] == r_row["k"]:
                merged = {f"l.{c}": v for c, v in l_row.items()}
                merged |= {f"r.{c}": v for c, v in r_row.items()}
                out.add(Row(merged), nl * nr)
    return out


keys = st.one_of(st.none(), st.integers(0, 3))
left_rows = st.builds(lambda k, v: Row({"k": k, "v": v}), keys, st.integers(0, 2))
right_rows = st.builds(lambda k, w: Row({"k": k, "w": w}), keys, st.integers(0, 2))

# One step = what one transaction does to each side: rows to add, and picks
# among existing rows to remove (so collections never go negative).
removes = st.lists(st.integers(0, 50), max_size=3)
steps = st.lists(
    st.tuples(
        st.tuples(st.lists(left_rows, max_size=4), removes),
        st.tuples(st.lists(right_rows, max_size=4), removes),
    ),
    max_size=12,
)


def step_deltas(
    model: Multiset[Row], adds: list[Row], removes: list[int], t: Lsn
) -> list[Delta[Row]]:
    out = [Delta(r, t, 1) for r in adds]
    present = [r for r, n in model.items() for _ in range(n)]
    for pick in removes:
        if present:
            out.append(Delta(present.pop(pick % len(present)), t, -1))
    for d in out:
        model.add(d.row, d.diff)
    return out


@given(steps)
def test_incremental_join_equals_join_of_accumulated_inputs(
    state_store: StateStore, script: list[Any]
) -> None:
    join = make_join(state_store)
    left: Multiset[Row] = Multiset()
    right: Multiset[Row] = Multiset()
    output: list[Delta[Row]] = []
    for n, ((l_adds, l_removes), (r_adds, r_removes)) in enumerate(script):
        t = Lsn(n)
        produced = join.process(0, step_deltas(left, l_adds, l_removes, t))
        produced += join.process(1, step_deltas(right, r_adds, r_removes, t))
        assert all(d.time == t for d in produced)
        output += produced
        assert accumulate(output) == naive_join(left, right)


def test_unconsolidated_batches_and_duplicates(state_store: StateStore) -> None:
    join = make_join(state_store)
    a = Row({"k": 1, "v": 0})
    t = Lsn(1)
    out = join.process(0, [Delta(a, t, 1), Delta(a, t, 1), Delta(a, t, -1), Delta(a, t, 1)])
    assert out == []  # nothing on the right yet
    out = join.process(1, [Delta(Row({"k": 1, "w": 9}), t, 3)])
    assert accumulate(out) == Multiset.from_pairs(
        [(Row({"l.k": 1, "l.v": 0, "r.k": 1, "r.w": 9}), 6)]  # 2 x 3
    )


def test_keys_join_by_value_not_by_type(state_store: StateStore) -> None:
    # bigint = numeric in SQL; 1 == Decimal("1.00") in Python.
    join = make_join(state_store)
    join.process(0, [Delta(Row({"k": 1, "v": 0}), Lsn(1), 1)])
    out = join.process(1, [Delta(Row({"k": Decimal("1.00"), "w": 0}), Lsn(1), 1)])
    assert len(out) == 1


def test_null_keys_never_match(state_store: StateStore) -> None:
    join = make_join(state_store)
    join.process(0, [Delta(Row({"k": None, "v": 0}), Lsn(1), 1)])
    assert join.process(1, [Delta(Row({"k": None, "w": 0}), Lsn(1), 1)]) == []


def test_composite_keys(state_store: StateStore) -> None:
    join = Join(
        Source("public.l"),
        Source("public.r"),
        left_on=("a", "b"),
        right_on=("x", "y"),
        left_prefix="l",
        right_prefix="r",
        store=state_store,
        name=f"j{next(_names)}",
    )
    t = Lsn(1)
    join.process(0, [Delta(Row({"a": 1, "b": "p"}), t, 1), Delta(Row({"a": 1, "b": "q"}), t, 1)])
    out = join.process(1, [Delta(Row({"x": 1, "y": "q"}), t, 1)])
    assert [dict(d.row) for d in out] == [{"l.a": 1, "l.b": "q", "r.x": 1, "r.y": "q"}]


def test_an_already_qualified_left_side_is_kept_as_is(state_store: StateStore) -> None:
    join = make_join(state_store, left_prefix=None)
    join.process(1, [Delta(Row({"k": 5, "w": 0}), Lsn(1), 1)])
    out = join.process(0, [Delta(Row({"o.k": 5, "o.v": 1}), Lsn(1), 1)])
    # The key column is found by its unqualified name when that is unambiguous.
    assert [dict(d.row) for d in out] == [{"o.k": 5, "o.v": 1, "r.k": 5, "r.w": 0}]


def test_retracting_more_than_was_inserted_raises(state_store: StateStore) -> None:
    join = make_join(state_store)
    join.process(0, [Delta(Row({"k": 1, "v": 0}), Lsn(1), 1)])
    with pytest.raises(JoinStateError, match="multiplicity -1"):
        join.process(0, [Delta(Row({"k": 1, "v": 0}), Lsn(2), -2)])


def test_time_must_not_go_backwards(state_store: StateStore) -> None:
    # The indices hold accumulated state without times; that is only valid
    # if every delta is at or after everything already in them.
    join = make_join(state_store)
    join.process(0, [Delta(Row({"k": 1, "v": 0}), Lsn(5), 1)])
    with pytest.raises(FrontierViolationError):
        join.process(1, [Delta(Row({"k": 1, "w": 0}), Lsn(4), 1)])


def test_unqualified_lookup_on_joined_rows() -> None:
    r = Row({"orders.status": "paid", "orders.id": 1, "customers.id": 7})
    assert r["status"] == "paid"
    assert r["orders.id"] == 1
    assert "status" in r
    with pytest.raises(KeyError, match="ambiguous"):
        r["id"]
    with pytest.raises(KeyError):
        r["missing"]
    with pytest.raises(KeyError):
        r["orders.missing"]
