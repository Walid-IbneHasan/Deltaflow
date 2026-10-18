"""group_by + aggregates, and distinct, against a naive recompute after every
step. Retractions are drawn from a model, so the input never goes negative,
and are biased towards the current extremes: deleting the row that holds a
group's min or max is the case min/max must survive."""

from __future__ import annotations

import itertools
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Delta, FrontierViolationError, Multiset, Row, accumulate
from deltaflow.operators import (
    Aggregate,
    AggregateError,
    Count,
    Distinct,
    GroupBy,
    Max,
    Min,
    Source,
    Sum,
)
from deltaflow.replication.lsn import Lsn
from deltaflow.state.store import StateStore

_names = itertools.count()

AGGS: dict[str, Aggregate] = {
    "n": Count(),
    "nn": Count("v"),
    "s": Sum("v"),
    "lo": Min("v"),
    "hi": Max("v"),
}


def group_by(store: StateStore, keys: tuple[str, ...] = ("g",)) -> GroupBy:
    return GroupBy(
        Source("public.t"), keys=keys, aggregates=AGGS, store=store, name=f"g{next(_names)}"
    )


def naive(rows: Multiset[Row], keys: tuple[str, ...]) -> Multiset[Row]:
    groups: dict[tuple[Any, ...], list[Row]] = {}
    for r, n in rows.items():
        groups.setdefault(tuple(r[k] for k in keys), []).extend([r] * n)
    if not keys:
        groups.setdefault((), [])  # SQL: a global aggregate always has one row
    out: Multiset[Row] = Multiset()
    for key, members in groups.items():
        values = [r["v"] for r in members if r["v"] is not None]
        out.add(
            Row(
                {
                    **dict(zip(keys, key, strict=True)),
                    "n": len(members),
                    "nn": len(values),
                    "s": sum(values) if values else None,
                    "lo": min(values) if values else None,
                    "hi": max(values) if values else None,
                }
            )
        )
    return out


values = st.one_of(
    st.none(), st.integers(-5, 5), st.sampled_from([Decimal("1.50"), Decimal("-2.25")])
)
rows = st.builds(
    lambda g, v: Row({"g": g, "v": v}), st.one_of(st.none(), st.integers(0, 2)), values
)
# per step: rows to add, and removals: ("any", pick) or ("min"/"max", group pick)
removal = st.tuples(st.sampled_from(["any", "min", "max"]), st.integers(0, 50))
steps = st.lists(st.tuples(st.lists(rows, max_size=5), st.lists(removal, max_size=4)), max_size=15)


def removals(model: Multiset[Row], picks: list[tuple[str, int]], t: Lsn) -> list[Delta[Row]]:
    out = []
    for how, pick in picks:
        present = [r for r, n in model.items() for _ in range(n)]
        if how != "any":
            present = [r for r in present if r["v"] is not None]
        if not present:
            continue
        if how == "any":
            victim = present[pick % len(present)]
        else:
            group = present[pick % len(present)]["g"]
            members = [r for r in present if r["g"] == group]
            victim = (min if how == "min" else max)(members, key=lambda r: r["v"])
        model.add(victim, -1)
        out.append(Delta(victim, t, -1))
    return out


@pytest.mark.parametrize("keys", [("g",), ()], ids=["grouped", "global"])
@given(script=steps)
def test_aggregates_equal_recompute_after_every_step(
    state_store: StateStore, keys: tuple[str, ...], script: list[Any]
) -> None:
    op = group_by(state_store, keys)
    model: Multiset[Row] = Multiset()
    output = list(op.initial())
    assert accumulate(output) == naive(model, keys)
    for n, (adds, picks) in enumerate(script):
        t = Lsn(n + 1)
        batch = [Delta(r, t, 1) for r in adds]
        for r in adds:
            model.add(r, 1)
        batch += removals(model, picks, t)
        produced = op.process(0, batch)
        assert all(d.time == t for d in produced)
        output += produced
        assert accumulate(output) == naive(model, keys)


def test_min_and_max_survive_deleting_the_extreme(state_store: StateStore) -> None:
    op = GroupBy(
        Source("public.t"),
        keys=("g",),
        aggregates={"lo": Min("v"), "hi": Max("v")},
        store=state_store,
        name=f"g{next(_names)}",
    )
    out = op.process(0, [Delta(Row({"g": 1, "v": v}), Lsn(1), 1) for v in (5, 3, 9, 3)])
    out += op.process(0, [Delta(Row({"g": 1, "v": 9}), Lsn(2), -1)])  # the max goes
    out += op.process(0, [Delta(Row({"g": 1, "v": 3}), Lsn(3), -1)])  # one of two 3s goes
    assert accumulate(out) == Multiset.of([Row({"g": 1, "lo": 3, "hi": 5})])
    out += op.process(0, [Delta(Row({"g": 1, "v": 3}), Lsn(4), -1)])  # the other 3
    assert accumulate(out) == Multiset.of([Row({"g": 1, "lo": 5, "hi": 5})])
    out += op.process(0, [Delta(Row({"g": 1, "v": 5}), Lsn(5), -1)])
    assert accumulate(out) == Multiset()  # empty group: no row, as in SQL


def test_an_update_inside_a_group_is_one_retraction_and_one_insertion(
    state_store: StateStore,
) -> None:
    op = group_by(state_store)
    op.process(0, [Delta(Row({"g": 1, "v": 2}), Lsn(1), 1)])
    out = op.process(
        0, [Delta(Row({"g": 1, "v": 2}), Lsn(2), -1), Delta(Row({"g": 1, "v": 7}), Lsn(2), 1)]
    )
    assert [(dict(d.row), d.diff) for d in out] == [
        ({"g": 1, "n": 1, "nn": 1, "s": 2, "lo": 2, "hi": 2}, -1),
        ({"g": 1, "n": 1, "nn": 1, "s": 7, "lo": 7, "hi": 7}, 1),
    ]
    # A change that leaves the group's result unchanged emits nothing.
    same = op.process(
        0, [Delta(Row({"g": 1, "v": 7}), Lsn(3), -1), Delta(Row({"g": 1, "v": 7}), Lsn(3), 1)]
    )
    assert same == []


def test_sum_is_exact_or_refuses(state_store: StateStore) -> None:
    op = GroupBy(
        Source("public.t"),
        keys=(),
        aggregates={"s": Sum("v")},
        store=state_store,
        name=f"g{next(_names)}",
    )
    op.initial()
    tiny, huge = Decimal("1E-30"), Decimal(10**30)
    out = op.process(0, [Delta(Row({"v": tiny}), Lsn(1), 1), Delta(Row({"v": huge}), Lsn(1), 1)])
    # 61 significant digits. (huge + tiny in the default 28-digit context
    # would round the tiny part away: the very error this guards against.)
    assert out[-1].row["s"] == Decimal("1" + "0" * 30 + "." + "0" * 29 + "1")
    assert huge + tiny == huge
    with pytest.raises(AggregateError, match="exactly"):
        op.process(
            0,
            [
                Delta(Row({"v": Decimal("1E+2000")}), Lsn(2), 1),
                Delta(Row({"v": Decimal("1E-2000")}), Lsn(2), 1),
            ],
        )


@pytest.mark.parametrize("value", [1.5, True])
def test_sum_refuses_floats_and_bools(state_store: StateStore, value: object) -> None:
    op = GroupBy(
        Source("public.t"),
        keys=(),
        aggregates={"s": Sum("v")},
        store=state_store,
        name=f"g{next(_names)}",
    )
    with pytest.raises(AggregateError, match="Sum"):
        op.process(0, [Delta(Row({"v": value}), Lsn(1), 1)])


def test_retracting_from_an_empty_group_raises(state_store: StateStore) -> None:
    op = group_by(state_store)
    with pytest.raises(AggregateError, match="negative"):
        op.process(0, [Delta(Row({"g": 1, "v": 1}), Lsn(1), -1)])


def test_time_must_not_go_backwards(state_store: StateStore) -> None:
    op = group_by(state_store)
    op.process(0, [Delta(Row({"g": 1, "v": 1}), Lsn(5), 1)])
    with pytest.raises(FrontierViolationError):
        op.process(0, [Delta(Row({"g": 1, "v": 1}), Lsn(4), 1)])


def test_a_batch_spanning_times_emits_per_time(state_store: StateStore) -> None:
    op = GroupBy(
        Source("public.t"),
        keys=("g",),
        aggregates={"n": Count()},
        store=state_store,
        name=f"g{next(_names)}",
    )
    out = op.process(0, [Delta(Row({"g": 1}), Lsn(1), 1), Delta(Row({"g": 1}), Lsn(2), 1)])
    assert [(dict(d.row), d.time, d.diff) for d in out] == [
        ({"g": 1, "n": 1}, Lsn(1), 1),
        ({"g": 1, "n": 1}, Lsn(2), -1),
        ({"g": 1, "n": 2}, Lsn(2), 1),
    ]


distinct_rows = st.builds(
    lambda a, b: Row({"a": a, "b": b}), st.integers(0, 2), st.one_of(st.none(), st.integers(0, 1))
)


@given(
    st.lists(
        st.tuples(st.lists(distinct_rows, max_size=5), st.lists(st.integers(0, 50), max_size=4)),
        max_size=15,
    )
)
def test_distinct_equals_the_set_of_present_rows(
    state_store: StateStore, script: list[Any]
) -> None:
    op = Distinct(Source("public.t"), store=state_store, name=f"d{next(_names)}")
    model: Multiset[Row] = Multiset()
    output: list[Delta[Row]] = []
    for n, (adds, picks) in enumerate(script):
        t = Lsn(n + 1)
        batch = [Delta(r, t, 1) for r in adds]
        for r in adds:
            model.add(r, 1)
        batch += removals(model, [("any", p) for p in picks], t)
        output += op.process(0, batch)
        assert accumulate(output) == Multiset.of(r for r, _ in model.items())
