"""Map and filter are linear: they commute with + and with negation, so
applying them to deltas and accumulating gives the same collection as
accumulating first and applying them to the whole collection. That equality,
at every frontier, is what "incremental equals recompute" means for them."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Delta, Frontier, Multiset, Row, accumulate, consolidate
from deltaflow.operators import Filter, Map, Operator, Source
from deltaflow.replication.lsn import Lsn

rows = st.builds(
    lambda k, v: Row({"k": k, "v": v}), st.integers(0, 4), st.one_of(st.none(), st.integers(-3, 3))
)
deltas = st.builds(Delta, rows, st.integers(0, 5).map(Lsn), st.integers(-2, 2))
batches = st.lists(deltas, max_size=30)


def positive_v(r: Row) -> bool:
    return r["v"] is not None and r["v"] > 0


def project_k(r: Row) -> Mapping[str, Any]:
    # Not injective: distinct inputs collapse onto the same output row, so
    # multiplicities must add up.
    return {"k": r["k"]}


def v_squared(r: Row) -> Mapping[str, Any]:
    return {"k": r["k"], "v2": None if r["v"] is None else r["v"] ** 2}


def make_filter() -> Operator:
    return Filter(Source("t"), positive_v)


def make_map_project() -> Operator:
    return Map(Source("t"), project_k)


def make_map_square() -> Operator:
    return Map(Source("t"), v_squared)


OPERATORS: dict[str, tuple[Callable[[], Operator], Callable[[Multiset[Row]], Multiset[Row]]]] = {
    "filter": (
        make_filter,
        lambda m: Multiset.from_pairs((r, n) for r, n in m.items() if positive_v(r)),
    ),
    "map-project": (
        make_map_project,
        lambda m: Multiset.from_pairs((Row(project_k(r)), n) for r, n in m.items()),
    ),
    "map-square": (
        make_map_square,
        lambda m: Multiset.from_pairs((Row(v_squared(r)), n) for r, n in m.items()),
    ),
}
op_names = st.sampled_from(sorted(OPERATORS))


def run(op: Operator, batch: list[Delta[Row]]) -> set[tuple[Row, Lsn, int]]:
    """Consolidated output as a set: consolidation orders rows within a time
    by first appearance, and rows have no natural order to compare by."""
    return as_set(op.process(0, batch))


def as_set(batch: list[Delta[Row]]) -> set[tuple[Row, Lsn, int]]:
    return {(d.row, d.time, d.diff) for d in consolidate(batch)}


@given(op_names, batches, batches)
def test_operator_distributes_over_concatenation(
    name: str, a: list[Delta[Row]], b: list[Delta[Row]]
) -> None:
    make, _ = OPERATORS[name]
    op = make()
    assert run(op, [*a, *b]) == as_set([*op.process(0, a), *op.process(0, b)])


@given(op_names, batches)
def test_operator_commutes_with_negation(name: str, a: list[Delta[Row]]) -> None:
    make, _ = OPERATORS[name]
    op = make()
    negated = [Delta(d.row, d.time, -d.diff) for d in a]
    assert run(op, negated) == {(row, t, -diff) for row, t, diff in run(op, a)}


@given(op_names, batches)
def test_operator_on_consolidated_input_equals_consolidated_output(
    name: str, a: list[Delta[Row]]
) -> None:
    make, _ = OPERATORS[name]
    op = make()
    assert run(op, consolidate(a)) == run(op, a)


@given(op_names, batches)
def test_incremental_equals_recompute_at_every_frontier(name: str, a: list[Delta[Row]]) -> None:
    make, on_collection = OPERATORS[name]
    op = make()
    out = op.process(0, a)
    for t in range(7):
        f = Frontier.at(Lsn(t))
        assert accumulate(out, f) == on_collection(accumulate(a, f))


@given(batches)
def test_operators_preserve_times(a: list[Delta[Row]]) -> None:
    for make, _ in OPERATORS.values():
        out = make().process(0, a)
        assert {d.time for d in out} <= {d.time for d in a}


def test_filter_requires_a_bool() -> None:
    # A predicate returning None (SQL-style "unknown") must not silently mean
    # "drop the row"; the caller has to decide.
    op = Filter(Source("t"), lambda r: r.get("v"))  # type: ignore[arg-type,return-value]
    with pytest.raises(TypeError, match=r"filter predicate .* returned NoneType"):
        op.process(0, [Delta(Row({"v": None}), Lsn(1), 1)])


def test_map_output_must_be_a_mapping_of_hashable_values() -> None:
    op = Map(Source("t"), lambda r: {"tags": [r["v"]]})
    with pytest.raises(TypeError, match="column 'tags'"):
        op.process(0, [Delta(Row({"v": 1}), Lsn(1), 1)])
    not_mapping = Map(Source("t"), lambda r: (r["v"],))  # type: ignore[arg-type,return-value]
    with pytest.raises(TypeError, match=r"map function .* returned tuple"):
        not_mapping.process(0, [Delta(Row({"v": 1}), Lsn(1), 1)])


def test_frontier_of_a_derived_operator_is_the_meet_of_its_inputs() -> None:
    src = Source("t")
    op = Map(Filter(src, positive_v), project_k)
    assert op.frontier == Frontier.at(Lsn(0))
    src.advance(Frontier.at(Lsn(7)))
    assert op.frontier == Frontier.at(Lsn(7))
