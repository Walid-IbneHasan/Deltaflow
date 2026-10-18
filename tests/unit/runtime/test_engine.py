"""The engine end to end, without a database: snapshot rows and committed
transactions in, view results out, checked against a model after every
transaction."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Frontier, FrontierViolationError, Multiset, Row
from deltaflow.planner import Deltaflow, PlanError
from deltaflow.replication.changes import (
    Change,
    RowDelete,
    RowInsert,
    RowUpdate,
    TableTruncate,
    Transaction,
)
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Column, Message, Relation
from deltaflow.runtime import Engine
from deltaflow.sinks import MemorySink, NegativeMultiplicityError
from deltaflow.state import TableSchema

T = Relation(
    1,
    "public",
    "t",
    "d",
    (Column(True, "id", 20, -1), Column(False, "a", 20, -1), Column(False, "bio", 25, -1)),
)
OTHER = Relation(2, "public", "other", "d", (Column(True, "id", 20, -1),))
T_SCHEMA = TableSchema("public.t", ("id", "a", "bio"), ("id",))
CONSISTENT_POINT = Lsn(0x1000)


def txn(n: int, *changes: Change) -> Transaction:
    """The n-th transaction after the consistent point."""
    commit = Lsn(CONSISTENT_POINT + 0x100 * n)
    return Transaction(
        n, commit, Lsn(commit + 0x10), datetime(2026, 1, 1, tzinfo=UTC), None, changes
    )


def a_positive(r: Row) -> bool:
    return r["a"] is not None and r["a"] > 0


def project_a(r: Row) -> Mapping[str, Any]:
    return {"a": r["a"]}


VIEWS: dict[
    str, tuple[Callable[[Deltaflow, MemorySink], object], Callable[[list[Row]], Multiset[Row]]]
] = {
    "all": (lambda df, s: df.view("all").source("public.t").sink(s), Multiset.of),
    "positive": (
        lambda df, s: df.view("positive").source("public.t").filter(a_positive).sink(s),
        lambda rows: Multiset.of(r for r in rows if a_positive(r)),
    ),
    "a_values": (
        lambda df, s: df.view("a_values").source("public.t").map(project_a).sink(s),
        lambda rows: Multiset.of(Row(project_a(r)) for r in rows),
    ),
    "positive_bio": (
        lambda df, s: (
            df.view("positive_bio")
            .source("public.t")
            .filter(a_positive)
            .select("id", "bio")
            .sink(s)
        ),
        lambda rows: Multiset.of(
            Row({"id": r["id"], "bio": r["bio"]}) for r in rows if a_positive(r)
        ),
    ),
}


def build(initial: list[dict[str, Any]]) -> tuple[Engine, dict[str, MemorySink]]:
    df = Deltaflow()
    sinks = {name: MemorySink() for name in VIEWS}
    for name, (register, _) in VIEWS.items():
        register(df, sinks[name])
    engine = Engine(df)
    engine.install(T_SCHEMA, [Row(r) for r in initial])
    engine.start(CONSISTENT_POINT)
    return engine, sinks


def check(
    sinks: dict[str, MemorySink], model: dict[int, dict[str, Any]], frontier: Frontier
) -> None:
    rows = [Row(r) for r in model.values()]
    for name, (_, expected) in VIEWS.items():
        assert sinks[name].frontier == frontier, name
        assert sinks[name].result == expected(rows), name


def test_snapshot_is_visible_once_the_engine_starts() -> None:
    df = Deltaflow()
    sink = MemorySink()
    df.view("positive").source("public.t").filter(a_positive).sink(sink)
    engine = Engine(df)
    engine.install(
        T_SCHEMA, [Row({"id": 1, "a": 5, "bio": "x"}), Row({"id": 2, "a": -1, "bio": None})]
    )
    assert sink.result == Multiset()  # the snapshot's time is not complete yet
    engine.start(CONSISTENT_POINT)
    assert engine.frontier == Frontier.at(CONSISTENT_POINT)
    assert sink.result == Multiset.of([Row({"id": 1, "a": 5, "bio": "x"})])


ops = st.lists(
    st.tuples(
        st.sampled_from(
            ["insert", "update", "update_toast", "rekey", "delete", "noise", "truncate"]
        ),
        st.integers(0, 50),
        st.none() | st.integers(-2, 2),
    ),
    max_size=8,
)


@given(st.lists(ops, max_size=25))
def test_views_match_the_model_after_every_transaction(
    script: list[list[tuple[str, int, int | None]]],
) -> None:
    model = {i: {"id": i, "a": i - 2, "bio": f"bio{i}"} for i in range(5)}
    engine, sinks = build(list(model.values()))
    check(sinks, model, Frontier.at(CONSISTENT_POINT))
    next_id = 100
    for n, ops_ in enumerate(script, start=1):
        changes: list[Change] = []
        for kind, pick, a in ops_:
            ids = list(model)
            if kind == "noise":
                changes.append(RowInsert(OTHER, Lsn(0), {"id": pick}))  # a table no view reads
                changes.append(Message(True, Lsn(0), "x", b""))
                continue
            if kind == "truncate":
                model.clear()
                changes.append(TableTruncate((T,), Lsn(0), False, False))
                continue
            if kind == "insert" or not ids:
                model[next_id] = {"id": next_id, "a": a, "bio": None if a is None else f"b{a}"}
                changes.append(RowInsert(T, Lsn(0), dict(model[next_id])))
                next_id += 1
                continue
            id_ = ids[pick % len(ids)]
            if kind == "delete":
                del model[id_]
                changes.append(RowDelete(T, Lsn(0), "key", {"id": id_}))
            elif kind == "rekey":
                model[next_id] = {**model.pop(id_), "id": next_id}
                changes.append(RowUpdate(T, Lsn(0), "key", {"id": id_}, dict(model[next_id])))
                next_id += 1
            else:
                model[id_]["a"] = a
                new = dict(model[id_])
                if kind == "update_toast":
                    new["bio"] = UNCHANGED_TOAST
                changes.append(RowUpdate(T, Lsn(0), None, None, new))
        t = txn(n, *changes)
        engine.apply(t)
        check(sinks, model, Frontier.at(t.end_lsn))


def test_a_transaction_behind_the_frontier_is_rejected() -> None:
    engine, _ = build([])
    engine.apply(txn(2))
    with pytest.raises(FrontierViolationError, match="behind the frontier"):
        engine.apply(txn(1, RowInsert(T, Lsn(0), {"id": 1, "a": 1, "bio": None})))


def test_apply_before_snapshot_is_rejected() -> None:
    df = Deltaflow()
    df.view("v").source("public.t").sink(MemorySink())
    engine = Engine(df)
    with pytest.raises(RuntimeError, match=r"snapshot of public\.t not installed"):
        engine.start(CONSISTENT_POINT)
    with pytest.raises(RuntimeError, match="not started"):
        engine.apply(txn(1))


def test_installing_an_unknown_table_is_rejected() -> None:
    df = Deltaflow()
    df.view("v").source("public.t").sink(MemorySink())
    with pytest.raises(PlanError, match=r"no view reads public.other"):
        Engine(df).install(TableSchema("public.other", ("id",), ("id",)), [])


def test_the_engine_freezes_its_registry() -> None:
    df = Deltaflow()
    df.view("v").source("public.t").sink(MemorySink())
    Engine(df)
    with pytest.raises(PlanError, match="frozen"):
        df.view("w")


def test_a_non_deterministic_map_is_caught_not_published() -> None:
    calls = iter(range(10**6))

    def tag(r: Row) -> Mapping[str, Any]:
        return {"id": r["id"], "call": next(calls)}  # different output every time

    df = Deltaflow()
    df.view("v").source("public.t").map(tag).sink(MemorySink())
    engine = Engine(df)
    engine.install(T_SCHEMA, [Row({"id": 1, "a": 1, "bio": None})])
    engine.start(CONSISTENT_POINT)
    with pytest.raises(NegativeMultiplicityError):
        engine.apply(txn(1, RowUpdate(T, Lsn(0), None, None, {"id": 1, "a": 2, "bio": None})))
