"""View registration: everything that can be rejected without a database is
rejected when the view is declared, not when the first change arrives."""

from __future__ import annotations

import pytest

from deltaflow.core import Row
from deltaflow.planner import Deltaflow, JoinStep, MapStep, PlanError
from deltaflow.sinks import MemorySink


def big(r: Row) -> bool:
    return bool(r["amount"] > 100)


def test_registered_view_records_its_pipeline() -> None:
    df = Deltaflow()
    sink = MemorySink()
    view = (
        df.view("big_orders").source("public.orders").filter(big).select("id", "amount").sink(sink)
    )
    assert view.name == "big_orders"
    assert view.source == "public.orders"
    assert [type(s).__name__ for s in view.steps] == ["FilterStep", "MapStep"]
    assert view.sink is sink
    assert df.views == (view,)
    assert df.sources == ("public.orders",)


def test_sources_are_shared_and_listed_once() -> None:
    df = Deltaflow()
    df.view("a").source("public.orders").sink(MemorySink())
    df.view("b").source("public.customers").sink(MemorySink())
    df.view("c").source("public.orders").filter(big).sink(MemorySink())
    assert df.sources == ("public.orders", "public.customers")


def test_select_projects_named_columns() -> None:
    df = Deltaflow()
    view = df.view("v").source("public.t").select("b", "a").sink(MemorySink())
    [step] = view.steps
    assert isinstance(step, MapStep)
    assert step.fn(Row({"a": 1, "b": 2, "c": 3})) == {"b": 2, "a": 1}


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda df: df.view("v").filter(big), "call .source"),
        (lambda df: df.view("v").sink(MemorySink()), "call .source"),
        (lambda df: df.view("v").source("orders"), "schema-qualified"),
        (
            lambda df: df.view("v").source("public.orders").source("public.x"),
            "already has a source",
        ),
        (lambda df: df.view("v").source("public.orders").filter("amount > 5"), "must be callable"),
        (lambda df: df.view("v").source("public.orders").map(None), "must be callable"),
        (lambda df: df.view("v").source("public.orders").select(), "at least one column"),
        (lambda df: df.view("bad name"), "view name"),
        (lambda df: df.view(""), "view name"),
    ],
)
def test_malformed_views_are_rejected_at_declaration(build: object, message: str) -> None:
    with pytest.raises(PlanError, match=message):
        build(Deltaflow())  # type: ignore[operator]


def test_async_functions_are_rejected() -> None:
    # An async predicate returns a coroutine, which is truthy: every row would
    # pass. Refuse it up front.
    async def pred(r: Row) -> bool:
        return True

    with pytest.raises(PlanError, match="async"):
        Deltaflow().view("v").source("public.t").filter(pred)  # type: ignore[arg-type]


def test_duplicate_view_names_are_rejected() -> None:
    df = Deltaflow()
    df.view("v").source("public.t").sink(MemorySink())
    with pytest.raises(PlanError, match="already registered"):
        df.view("v")


def test_a_builder_is_used_once() -> None:
    df = Deltaflow()
    builder = df.view("v").source("public.t")
    builder.sink(MemorySink())
    with pytest.raises(PlanError, match="already registered"):
        builder.filter(big)


def test_a_sink_serves_one_view() -> None:
    df = Deltaflow()
    sink = MemorySink()
    df.view("a").source("public.t").sink(sink)
    with pytest.raises(PlanError, match="already the sink of view 'a'"):
        df.view("b").source("public.t").sink(sink)


def test_no_views_after_the_engine_is_built() -> None:
    df = Deltaflow()
    df.view("a").source("public.t").sink(MemorySink())
    df.freeze()
    with pytest.raises(PlanError, match="frozen"):
        df.view("b")


def test_join_records_qualified_names_and_lists_both_sources() -> None:
    df = Deltaflow()
    view = (
        df.view("v")
        .source("public.orders")
        .join("public.customers", on=("customer_id", "id"))
        .filter(big)
        .sink(MemorySink())
    )
    join = view.steps[0]
    assert isinstance(join, JoinStep)
    assert (join.table, join.left_on, join.right_on) == (
        "public.customers",
        ("customer_id",),
        ("id",),
    )
    assert (join.left_prefix, join.right_prefix) == ("orders", "customers")
    assert df.sources == ("public.orders", "public.customers")


def test_chained_joins_keep_left_rows_qualified() -> None:
    df = Deltaflow()
    view = (
        df.view("v")
        .source("public.order_items")
        .join("public.orders", on=("order_id", "id"))
        .join("public.customers", on=[("orders.customer_id", "id")])
        .sink(MemorySink())
    )
    first, second = view.steps
    assert isinstance(first, JoinStep)
    assert isinstance(second, JoinStep)
    assert first.left_prefix == "order_items"
    assert second.left_prefix is None  # already qualified by the first join
    assert second.left_on == ("orders.customer_id",)


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda b: b.join("public.c", on=("a", "b"), how="left"), "only inner joins"),
        (lambda b: b.join("public.orders", on=("id", "id")), "same name"),
        (lambda b: b.join("other.orders", on=("id", "id")), "same name"),
        (lambda b: b.join("customers", on=("a", "b")), "schema-qualified"),
        (lambda b: b.join("public.c", on=("a",)), "pairs of column names"),
        (lambda b: b.join("public.c", on=[]), "pairs of column names"),
        (lambda b: b.join("public.c", on="a = b"), "pairs of column names"),
    ],
)
def test_malformed_joins_are_rejected(build: object, message: str) -> None:
    builder = Deltaflow().view("v").source("public.orders")
    with pytest.raises(PlanError, match=message):
        build(builder)  # type: ignore[operator]
