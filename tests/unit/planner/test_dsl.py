"""View registration: everything that can be rejected without a database is
rejected when the view is declared, not when the first change arrives."""

from __future__ import annotations

import pytest

from deltaflow.core import Row
from deltaflow.planner import (
    Count,
    Deltaflow,
    DistinctStep,
    GroupStep,
    JoinStep,
    MapStep,
    Max,
    PlanError,
    Sum,
)
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


def test_a_left_join_is_recorded() -> None:
    df = Deltaflow()
    view = (
        df.view("v")
        .source("public.customers")
        .join("public.orders", on=("id", "customer_id"), how="left")
        .sink(MemorySink())
    )
    (join,) = view.steps
    assert isinstance(join, JoinStep)
    assert join.how == "left"


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda b: b.join("public.c", on=("a", "b"), how="right"), "inner and left"),
        (lambda b: b.join("public.c", on=("a", "b"), how="full"), "inner and left"),
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


def test_group_by_agg_and_distinct_steps() -> None:
    df = Deltaflow()
    view = (
        df.view("orders_per_customer")
        .source("public.orders")
        .join("public.customers", on=("customer_id", "id"))
        .filter(lambda r: r["status"] != "cancelled")
        .group_by("customers.country")
        .agg(count=Count(), revenue=Sum("orders.amount"))
        .sink(MemorySink())
    )
    group = view.steps[2]
    assert isinstance(group, GroupStep)
    assert group.keys == ("customers.country",)
    assert group.aggregates == {"count": Count(), "revenue": Sum("orders.amount")}
    distinct = df.view("d").source("public.t").select("a").distinct().sink(MemorySink())
    assert isinstance(distinct.steps[-1], DistinctStep)
    total = df.view("total").source("public.t").agg(n=Count(), hi=Max("v")).sink(MemorySink())
    assert isinstance(total.steps[0], GroupStep)
    assert total.steps[0].keys == ()


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda b: b.group_by(), "at least one column"),
        (lambda b: b.group_by("a", "a"), "more than once"),
        (lambda b: b.group_by("a").agg(a=Count()), "collides with a group_by column"),
        (lambda b: b.group_by("a").agg(n="count"), "Count, Sum, Min or Max"),
        (lambda b: b.agg(), "at least one aggregate"),
        (lambda b: b.agg(n=Count()).join("public.c", on=("a", "b")), "after an aggregation"),
        (lambda b: b.distinct().join("public.c", on=("a", "b")), "after an aggregation"),
    ],
)
def test_malformed_aggregations_are_rejected(build: object, message: str) -> None:
    builder = Deltaflow().view("v").source("public.orders")
    with pytest.raises(PlanError, match=message):
        build(builder)  # type: ignore[operator]


def test_aggregations_after_aggregations_are_allowed() -> None:
    # e.g. how many countries have each order count
    df = Deltaflow()
    view = (
        df.view("v")
        .source("public.orders")
        .group_by("country")
        .agg(n=Count())
        .group_by("n")
        .agg(countries=Count())
        .sink(MemorySink())
    )
    assert len(view.steps) == 2
