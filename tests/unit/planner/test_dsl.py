"""View registration: everything that can be rejected without a database is
rejected when the view is declared, not when the first change arrives."""

from __future__ import annotations

import pytest

from deltaflow.core import Row
from deltaflow.planner import Deltaflow, MapStep, PlanError
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
