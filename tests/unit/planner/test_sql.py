"""SQL views translated into the plan, without a database: the shapes they
become, what is refused, and what the fingerprint covers. Whether their
results are Postgres's is tests/differential/test_sql_views.py."""

from __future__ import annotations

import pytest

from deltaflow.operators import Avg, Count, CountDistinct, Sum
from deltaflow.planner import Deltaflow, DistinctStep, FilterStep, GroupStep, JoinStep, MapStep
from deltaflow.planner.sqlexpr import SqlError
from deltaflow.runtime.engine import fingerprint
from deltaflow.sinks import MemorySink, PostgresSink


def test_create_view_names_the_view_and_its_sink() -> None:
    df = Deltaflow()
    view = df.sql(
        "CREATE VIEW analytics.by_status AS"
        " SELECT status, count(*) AS n FROM orders GROUP BY status"
    )
    assert view.name == "by_status"
    assert isinstance(view.sink, PostgresSink)
    assert view.sink.table == "analytics.by_status"
    assert view.source == "public.orders"  # unqualified tables are public
    (group,) = view.steps
    assert isinstance(group, GroupStep)
    assert group.keys == ("status",)
    assert group.aggregates == {"n": Count()}


def test_a_join_becomes_join_steps_with_keys_from_on() -> None:
    df = Deltaflow()
    view = df.sql(
        "SELECT c.country, sum(o.amount) AS revenue FROM orders o"
        " JOIN customers c ON c.id = o.customer_id"
        " LEFT JOIN public.regions r ON r.code = c.country GROUP BY c.country",
        name="v", sink=MemorySink(),
    )  # fmt: skip
    first, second, group = view.steps
    assert isinstance(first, JoinStep)
    assert (first.table, first.left_on, first.right_on, first.how) == (
        "public.customers", ("customer_id",), ("id",), "inner",
    )  # fmt: skip
    assert isinstance(second, JoinStep)
    # after a join, left rows are qualified by their table's name
    assert (second.left_on, second.right_on, second.how) == (
        ("customers.country",),
        ("code",),
        "left",
    )
    assert isinstance(group, GroupStep)
    assert group.output_keys == ("country",)
    assert group.aggregates == {"revenue": Sum("orders.amount")}


def test_expressions_are_computed_before_and_after_grouping() -> None:
    df = Deltaflow()
    view = df.sql(
        "SELECT length(name) % 5 AS bucket, avg(amount * 2) AS mean, sum(amount) / count(*) AS per"
        " FROM orders WHERE status <> 'cancelled' GROUP BY length(name) % 5 HAVING count(*) > 1",
        name="v", sink=MemorySink(),
    )  # fmt: skip
    kinds = [type(s).__name__ for s in view.steps]
    assert kinds == ["FilterStep", "MapStep", "GroupStep", "FilterStep", "MapStep"]
    group = view.steps[2]
    assert isinstance(group, GroupStep)
    assert any(isinstance(a, Avg) for a in group.aggregates.values())


def test_count_distinct_and_select_distinct() -> None:
    df = Deltaflow()
    a = df.sql("SELECT status, count(DISTINCT customer_id) AS c FROM orders GROUP BY status",
               name="a", sink=MemorySink())  # fmt: skip
    (group,) = a.steps
    assert isinstance(group, GroupStep)
    assert group.aggregates == {"c": CountDistinct("customer_id")}
    b = df.sql("SELECT DISTINCT country FROM customers", name="b", sink=MemorySink())
    assert [type(s) for s in b.steps] == [MapStep, DistinctStep]


def test_where_is_a_filter_and_inner_on_conditions_move_there() -> None:
    df = Deltaflow()
    view = df.sql(
        "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id AND c.country = 'NL'"
        " WHERE o.amount > 10",
        name="v", sink=MemorySink(),
    )  # fmt: skip
    assert [type(s) for s in view.steps] == [JoinStep, FilterStep, FilterStep, MapStep]


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("SELECT a FROM t UNION SELECT a FROM u", "UNION"),
        ("WITH x AS (SELECT 1) SELECT * FROM x", "WITH clause"),
        ("SELECT a FROM t ORDER BY a", "ORDER BY"),
        ("SELECT a FROM t LIMIT 3", "LIMIT"),
        ("SELECT DISTINCT ON (a) a, b FROM t", "DISTINCT ON"),
        ("SELECT a, count(*) AS n FROM t GROUP BY ROLLUP (a)", "GROUPING SETS"),
        ("SELECT t.a FROM t RIGHT JOIN u ON u.a = t.a", "RIGHT joins"),
        ("SELECT t.a FROM t FULL JOIN u ON u.a = t.a", "FULL joins"),
        ("SELECT a FROM t JOIN u USING (a)", "USING"),
        ("SELECT a FROM t, u", "comma list"),
        ("SELECT a FROM (SELECT 1 AS a) s", "subqueries"),
        ("SELECT t.a FROM t LEFT JOIN u ON u.a = t.a AND u.b > 1", "with a left join"),
        ("SELECT t.a FROM t JOIN u ON t.a > u.a", "needs an ON equality"),
        ("SELECT a + 1 FROM t", "give the output column"),
        ("SELECT a, a FROM t", "appear twice"),
        ("SELECT sum(DISTINCT a) AS s FROM t", r"count\(DISTINCT"),
        ("SELECT count(*) FILTER (WHERE a > 1) AS n FROM t", "FILTER"),
        ("SELECT sum(sum(a)) AS s FROM t GROUP BY b", "nested"),
        ("SELECT a, rank() OVER (ORDER BY a) AS r FROM t", "window"),
        ("SELECT b FROM t GROUP BY a", "GROUP BY clause"),
        ("SELECT a AS x, count(*) AS n FROM t GROUP BY x", "output column's name"),
        ("SELECT * FROM t JOIN u ON u.a = t.a", "SELECT \\* over a join"),
        ("SELECT now() AS t FROM t", "function now"),
        ("UPDATE t SET a = 1", "expected SELECT"),
    ],
)
def test_what_cannot_be_maintained_is_refused(sql: str, message: str) -> None:
    with pytest.raises(SqlError, match=message):
        Deltaflow().sql(sql, name="v", sink=MemorySink())


def test_a_bare_select_needs_a_name_and_sink() -> None:
    with pytest.raises(SqlError, match="name= and sink="):
        Deltaflow().sql("SELECT a FROM t")


def _fingerprint(sql: str) -> str:
    df = Deltaflow()
    df.sql(sql, name="v", sink=PostgresSink("a.v"))
    return fingerprint(df)


def test_the_fingerprint_covers_the_sql() -> None:
    """A Python view's function bodies are invisible to the fingerprint;
    a SQL view's logic is not: changing it forces a fresh bootstrap."""
    base = _fingerprint("SELECT id FROM orders WHERE amount > 10")
    assert _fingerprint("SELECT id FROM orders WHERE amount > 10") == base
    assert _fingerprint("SELECT id FROM orders WHERE amount > 11") != base
    assert _fingerprint("SELECT id FROM orders WHERE amount >= 10") != base
