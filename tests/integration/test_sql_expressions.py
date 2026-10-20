"""SQL expressions in views evaluate exactly as Postgres evaluates them.

Hypothesis builds random, type-correct expressions over a row of nullable
columns (two bigints, two numerics of mixed scales, two texts, a boolean)
and random rows. Each expression is evaluated by Postgres, in
`SELECT <expr> FROM (VALUES (...), (...)) AS v(i, j, n, m, t, u, b)`, and by the
compiled Python function. The results must be equal, and equal in form: the
same numeric scale, NULL where Postgres gives NULL, and an error where
Postgres raises one (division by zero, numeric overflow)."""

from __future__ import annotations

import decimal
import os
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from hypothesis import HealthCheck, event, given, reject, settings
from hypothesis import strategies as st

from deltaflow.core import Row
from deltaflow.planner.sqlexpr import EvalError, expression

COLUMNS = ("i", "j", "n", "m", "t", "u", "b")
CASTS = ("bigint", "bigint", "numeric", "numeric", "text", "text", "boolean")
#: the columns' static kinds, as a view's compiler gets them from the catalog
KINDS = {
    "i": "int",
    "j": "int",
    "n": "numeric",
    "m": "numeric",
    "t": "text",
    "u": "text",
    "b": "bool",
}

type Sql = psycopg.Connection[tuple[Any, ...]]

# --- rows ---------------------------------------------------------------------

ints = st.one_of(st.none(), st.integers(-10_000, 10_000))
nums = st.one_of(
    st.none(),
    st.integers(0, 6).flatmap(
        lambda p: st.decimals(min_value=-10_000, max_value=10_000, places=p,
                              allow_nan=False, allow_infinity=False)
    ),
)  # fmt: skip
texts = st.one_of(st.none(), st.text(alphabet="abAB_%x 1", max_size=4))
rows = st.tuples(ints, ints, nums, nums, texts, texts, st.one_of(st.none(), st.booleans()))

# --- expressions, by type -------------------------------------------------------

int_lit = st.integers(-50, 50).map(lambda v: f"({v})")
num_lit = (
    st.integers(0, 4)
    .flatmap(lambda p: st.decimals(min_value=-100, max_value=100, places=p, allow_nan=False))
    .map(lambda d: f"({d:f})")
)
text_lit = st.text(alphabet="abAB_%x", max_size=3).map(lambda s: "'" + s + "'")


def ints_of(children: st.SearchStrategy[str]) -> st.SearchStrategy[str]:
    return st.one_of(
        st.tuples(children, st.sampled_from(["+", "-", "*", "/", "%"]), children).map(
            lambda t: f"({t[0]} {t[1]} {t[2]})"
        ),
        children.map(lambda e: f"(- {e})"),
        children.map(lambda e: f"abs({e})"),
        st.tuples(children, children).map(lambda t: f"COALESCE({t[0]}, {t[1]})"),
        st.tuples(children, children).map(lambda t: f"NULLIF({t[0]}, {t[1]})"),
        st.tuples(children, children).map(lambda t: f"GREATEST({t[0]}, {t[1]})"),
        st.tuples(children, children).map(lambda t: f"LEAST({t[0]}, {t[1]})"),
    )


int_expr = st.recursive(
    st.one_of(st.sampled_from(["i", "j"]), int_lit, st.sampled_from(["length(t)", "length(u)"])),
    ints_of, max_leaves=4,
)  # fmt: skip


def nums_of(children: st.SearchStrategy[str]) -> st.SearchStrategy[str]:
    return st.one_of(
        st.tuples(children, st.sampled_from(["+", "-", "*", "/"]), children).map(
            lambda t: f"({t[0]} {t[1]} {t[2]})"
        ),
        st.tuples(children, st.sampled_from(["+", "-", "*", "/"]), int_expr).map(
            lambda t: f"({t[0]} {t[1]} {t[2]})"
        ),
        children.map(lambda e: f"abs({e})"),
        st.tuples(children, st.integers(-1, 3)).map(lambda t: f"round({t[0]}, {t[1]})"),
        children.map(lambda e: f"round({e})"),
        st.tuples(children, children).map(lambda t: f"COALESCE({t[0]}, {t[1]})"),
        st.tuples(children, children).map(lambda t: f"GREATEST({t[0]}, {t[1]})"),
        int_expr.map(lambda e: f"CAST({e} AS numeric)"),
        children.map(lambda e: f"CAST({e} AS numeric(12, 2))"),
    )


num_expr = st.recursive(st.one_of(st.sampled_from(["n", "m"]), num_lit), nums_of, max_leaves=4)

text_expr = st.recursive(
    st.one_of(st.sampled_from(["t", "u"]), text_lit),
    lambda c: st.one_of(
        st.tuples(c, c).map(lambda t: f"({t[0]} || {t[1]})"),
        st.tuples(c, c).map(lambda t: f"COALESCE({t[0]}, {t[1]})"),
        st.tuples(c, c).map(lambda t: f"NULLIF({t[0]}, {t[1]})"),
        int_expr.map(lambda e: f"CAST({e} AS text)"),
    ),
    max_leaves=3,
)


def comparison(
    a: st.SearchStrategy[str], ordered: bool, collate: str = ""
) -> st.SearchStrategy[str]:
    ops = ["=", "<>", "<", "<=", ">", ">="] if ordered else ["=", "<>"]
    return st.tuples(a, st.sampled_from(ops), a).map(lambda t: f"({t[0]}{collate} {t[1]} {t[2]})")


in_item = st.one_of(int_lit, st.just("NULL"), int_expr)

bool_leaf = st.one_of(
    st.just("b"),
    st.sampled_from(["TRUE", "FALSE", "CAST(NULL AS boolean)"]),
    comparison(int_expr, ordered=True),
    comparison(num_expr, ordered=True),
    st.tuples(num_expr, int_expr).map(lambda t: f"({t[0]} < {t[1]})"),
    comparison(text_expr, ordered=False),
    comparison(text_expr, ordered=True, collate=' COLLATE "C"'),
    # items with and without columns: Postgres parses the two differently
    st.tuples(int_expr, st.lists(in_item, min_size=1, max_size=4)).map(
        lambda t: f"({t[0]} IN ({', '.join(t[1])}))"
    ),
    st.tuples(int_expr, st.lists(in_item, min_size=1, max_size=4)).map(
        lambda t: f"({t[0]} NOT IN ({', '.join(t[1])}))"
    ),
    st.tuples(num_expr, num_lit, num_lit).map(lambda t: f"({t[0]} BETWEEN {t[1]} AND {t[2]})"),
    st.tuples(int_expr, int_expr).map(lambda t: f"({t[0]} IS DISTINCT FROM {t[1]})"),
    st.tuples(text_expr, st.text(alphabet="ab%_\\", max_size=4)).map(
        lambda t: f"({t[0]} LIKE '{t[1]}')"
    ),
    st.tuples(
        st.sampled_from(["i", "n", "t", "b"]), st.sampled_from(["IS NULL", "IS NOT NULL"])
    ).map(lambda t: f"({t[0]} {t[1]})"),
)
bool_expr = st.recursive(
    bool_leaf,
    lambda c: st.one_of(
        st.tuples(c, st.sampled_from(["AND", "OR"]), c).map(lambda t: f"({t[0]} {t[1]} {t[2]})"),
        c.map(lambda e: f"(NOT {e})"),
        st.tuples(c, st.sampled_from(["IS TRUE", "IS NOT TRUE", "IS FALSE", "IS UNKNOWN"])).map(
            lambda t: f"({t[0]} {t[1]})"
        ),
    ),
    max_leaves=4,
)
case_expr = st.tuples(bool_expr, int_expr, int_expr).map(
    lambda t: f"CASE WHEN {t[0]} THEN {t[1]} ELSE {t[2]} END"
) | st.tuples(int_expr, int_lit, text_expr).map(
    lambda t: f"CASE {t[0]} WHEN {t[1]} THEN {t[2]} END"
)
expressions = st.one_of(int_expr, num_expr, text_expr, bool_expr, case_expr)

# --- the comparison ---------------------------------------------------------------


@pytest.fixture(scope="module")
def conn(dsn: str) -> Iterator[Sql]:
    with psycopg.connect(dsn, autocommit=True) as c:
        yield c


def postgres(conn: Sql, sql: str, row: tuple[Any, ...]) -> tuple[str, Any]:
    values = ", ".join(f"%s::{cast}" for cast in CASTS)
    # Two rows, read one: Postgres pulls a single-row VALUES up into the
    # query, its values become constants, and the whole expression folds
    # with them. A view's columns come from rows, never constants. % in the
    # expression (modulo, LIKE) is not a placeholder.
    query = (
        f"SELECT {sql.replace('%', '%%')} FROM (VALUES ({values}), ({values}))"
        f" AS v({', '.join(COLUMNS)}) LIMIT 1"
    )
    try:
        result = conn.execute(query, row + row).fetchone()
    except psycopg.DataError as exc:  # division by zero, out of range, ...
        return "error", type(exc).__name__
    except psycopg.errors.UndefinedFunction:
        # Ill-typed (round(round(1), 0) has no round(float8, int)): a view
        # never gets this far, since Postgres checks every view first.
        reject()
    assert result is not None
    return "value", result[0]


def ours(sql: str, row: tuple[Any, ...]) -> tuple[str, Any]:
    try:  # compiling folds constants, and raises where Postgres's planner would
        fn = expression(sql, KINDS)
        return "value", fn(Row(dict(zip(COLUMNS, row, strict=True))))
    except EvalError as exc:
        return "error", str(exc)


def same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is b
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(b, Decimal):
        mine = a if isinstance(a, Decimal) else Decimal(a)
        return mine == b and mine.as_tuple().exponent == b.as_tuple().exponent
    return type(a) is type(b) and a == b


# DELTAFLOW_SQL_EXAMPLES=20000 for a longer soak
@settings(
    max_examples=int(os.environ.get("DELTAFLOW_SQL_EXAMPLES", "1500")),
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
)
@given(expressions, rows)
def test_expressions_evaluate_as_postgres_evaluates_them(
    conn: Sql, sql: str, row: tuple[Any, ...]
) -> None:
    theirs = postgres(conn, sql, row)
    event(f"postgres: {theirs[0]}{' NULL' if theirs == ('value', None) else ''}")
    with decimal.localcontext(prec=28):  # ours must not depend on the default context
        mine = ours(sql, row)
    if theirs[0] == "error":
        assert mine[0] == "error", (sql, row, theirs, mine)
    else:
        assert mine[0] == "value", (sql, row, theirs, mine)
        assert same(mine[1], theirs[1]), (sql, row, theirs, mine)
