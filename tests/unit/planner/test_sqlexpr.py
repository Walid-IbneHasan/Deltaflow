"""SQL expressions without a database: what is refused, and the semantics
that differ from Python's. tests/integration/test_sql_expressions.py
compares thousands of random expressions with Postgres itself."""

from __future__ import annotations

from decimal import Decimal

import pytest

from deltaflow.core import Row
from deltaflow.planner.sqlexpr import EvalError, SqlError, expression

ROW = Row({"i": 7, "j": -2, "n": Decimal("1.50"), "t": "ab", "u": None, "ts": None})


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("(SELECT 1)", "subquery"),
        ("now()", "function now"),
        ("CURRENT_DATE", "current time or session"),
        ("t ILIKE 'a%'", "ILIKE"),
        ("t LIKE 'a!%' ESCAPE '!'", "ESCAPE"),
        ("ARRAY[1, 2]", "array"),
        ("t COLLATE \"en_US\" < 'b'", "C collation"),
        ("count(*)", "aggregate"),
        ("sum(i) OVER ()", "aggregate or window"),
        ("t ~ 'a'", "operator ~"),
        ("CAST(i AS int[])", "array types"),
        ("CAST(t AS varchar(3))", "a cast to varchar"),
        ("CAST(i AS money)", "a cast to money"),
    ],
)
def test_unsupported_sql_is_refused_at_registration(sql: str, message: str) -> None:
    with pytest.raises(SqlError, match=message):
        expression(sql)


@pytest.mark.parametrize(
    ("sql", "value"),
    [
        ("i / j", -3),  # truncation toward zero, not floor
        ("i % j", 1),  # the sign of the dividend
        ("-i / j", 3),
        ("n * 2", Decimal("3.00")),  # the scales add up
        ("n / 3", Decimal("0.50000000000000000000")),  # select_div_scale
        ("round(n, 1)", Decimal("1.5")),
        ("round(2.5)", Decimal("3")),  # numeric: half away from zero
        ("round(5)", 5.0),  # round(integer) is round(double precision)
        ("CAST(2.5 AS int)", 3),
        ("u IS NULL AND NOT (u = 'x') IS NOT NULL", True),
        ("NULL::int IN (1)", None),
        ("i IN (1, NULL)", None),  # not found, and a NULL among the values
        ("i NOT IN (1, NULL)", None),
        ("'' LIKE '\\'", False),  # matching never reaches the trailing escape
        ("COALESCE(u, t)", "ab"),
        ("GREATEST(i, NULL, j)", 7),
        ("i = '7'", True),  # an untyped literal takes the column's type
        ("CASE WHEN i > 5 THEN 'big' END", "big"),
        ("t || i", "ab7"),
    ],
)
def test_postgres_semantics(sql: str, value: object) -> None:
    result = expression(sql)(ROW)
    assert result == value
    assert type(result) is type(value)
    if isinstance(value, Decimal):
        assert result.as_tuple().exponent == value.as_tuple().exponent


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("t < 'b'", 'COLLATE "C"'),  # a locale's order is not reproduced
        ("i / 0", "division by zero"),
        ("CAST(n * 10000000000 AS numeric(12, 2))", "numeric field overflow"),
        ("i * 9223372036854775807", "bigint out of range"),
        ("i = 'x'", "invalid input syntax for type integer"),
        ("t LIKE 'a\\'", "must not end with escape"),
    ],
)
def test_runtime_errors_where_postgres_raises_or_would_differ(sql: str, message: str) -> None:
    with pytest.raises(EvalError, match=message):
        expression(sql)(ROW)


def test_constant_folding_raises_as_postgres_plans() -> None:
    """Postgres folds a CASE branch whose test is not constant, so 1/0 there
    raises even if no row takes it; a branch after a constant FALSE test,
    or an AND after a constant FALSE, is never folded."""
    with pytest.raises(EvalError, match="division by zero"):
        expression("CASE WHEN i > 0 THEN 1 / 0 ELSE 1 END")
    assert expression("CASE WHEN FALSE THEN 1 / 0 ELSE 1 END")(ROW) == 1
    assert expression("FALSE AND 1 / 0 = 1")(ROW) is False
    assert expression("i + NULL / 0")(ROW) is None  # strict: NULL, before dividing


def test_a_literal_that_cannot_take_the_common_type_fails_even_unreached() -> None:
    with pytest.raises(EvalError, match="invalid input syntax for type integer"):
        expression("COALESCE(1, 'x')")


def test_common_type_promotes_every_branch() -> None:
    """COALESCE(numeric, float8) is float8, also when the numeric is not NULL."""
    result = expression("COALESCE(n, round(1))")(ROW)
    assert result == 1.5
    assert isinstance(result, float)
