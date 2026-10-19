"""Avg reproduces Postgres's avg exactly, display scale included.

Postgres computes avg as numeric_div(sum, count). The result scale comes
from select_div_scale: at least 16 significant digits, no fewer decimals
than the sum has, rounded half away from zero. Spot checks cannot cover
that rule, so this asks Postgres for random inputs and compares both the
value and its scale."""

from __future__ import annotations

import decimal
from collections.abc import Iterator
from decimal import Decimal

import psycopg
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from deltaflow.operators.aggregate import numeric_avg, scale_of

numerics = st.integers(0, 24).flatmap(
    lambda places: st.decimals(
        min_value=Decimal(-(10**22)), max_value=Decimal(10**22),
        places=places, allow_nan=False, allow_infinity=False,
    )
)  # fmt: skip


@pytest.fixture(scope="module")
def conn(dsn: str) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    with psycopg.connect(dsn, autocommit=True) as c:
        yield c


def postgres_avg(
    conn: psycopg.Connection[tuple[object, ...]], values: list[object], cast: str
) -> Decimal:
    row = conn.execute(f"SELECT avg(x) FROM unnest(%s::{cast}[]) AS x", (values,)).fetchone()
    assert row is not None
    assert isinstance(row[0], Decimal)
    return row[0]


def same(ours: Decimal, theirs: Decimal) -> bool:
    """Equal, and printed alike: the same number of decimals."""
    return ours == theirs and ours.as_tuple().exponent == theirs.as_tuple().exponent


@settings(max_examples=400)
@given(st.lists(numerics, min_size=1, max_size=10))
def test_avg_of_numerics(
    conn: psycopg.Connection[tuple[object, ...]], values: list[Decimal]
) -> None:
    theirs = postgres_avg(conn, list(values), "numeric")
    with decimal.localcontext(prec=1000):  # the default 28 digits would round the sum
        total = sum(values, Decimal(0))
    ours = numeric_avg(total, len(values), max(map(scale_of, values)))
    assert same(ours, theirs), (values, ours, theirs)


@settings(max_examples=200)
@given(st.lists(st.integers(-(10**18), 10**18), min_size=1, max_size=10))
def test_avg_of_integers(conn: psycopg.Connection[tuple[object, ...]], values: list[int]) -> None:
    theirs = postgres_avg(conn, list(values), "bigint")
    ours = numeric_avg(sum(values), len(values), 0)
    assert same(ours, theirs), (values, ours, theirs)
