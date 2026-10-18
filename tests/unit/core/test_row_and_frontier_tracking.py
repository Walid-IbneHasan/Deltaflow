from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Frontier, FrontierRegressionError, MutableFrontier, Row
from deltaflow.replication.lsn import Lsn

values = st.one_of(st.none(), st.integers(), st.text(max_size=5), st.booleans())
mappings = st.dictionaries(st.sampled_from("abcde"), values)


@given(mappings, st.randoms(use_true_random=False))
def test_row_equality_and_hash_ignore_column_order(m: dict[str, object], rnd: object) -> None:
    items = list(m.items())
    rnd.shuffle(items)  # type: ignore[attr-defined]
    a, b = Row(m), Row(items)
    assert a == b
    assert hash(a) == hash(b)
    assert a == m  # compares equal to a plain mapping with the same contents


def test_row_is_an_immutable_mapping() -> None:
    r = Row({"id": 1, "amount": Decimal("1.50")})
    assert r["id"] == 1
    assert list(r) == ["id", "amount"]
    assert len(r) == 2
    assert r.get("missing") is None
    with pytest.raises(TypeError):
        r["id"] = 2  # type: ignore[index]
    assert repr(r) == "Row({'id': 1, 'amount': Decimal('1.50')})"


def test_rows_with_different_values_differ() -> None:
    assert Row({"a": 1}) != Row({"a": 2})
    assert Row({"a": 1}) != Row({"a": 1, "b": None})
    assert Row({"a": None}) != Row({})


def test_unhashable_value_is_rejected_at_construction() -> None:
    with pytest.raises(TypeError, match="column 'tags'"):
        Row({"id": 1, "tags": ["x"]})


def test_mutable_frontier_advances_monotonically() -> None:
    f = MutableFrontier(Frontier.at(Lsn(5)))
    f.advance(Frontier.at(Lsn(5)))  # staying put is allowed
    f.advance(Frontier.at(Lsn(9)))
    assert f.current == Frontier.at(Lsn(9))
    with pytest.raises(FrontierRegressionError, match="0/9 to 0/8"):
        f.advance(Frontier.at(Lsn(8)))
    f.advance(Frontier.CLOSED)
    assert f.current == Frontier.CLOSED
    with pytest.raises(FrontierRegressionError):
        f.advance(Frontier.at(Lsn(100)))
