"""Key encoding for the state store.

A retraction finds its index entry by key bytes, so the encoding must be
*canonical*: equal values (by Python ==, which is what the multisets use)
must encode to equal bytes, and unequal values to unequal bytes. Keys are
also concatenated, so encodings must be prefix-free.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Row
from deltaflow.state.codec import decode_row, encode_key, encode_ordered, encode_row, encode_row_key

numbers = st.one_of(
    st.integers(-(10**30), 10**30),
    st.decimals(allow_nan=False, places=None, min_value=-(10**6), max_value=10**6),
    st.floats(allow_nan=False),
    st.booleans(),
)
instants = st.datetimes(
    min_value=datetime(1990, 1, 1), max_value=datetime(2100, 1, 1), timezones=st.timezones()
)
scalars = st.one_of(
    st.none(),
    numbers,
    st.text(max_size=8),
    st.binary(max_size=8),
    instants,
    st.datetimes(),
    st.dates(),
    st.times(),
    st.timedeltas(),
    st.uuids(),
)


@given(scalars, scalars)
def test_encoding_is_canonical_for_python_equality(a: Any, b: Any) -> None:
    assert (encode_key((a,)) == encode_key((b,))) == (a == b)


@given(numbers)
def test_equal_numbers_of_different_types_share_an_encoding(x: Any) -> None:
    for other in (Decimal(x) if not isinstance(x, Decimal) else x, x * 1):
        if other == x:
            assert encode_key((other,)) == encode_key((x,))


def test_examples_that_would_break_a_naive_encoding() -> None:
    same = [
        (Decimal("1.0"), Decimal("1.00"), 1, 1.0, True),
        (Decimal("1E+2"), 100),
        (0, -0.0, Decimal("-0")),
        (
            datetime(2026, 1, 1, 12, tzinfo=UTC),
            datetime(2026, 1, 1, 14, tzinfo=timezone(timedelta(hours=2))),
        ),
    ]
    for group in same:
        assert len({encode_key((v,)) for v in group}) == 1, group
    different = [("1", 1), (b"a", "a"), (date(2026, 1, 1), datetime(2026, 1, 1)), (None, 0)]
    for a, b in different:
        assert encode_key((a,)) != encode_key((b,))


@given(instants, st.timezones())
def test_the_same_instant_in_any_zone_shares_an_encoding(instant: datetime, zone: Any) -> None:
    # The snapshot reads timestamptz in the session's zone, the stream in UTC.
    assert encode_key((instant.astimezone(zone),)) == encode_key((instant,))


@given(st.decimals(allow_nan=False, allow_infinity=False), st.integers(0, 40))
def test_trailing_zeros_do_not_matter(d: Decimal, zeros: int) -> None:
    # Beyond the default 28-digit context too, where normalize() would round.
    padded = Decimal(f"{d:f}{'' if '.' in f'{d:f}' else '.'}{'0' * zeros}")
    assert padded == d
    assert encode_key((padded,)) == encode_key((d,))
    assert encode_key((Decimal("1." + "0" * 40 + "1"),)) != encode_key((Decimal(1),))


@given(st.lists(scalars, max_size=4), st.lists(scalars, max_size=4))
def test_tuple_encodings_are_prefix_free(a: list[Any], b: list[Any]) -> None:
    ea, eb = encode_key(tuple(a)), encode_key(tuple(b))
    if len(a) == len(b) and ea != eb:
        assert not eb.startswith(ea)
        assert not ea.startswith(eb)


rows = st.dictionaries(st.sampled_from(["a", "b", "c", "x.y"]), scalars, max_size=4).map(Row)


@given(rows, rows)
def test_row_keys_are_canonical(a: Row, b: Row) -> None:
    assert (encode_row_key(a) == encode_row_key(b)) == (a == b)


@given(rows)
def test_rows_round_trip_exactly(r: Row) -> None:
    back = decode_row(encode_row(r))
    assert back == r
    assert list(back) == list(r)  # column order kept
    for k in r:
        assert type(back[k]) is type(r[k])
        assert repr(back[k]) == repr(r[k])  # Decimal scale, tzinfo survive


@pytest.mark.parametrize(
    "value",
    [float("nan"), Decimal("NaN"), time(12, tzinfo=UTC), [1], {"a": 1}, object()],
)
def test_values_without_a_canonical_encoding_are_rejected(value: object) -> None:
    with pytest.raises(TypeError, match="cannot be used as a key"):
        encode_key((value,))


def test_uuid_and_bytes_like() -> None:
    u = uuid.uuid4()
    assert encode_key((u,)) == encode_key((uuid.UUID(str(u)),))
    assert encode_key((memoryview(b"ab"),)) == encode_key((b"ab",))


# --- order-preserving encoding (min/max state) -------------------------------

ordered_numbers = st.one_of(
    st.integers(-(10**30), 10**30),
    st.decimals(allow_nan=False, places=None, min_value=-(10**8), max_value=10**8),
    st.floats(allow_nan=False),
)
same_type_pairs = st.one_of(
    st.tuples(ordered_numbers, ordered_numbers),
    st.tuples(st.text(max_size=6), st.text(max_size=6)),
    st.tuples(st.binary(max_size=6), st.binary(max_size=6)),
    st.tuples(instants, instants),
    st.tuples(st.datetimes(), st.datetimes()),
    st.tuples(st.dates(), st.dates()),
    st.tuples(st.times(), st.times()),
    st.tuples(st.timedeltas(), st.timedeltas()),
    st.tuples(st.uuids(), st.uuids()),
)


@given(same_type_pairs)
def test_ordered_encoding_preserves_order_and_equality(pair: tuple[Any, Any]) -> None:
    a, b = pair
    ea, eb = encode_ordered(a), encode_ordered(b)
    assert (ea < eb) == (a < b)
    assert (ea == eb) == (a == b)


@given(st.lists(ordered_numbers, min_size=1, max_size=20))
def test_sorting_by_encoding_sorts_numbers(xs: list[Any]) -> None:
    by_bytes = sorted(xs, key=encode_ordered)
    assert [Decimal(x) for x in by_bytes] == sorted(Decimal(x) for x in xs)


def test_ordered_encoding_examples() -> None:
    xs = [
        Decimal("-Infinity"), Decimal("-100"), Decimal("-1.23"), Decimal("-1.2"), -1,
        Decimal("-0.001"), 0, Decimal("0.001"), 1, Decimal("1.2"), Decimal("1.23"), 100,
        Decimal("Infinity"),
    ]  # fmt: skip
    assert sorted(xs, key=encode_ordered) == xs
    assert encode_ordered(Decimal("1.50")) == encode_ordered(1.5) == encode_ordered(Decimal("1.5"))
    # Code-point order, which is what Postgres's "C" collation gives for UTF-8.
    assert sorted(["b", "B", "a", "é", "z"], key=encode_ordered) == ["B", "a", "b", "z", "é"]
    with pytest.raises(TypeError):
        encode_ordered(float("nan"))
    with pytest.raises(TypeError):
        encode_ordered(None)
