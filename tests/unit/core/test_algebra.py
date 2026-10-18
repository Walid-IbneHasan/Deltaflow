"""Algebra laws for deltas, multisets, consolidation and frontiers.

Rows here are small ints: the laws are about multiplicities and times, not
row contents, and small domains make collisions (the interesting case) common.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Delta, Frontier, Multiset, accumulate, consolidate
from deltaflow.replication.lsn import Lsn

rows = st.integers(0, 5)
times = st.integers(0, 6).map(Lsn)
diffs = st.integers(-3, 3)
deltas = st.builds(Delta, rows, times, diffs)
batches = st.lists(deltas, max_size=30)
multisets = st.lists(st.tuples(rows, diffs), max_size=15).map(Multiset.from_pairs)
frontiers = st.one_of(st.just(Frontier.CLOSED), times.map(Frontier.at))


def as_set[T](batch: list[Delta[T]]) -> set[tuple[T, Lsn, int]]:
    return {(d.row, d.time, d.diff) for d in batch}


def every_frontier() -> list[Frontier]:
    return [*(Frontier.at(Lsn(t)) for t in range(8)), Frontier.CLOSED]


# --- Multiset: an abelian group under +, with zero entries never stored ----


@given(multisets, multisets, multisets)
def test_multiset_addition_is_associative(
    a: Multiset[int], b: Multiset[int], c: Multiset[int]
) -> None:
    assert (a + b) + c == a + (b + c)


@given(multisets, multisets)
def test_multiset_addition_is_commutative(a: Multiset[int], b: Multiset[int]) -> None:
    assert a + b == b + a


@given(multisets)
def test_multiset_identity_and_inverse(a: Multiset[int]) -> None:
    empty: Multiset[int] = Multiset()
    assert a + empty == a
    assert a + (-a) == empty
    assert a - a == empty
    assert not (a - a)


@given(multisets)
def test_multiset_never_stores_zero(a: Multiset[int]) -> None:
    assert all(n != 0 for _, n in a.items())
    assert len(a) == sum(1 for _ in a.items())


def test_multiset_counts_and_negatives() -> None:
    m = Multiset.from_pairs([(1, 2), (2, -1), (1, -2), (3, 1), (3, 1)])
    assert m.count(1) == 0
    assert m.count(3) == 2
    assert m.count(99) == 0
    assert m.negatives() == {2: -1}
    assert m.size() == 1  # total multiplicity: 2 + (-1)
    assert Multiset.of([3, 3]) + Multiset.from_pairs([(2, -1)]) == m


# --- consolidation ----------------------------------------------------------


@given(batches)
def test_consolidation_preserves_the_accumulated_collection_at_every_frontier(
    batch: list[Delta[int]],
) -> None:
    out = consolidate(batch)
    for f in every_frontier():
        assert accumulate(out, f) == accumulate(batch, f)


@given(batches)
def test_consolidation_is_canonical(batch: list[Delta[int]]) -> None:
    out = consolidate(batch)
    assert all(d.diff != 0 for d in out)
    keys = [(d.row, d.time) for d in out]
    assert len(keys) == len(set(keys))
    assert [d.time for d in out] == sorted(d.time for d in out)


@given(batches)
def test_consolidation_is_idempotent(batch: list[Delta[int]]) -> None:
    once = consolidate(batch)
    assert consolidate(once) == once


@given(batches, st.randoms(use_true_random=False))
def test_consolidation_ignores_arrival_order(batch: list[Delta[int]], rnd: object) -> None:
    shuffled = list(batch)
    rnd.shuffle(shuffled)  # type: ignore[attr-defined]
    assert as_set(consolidate(shuffled)) == as_set(consolidate(batch))


@given(batches, batches)
def test_consolidation_can_be_done_incrementally(a: list[Delta[int]], b: list[Delta[int]]) -> None:
    assert as_set(consolidate([*consolidate(a), *b])) == as_set(consolidate([*a, *b]))


def test_update_is_retraction_plus_insertion() -> None:
    t = Lsn(5)
    update = [Delta("old", t, -1), Delta("new", t, +1)]
    before = Multiset.of(["old", "other"])
    assert before + accumulate(update) == Multiset.of(["new", "other"])
    # An update that changes nothing consolidates away completely.
    assert consolidate([Delta("same", t, -1), Delta("same", t, +1)]) == []


# --- accumulation -----------------------------------------------------------


@given(batches, batches, frontiers)
def test_accumulation_is_a_homomorphism(
    a: list[Delta[int]], b: list[Delta[int]], f: Frontier
) -> None:
    assert accumulate([*a, *b], f) == accumulate(a, f) + accumulate(b, f)
    negated = [Delta(d.row, d.time, -d.diff) for d in a]
    assert accumulate(negated, f) == -accumulate(a, f)


@given(batches, frontiers)
def test_accumulation_only_counts_complete_times(batch: list[Delta[int]], f: Frontier) -> None:
    complete = [d for d in batch if f.is_complete(d.time)]
    assert accumulate(batch, f) == accumulate(complete)


# --- frontiers --------------------------------------------------------------


@given(frontiers, frontiers, frontiers)
def test_meet_is_a_semilattice(a: Frontier, b: Frontier, c: Frontier) -> None:
    assert Frontier.meet(a, b) == Frontier.meet(b, a)
    assert Frontier.meet(Frontier.meet(a, b), c) == Frontier.meet(a, Frontier.meet(b, c))
    assert Frontier.meet(a, a) == a
    assert Frontier.meet(a, Frontier.CLOSED) == a
    m = Frontier.meet(a, b)
    assert m <= a
    assert m <= b


@given(frontiers, frontiers, times)
def test_completeness_is_monotone_in_the_frontier(a: Frontier, b: Frontier, t: Lsn) -> None:
    lo, hi = (a, b) if a <= b else (b, a)
    if lo.is_complete(t):
        assert hi.is_complete(t)
    # A time is either complete or still admissible, never both.
    assert lo.is_complete(t) != lo.admits(t)


def test_frontier_semantics() -> None:
    f = Frontier.at(Lsn(10))
    assert f.is_complete(Lsn(9))
    assert not f.is_complete(Lsn(10))
    assert f.admits(Lsn(10))
    assert not f.admits(Lsn(9))
    assert Frontier.CLOSED.is_complete(Lsn(2**64 - 1))
    assert not Frontier.CLOSED.admits(Lsn(0))
    assert Frontier.meet() == Frontier.CLOSED
    assert Frontier.at(Lsn(3)) <= Frontier.at(Lsn(4)) <= Frontier.CLOSED
    closed, open_ = Frontier.CLOSED, Frontier.at(Lsn(4))
    assert not closed <= open_
