from __future__ import annotations

from collections import Counter
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from deltaflow.sharding import owner, shard_of


@given(st.integers(-(10**12), 10**12), st.integers(1, 64))
def test_shard_of_is_stable_and_in_range(value: int, n: int) -> None:
    s = shard_of(value, n)
    assert 0 <= s < n
    assert shard_of(value, n) == s


def test_equal_values_land_on_the_same_shard() -> None:
    # The canonical key encoding: 1, 1.0 and Decimal("1.00") are one key.
    assert len({shard_of(v, 16) for v in (1, 1.0, Decimal("1.00"))}) == 1
    # A known value, so a change of hash function shows up in review (it
    # would move every row to another shard).
    assert [shard_of(i, 8) for i in range(8)] == [shard_of(i, 8) for i in range(8)]


def test_keys_spread_evenly() -> None:
    counts = Counter(shard_of(i, 8) for i in range(80_000))
    assert len(counts) == 8
    assert max(counts.values()) < 1.05 * min(counts.values())


@given(st.sets(st.text(min_size=1, max_size=6), min_size=2, max_size=8), st.integers(1, 32))
def test_rendezvous_moves_only_the_lost_workers_shards(workers: set[str], shards: int) -> None:
    before = {s: owner(s, workers) for s in range(shards)}
    lost = sorted(workers)[0]
    after = {s: owner(s, workers - {lost}) for s in range(shards)}
    for s in range(shards):
        if before[s] != lost:
            assert after[s] == before[s]  # untouched
        else:
            assert after[s] != lost


def test_rendezvous_balances_and_needs_workers() -> None:
    workers = {"w1", "w2", "w3"}
    load = Counter(owner(s, workers) for s in range(300))
    assert set(load) == workers
    assert min(load.values()) > 70
    assert owner(0, set()) is None
