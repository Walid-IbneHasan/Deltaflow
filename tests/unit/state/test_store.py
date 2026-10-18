from __future__ import annotations

import os
from pathlib import Path

import pytest

from deltaflow.state.store import StateStore

MB = 1 << 20


def test_keyspaces_are_isolated_and_scans_are_exact(tmp_path: Path) -> None:
    with StateStore(tmp_path / "s", memory_budget=8 * MB) as store:
        a, b, ab = store.keyspace("a"), store.keyspace("b"), store.keyspace("ab")
        a.put(b"k1", b"1")
        a.put(b"k12", b"12")
        a.put(b"k2", b"2")
        b.put(b"k1", b"other")
        ab.put(b"k1", b"ab")
        assert a.get(b"k1") == b"1"
        assert a.get(b"nope") is None
        assert list(a.scan(b"k1")) == [(b"k1", b"1"), (b"k12", b"12")]
        assert list(a.scan()) == [(b"k1", b"1"), (b"k12", b"12"), (b"k2", b"2")]
        assert list(b.scan()) == [(b"k1", b"other")]
        assert list(ab.scan()) == [(b"k1", b"ab")]  # "a" + "b..." does not leak into "ab"
        a.delete(b"k12")
        assert list(a.scan(b"k1")) == [(b"k1", b"1")]
        a.clear()
        assert list(a.scan()) == []
        assert b.get(b"k1") == b"other"


def test_writes_in_a_batch_land_together(tmp_path: Path) -> None:
    with StateStore(tmp_path / "s", memory_budget=8 * MB) as store:
        ks = store.keyspace("x")
        with store.batch() as batch:
            batch.put(ks, b"a", b"1")
            batch.put(ks, b"b", b"2")
            batch.delete(ks, b"a")
            assert ks.get(b"b") is None  # not visible before the batch is written
        assert list(ks.scan()) == [(b"b", b"2")]


def test_state_larger_than_the_budget_spills_to_disk_and_reads_back(tmp_path: Path) -> None:
    budget = 2 * MB
    with StateStore(tmp_path / "s", memory_budget=budget) as store:
        ks = store.keyspace("big")
        value = os.urandom(4096)
        n = 10_000  # ~40 MB, 20x the budget
        for i in range(n):
            ks.put(i.to_bytes(4, "big"), value + i.to_bytes(4, "big"))
        stats = store.stats()
        assert stats.sst_bytes > 10 * budget, stats
        assert stats.cache_usage <= budget * 1.1, stats
        for i in range(0, n, 97):
            assert ks.get(i.to_bytes(4, "big")) == value + i.to_bytes(4, "big")
        assert sum(1 for _ in ks.scan()) == n


def test_a_fresh_store_starts_empty(tmp_path: Path) -> None:
    path = tmp_path / "s"
    with StateStore(path, memory_budget=8 * MB) as store:
        store.keyspace("x").put(b"k", b"v")
    with StateStore(path, memory_budget=8 * MB) as store:
        assert list(store.keyspace("x").scan()) == []


def test_budget_must_be_sane(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least"):
        StateStore(tmp_path / "s", memory_budget=1000)


def test_first_and_last_of_a_prefix(tmp_path: Path) -> None:
    with StateStore(tmp_path / "s", memory_budget=8 * MB) as store:
        ks, other = store.keyspace("k"), store.keyspace("l")
        assert ks.first(b"g1") is None
        assert ks.last(b"g1") is None
        for key in (b"g0\x05", b"g1\x01", b"g1\x07", b"g1\xff\xff", b"g2\x00"):
            ks.put(key, key)
        other.put(b"", b"next keyspace")
        assert ks.first(b"g1") == (b"g1\x01", b"g1\x01")
        assert ks.last(b"g1") == (b"g1\xff\xff", b"g1\xff\xff")
        assert ks.last(b"g2") == (b"g2\x00", b"g2\x00")  # not the next keyspace's entry
        assert ks.first(b"") == (b"g0\x05", b"g0\x05")
        assert ks.last(b"") == (b"g2\x00", b"g2\x00")
        ks.delete(b"g1\xff\xff")
        assert ks.last(b"g1") == (b"g1\x07", b"g1\x07")
