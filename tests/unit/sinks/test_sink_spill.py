"""The Postgres sink with a spill keyspace: past `spill_after` deltas or
keys, its buffers move into the state store, so a large transaction (or a
bootstrap's whole view) is never in memory at once. The writes it produces
must be exactly those of the in-memory sink."""

from __future__ import annotations

import random
import uuid

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from deltaflow.core import Delta, Frontier, Row
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks import PostgresSink, SinkError
from deltaflow.sinks.postgres import SinkWrites
from deltaflow.state.store import StateStore

COLUMNS = ("k", "n")
KEYS = "ABCDEFGH"


def bound(store: StateStore | None, spill_after: int = 1) -> PostgresSink:
    sink = PostgresSink("analytics.t")
    sink.bind(
        view="t",
        columns=COLUMNS,
        key=("k",),
        watermark=None,
        spill=None if store is None else store.keyspace(f"spill:{uuid.uuid4().hex}"),
        spill_after=spill_after,
    )
    return sink


def row(k: str, n: int) -> Row:
    return Row({"k": k, "n": n})


def summary(writes: list[SinkWrites]) -> tuple[set[Row], set[Row], set[tuple[object, ...]]]:
    return (
        {r for w in writes for r in w.inserts},
        {r for w in writes for r in w.updates},
        {k for w in writes for k in w.deletes},
    )


# per step: ("txn", seed) | ("advance", 0) | ("take", chunk size)
steps = st.lists(
    st.tuples(st.sampled_from(["txn", "txn", "txn", "advance", "take"]), st.integers(1, 50)),
    max_size=40,
)


@settings(max_examples=200)
@given(plan=steps, spill_after=st.integers(1, 5))
def test_spilled_writes_are_the_in_memory_writes(
    state_store: StateStore, plan: list[tuple[str, int]], spill_after: int
) -> None:
    memory, spilled = bound(None), bound(state_store, spill_after)
    table: dict[str, int] = {}
    time, frontier = 0, 0
    for kind, arg in plan:
        if kind == "txn":
            # A complete, valid change of the view at a new time: some keys
            # retracted, some re-added, some added; deltas in random order.
            time += 1
            rng = random.Random(arg)
            deltas = []
            for k in rng.sample(KEYS, rng.randint(1, len(KEYS))):
                if k in table:
                    deltas.append(Delta(row(k, table.pop(k)), Lsn(time), -1))
                if rng.random() < 0.6:
                    table[k] = rng.randint(0, 9)
                    deltas.append(Delta(row(k, table[k]), Lsn(time), 1))
            rng.shuffle(deltas)
            for sink in (memory, spilled):
                sink.accept(deltas)
        elif kind == "advance":
            frontier = time + 1
            for sink in (memory, spilled):
                sink.advance(Frontier.at(Lsn(frontier)))
        else:
            assert summary([memory.take()]) == summary(list(spilled.take_chunks(arg)))
    for sink in (memory, spilled):
        sink.advance(Frontier.at(Lsn(time + 1)))
    assert summary([memory.take()]) == summary(list(spilled.take_chunks(3)))


def test_a_large_transaction_stays_out_of_memory(state_store: StateStore) -> None:
    sink = bound(state_store, spill_after=100)
    sink.accept([Delta(row(f"k{i}", i), Lsn(5), 1) for i in range(1_000)])
    assert sink.buffered <= 100
    sink.advance(Frontier.at(Lsn(6)))
    assert sink.buffered <= 100
    chunks = list(sink.take_chunks(300))
    assert [len(c.inserts) for c in chunks] == [300, 300, 300, 100]
    assert {r["n"] for c in chunks for r in c.inserts} == set(range(1_000))
    assert list(sink.take_chunks(300)) == []  # taken: nothing left


def test_spilled_deltas_are_checked_like_any_others(state_store: StateStore) -> None:
    sink = bound(state_store, spill_after=1)
    sink.accept([Delta(row("A", 1), Lsn(1), 1), Delta(row("A", 2), Lsn(1), 1)])
    with pytest.raises(SinkError, match="has two rows"):
        sink.advance(Frontier.at(Lsn(2)))


def test_spilled_replays_at_or_below_the_watermark_are_dropped(state_store: StateStore) -> None:
    sink = PostgresSink("analytics.t")
    sink.bind(
        view="t", columns=COLUMNS, key=("k",), watermark=Lsn(10),
        spill=state_store.keyspace(f"spill:{uuid.uuid4().hex}"), spill_after=1,
    )  # fmt: skip
    sink.accept([Delta(row("A", 1), Lsn(5), 1), Delta(row("B", 1), Lsn(5), 1)])
    sink.advance(Frontier.at(Lsn(10)))
    assert list(sink.take_chunks(10)) == []
    sink.accept([Delta(row("C", 1), Lsn(11), 1), Delta(row("D", 1), Lsn(11), 1)])
    sink.advance(Frontier.at(Lsn(12)))
    assert summary(list(sink.take_chunks(10)))[0] == {row("C", 1), row("D", 1)}
