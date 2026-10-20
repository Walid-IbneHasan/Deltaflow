"""An engine restored from a checkpoint continues exactly where the
checkpoint was taken: replaying the transactions after it produces exactly
the changes the original engine produced for them."""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from deltaflow.core import Delta, Frontier, Row, accumulate
from deltaflow.planner import Count, Deltaflow, Max, Min, PlanError
from deltaflow.replication.changes import RowDelete, RowInsert, RowUpdate, Transaction
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import Column, Relation
from deltaflow.runtime import Engine
from deltaflow.sinks import MemorySink, Sink
from deltaflow.state import TableSchema
from deltaflow.state.durable import StateDirectory

ORDERS = Relation(
    1,
    "public",
    "orders",
    "d",
    (Column(True, "id", 20, -1), Column(False, "cid", 20, -1), Column(False, "amt", 1700, -1)),
)
CUSTOMERS = Relation(
    2, "public", "customers", "d", (Column(True, "id", 20, -1), Column(False, "country", 25, -1))
)
O_SCHEMA = TableSchema("public.orders", ("id", "cid", "amt"), ("id",))
C_SCHEMA = TableSchema("public.customers", ("id", "country"), ("id",))
CP = Lsn(0x1000)
MB = 1 << 20


class Recorder(Sink):
    """Records every delta it is given. A MemorySink cannot be resumed (it
    would start empty and refuse to retract rows from before the checkpoint);
    that is why durable runs require a durable sink."""

    def __init__(self, into: list[Delta[Row]]) -> None:
        self.into = into

    def accept(self, batch: Sequence[Delta[Row]]) -> None:
        self.into.extend(batch)

    def advance(self, frontier: Frontier) -> None:
        pass


def registry(changes: list[Delta[Row]]) -> Deltaflow:
    df = Deltaflow()
    (
        df.view("per_country")
        .source("public.orders")
        .join("public.customers", on=("cid", "id"))
        .group_by("customers.country")
        .agg(n=Count(), lo=Min("amt"), hi=Max("amt"))
        .sink(Recorder(changes))
    )
    # A global aggregate: its row exists from the start and must not be
    # re-created on resume.
    df.view("totals").source("public.orders").agg(n=Count(), hi=Max("amt")).sink(Recorder(changes))
    return df


def workload(seed: int, n: int) -> list[Transaction]:
    rng = random.Random(seed)
    orders: dict[int, int] = {i: i % 3 for i in range(10)}
    txns = []
    next_id = 100
    for k in range(1, n + 1):
        changes: list[RowInsert | RowUpdate | RowDelete] = []
        for _ in range(rng.randint(1, 4)):
            roll = rng.random()
            if roll < 0.4 or not orders:
                orders[next_id] = rng.randrange(3)
                changes.append(
                    RowInsert(
                        ORDERS,
                        Lsn(0),
                        {"id": next_id, "cid": orders[next_id], "amt": Decimal(rng.randrange(100))},
                    )
                )
                next_id += 1
            elif roll < 0.7:
                oid = rng.choice(list(orders))
                orders[oid] = rng.randrange(3)
                changes.append(
                    RowUpdate(
                        ORDERS,
                        Lsn(0),
                        None,
                        None,
                        {"id": oid, "cid": orders[oid], "amt": Decimal(rng.randrange(100))},
                    )
                )
            elif roll < 0.9:
                oid = rng.choice(list(orders))
                del orders[oid]
                changes.append(RowDelete(ORDERS, Lsn(0), "key", {"id": oid}))
            else:
                changes.append(
                    RowUpdate(
                        CUSTOMERS,
                        Lsn(0),
                        None,
                        None,
                        {"id": rng.randrange(3), "country": rng.choice("ABC")},
                    )
                )
        commit = Lsn(CP + 0x100 * k)
        txns.append(
            Transaction(
                k, commit, Lsn(commit + 8), datetime(2026, 1, 1, tzinfo=UTC), None, tuple(changes)
            )
        )
    return txns


def install(engine: Engine) -> None:
    engine.install(O_SCHEMA, [Row({"id": i, "cid": i % 3, "amt": Decimal(i)}) for i in range(10)])
    engine.install(C_SCHEMA, [Row({"id": i, "country": "ABC"[i]}) for i in range(3)])
    engine.start(CP)


@pytest.mark.parametrize("cut", [0, 1, 17, 39])
def test_replay_after_restore_produces_exactly_the_original_changes(
    tmp_path: Path, cut: int
) -> None:
    txns = workload(seed=cut, n=40)
    # The original run, start to finish, checkpointing after transaction `cut`.
    original: list[Delta[Row]] = []
    sd = StateDirectory(tmp_path / "state")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry(original), store)
    install(engine)
    for i, txn in enumerate(txns):
        if i == cut:
            sd.checkpoint(store, Lsn(engine.frontier.lower or 0), engine.metadata())
        engine.apply(txn)
    store.close()  # "crash": live state is discarded below
    before_cut = [d for d in original if d.time < txns[cut].commit_lsn]

    ck = sd.latest()
    assert ck is not None
    resumed_changes: list[Delta[Row]] = []
    restored = sd.open_live(memory_budget=8 * MB, restore=ck)
    resumed = Engine(registry(resumed_changes), restored, resume=ck.meta)
    assert resumed.frontier == Frontier.at(ck.lsn)
    for txn in txns[cut:]:
        resumed.apply(txn)
    restored.close()
    # What the resumed engine emitted, on top of what was emitted before the
    # checkpoint, is the whole original history.
    assert accumulate([*before_cut, *resumed_changes]) == accumulate(original)
    assert sorted(map(repr, resumed_changes)) == sorted(
        map(repr, [d for d in original if d.time >= txns[cut].commit_lsn])
    )


def checkpoint_after(tmp_path: Path, txns: list[Transaction], cut: int) -> StateDirectory:
    """Run the original registry through txns[:cut] and checkpoint there."""
    sd = StateDirectory(tmp_path / "state")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry([]), store)
    install(engine)
    for txn in txns[:cut]:
        engine.apply(txn)
    sd.checkpoint(store, Lsn(engine.frontier.lower or 0), engine.metadata())
    store.close()
    return sd


def by_cid(df: Deltaflow, sink: Sink) -> None:
    df.view("by_cid").source("public.orders").group_by("cid").agg(n=Count(), hi=Max("amt")).sink(
        sink
    )


def from_scratch(
    txns: list[Transaction], build: Callable[[Deltaflow, Sink], None], tmp: Path
) -> MemorySink:
    """The view's result from a fresh engine fed everything: the reference."""
    df = Deltaflow()
    sink = MemorySink()
    build(df, sink)
    sd = StateDirectory(tmp)
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(df, store)
    engine.install(O_SCHEMA, [Row({"id": i, "cid": i % 3, "amt": Decimal(i)}) for i in range(10)])
    engine.start(CP)
    for txn in txns:
        engine.apply(txn)
    store.close()
    return sink


def test_a_new_and_a_changed_view_are_built_from_the_arrangements(tmp_path: Path) -> None:
    """Resuming under changed views rebuilds only those: from the source
    rows the checkpoint holds (its arrangements), exactly as of its
    position. The unchanged view continues as if nothing had happened."""
    txns = workload(seed=7, n=40)
    sd = checkpoint_after(tmp_path, txns, cut=20)
    ck = sd.latest()
    assert ck is not None

    kept: list[Delta[Row]] = []
    changed, added = MemorySink(), MemorySink()
    df = Deltaflow()
    (
        df.view("per_country")
        .source("public.orders")
        .join("public.customers", on=("cid", "id"))
        .group_by("customers.country")
        .agg(n=Count(), lo=Min("amt"), hi=Max("amt"))
        .sink(Recorder(kept))
    )
    df.view("totals").source("public.orders").agg(n=Count(), lo=Min("amt")).sink(changed)
    by_cid(df, added)
    restored = sd.open_live(memory_budget=8 * MB, restore=ck)
    engine = Engine(df, restored, resume=ck.meta)
    assert engine.rebuilt == ("totals", "by_cid")
    assert engine.dropped == ()
    for txn in txns[20:]:
        engine.apply(txn)
    restored.close()

    def totals(df: Deltaflow, sink: Sink) -> None:
        df.view("totals").source("public.orders").agg(n=Count(), lo=Min("amt")).sink(sink)

    assert changed.result == from_scratch(txns, totals, tmp_path / "a").result
    assert added.result == from_scratch(txns, by_cid, tmp_path / "b").result
    # the kept view emitted exactly what the original did after the checkpoint
    original: list[Delta[Row]] = []
    reference = StateDirectory(tmp_path / "c").open_live(memory_budget=8 * MB)
    ref = Engine(registry(original), reference)
    install(ref)
    for txn in txns:
        ref.apply(txn)
    reference.close()
    after = [d for d in original if d.time >= txns[20].commit_lsn and "country" in repr(d.row)]
    assert sorted(map(repr, kept)) == sorted(map(repr, after))


def test_a_removed_view_drops_its_state(tmp_path: Path) -> None:
    sd = checkpoint_after(tmp_path, workload(seed=3, n=10), cut=10)
    ck = sd.latest()
    assert ck is not None
    totals_keyspaces = ck.meta["views"]["totals"]["keyspaces"]
    assert totals_keyspaces  # a global aggregate keeps its group
    df = Deltaflow()
    # the same view, sink type included: a sink's type is part of a view's fingerprint
    df.view("totals").source("public.orders").agg(n=Count(), hi=Max("amt")).sink(Recorder([]))
    restored = sd.open_live(memory_budget=8 * MB, restore=ck)
    engine = Engine(df, restored, resume=ck.meta)
    assert engine.rebuilt == ()
    assert engine.dropped == ("per_country",)
    for name in ck.meta["views"]["per_country"]["keyspaces"]:
        assert restored.keyspace(name).first() is None, name
    # customers is no longer read: its copy is dropped too, or it would go stale
    assert "public.customers" not in engine.metadata()["arrangements"]
    restored.close()


def test_a_view_over_a_table_the_checkpoint_has_no_copy_of_cannot_resume(tmp_path: Path) -> None:
    sd = checkpoint_after(tmp_path, workload(seed=1, n=5), cut=5)
    ck = sd.latest()
    assert ck is not None
    df = Deltaflow()
    df.view("other").source("public.other").sink(MemorySink())
    assert "public.other" in (Engine.cannot_resume(df, ck.meta) or "")
    restored = sd.open_live(memory_budget=8 * MB, restore=ck)
    with pytest.raises(PlanError, match=r"no copy of public\.other"):
        Engine(df, restored, resume=ck.meta)
    restored.close()
