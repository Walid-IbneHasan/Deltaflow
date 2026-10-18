"""An engine restored from a checkpoint continues exactly where the
checkpoint was taken: replaying the transactions after it produces exactly
the changes the original engine produced for them."""

from __future__ import annotations

import random
from collections.abc import Sequence
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


def test_resuming_with_different_views_is_refused(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path / "state")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry([]), store)
    install(engine)
    sd.checkpoint(store, CP, engine.metadata())
    store.close()
    ck = sd.latest()
    assert ck is not None
    other = Deltaflow()
    other.view("v").source("public.orders").sink(MemorySink())
    restored = sd.open_live(memory_budget=8 * MB, restore=ck)
    with pytest.raises(PlanError, match="views changed"):
        Engine(other, restored, resume=ck.meta)
    restored.close()
