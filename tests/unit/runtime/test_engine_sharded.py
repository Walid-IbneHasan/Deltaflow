"""Sharding is exact: N shard engines, each fed the whole stream but keeping
only its shard's rows, produce partial results that merge to exactly what
one unsharded engine produces. That holds after every transaction, with
orders moving between shards (their customer changes), deletes, cascades
and unchanged TOAST values."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Row
from deltaflow.planner import Count, Deltaflow, Max, Min, PlanError, Sum
from deltaflow.planner.merge import merge_partials
from deltaflow.replication.changes import Change, RowDelete, RowInsert, RowUpdate, Transaction
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Column, Relation
from deltaflow.runtime import Engine
from deltaflow.sharding import shard_of
from deltaflow.sinks import MemorySink
from deltaflow.state import SourceStateError, TableSchema
from deltaflow.state.store import StateStore

ORDERS = Relation(
    1, "public", "orders", "d",
    (Column(True, "id", 20, -1), Column(False, "cid", 20, -1), Column(False, "amt", 1700, -1)),
)  # fmt: skip
CUSTOMERS = Relation(
    2, "public", "customers", "d",
    (Column(True, "id", 20, -1), Column(False, "country", 25, -1), Column(False, "bio", 25, -1)),
)  # fmt: skip
O_SCHEMA = TableSchema("public.orders", ("id", "cid", "amt"), ("id",))
C_SCHEMA = TableSchema("public.customers", ("id", "country", "bio"), ("id",))
CP = Lsn(0x1000)
SHARDS = 3
SHARD_BY = {"public.orders": "cid", "public.customers": "id"}


def registry(shards: int | None) -> tuple[Deltaflow, dict[str, MemorySink]]:
    df = Deltaflow(shards=shards, shard_by=SHARD_BY) if shards else Deltaflow()
    sinks = {n: MemorySink() for n in ("joined", "per_country", "totals", "distinct_cids")}
    (
        df.view("joined")
        .source("public.orders")
        .filter(lambda r: r["amt"] > 0)
        .join("public.customers", on=("cid", "id"))
        .select("orders.id", "country", "bio")
        .sink(sinks["joined"])
    )
    (
        df.view("per_country")
        .source("public.orders")
        .join("public.customers", on=("cid", "id"))
        .group_by(country="customers.country")
        .agg(n=Count(), revenue=Sum("amt"), lo=Min("amt"), hi=Max("amt"))
        .sink(sinks["per_country"])
    )
    (
        df.view("totals")
        .source("public.orders")
        .agg(n=Count(), revenue=Sum("amt"), hi=Max("amt"))
        .sink(sinks["totals"])
    )
    df.view("distinct_cids").source("public.orders").select("cid").distinct().sink(
        sinks["distinct_cids"]
    )
    return df, sinks


op = st.tuples(
    st.sampled_from(
        ["new_order", "move", "amt", "del_order", "new_cust", "country", "toast", "del_cust"]
    ),
    st.integers(0, 1000),
    st.integers(-1, 4),
)


@given(st.lists(st.lists(op, max_size=6), max_size=15))
def test_merged_shards_equal_the_unsharded_engine(  # noqa: PLR0912, PLR0915 - one script
    state_store: StateStore, script: list[list[tuple[str, int, int]]]
) -> None:
    customers = {i: {"id": i, "country": "AB"[i % 2], "bio": f"bio{i}"} for i in range(6)}
    orders = {i: {"id": i, "cid": i % 6, "amt": Decimal(i)} for i in range(12)}

    whole, whole_sinks = registry(None)
    engines: list[tuple[Engine, dict[str, MemorySink]]] = []
    for shard in [None, *range(SHARDS)]:
        df, sinks = (whole, whole_sinks) if shard is None else registry(SHARDS)
        engine = Engine(df, state_store, namespace=uuid.uuid4().hex, shard=shard)
        engine.install(O_SCHEMA, map(Row, orders.values()))
        engine.install(C_SCHEMA, map(Row, customers.values()))
        engine.start(CP)
        engines.append((engine, sinks))

    def check() -> None:
        _, reference = engines[0]
        for name, df_view in ((v.name, v) for v in registry(SHARDS)[0].views):
            partials = [sinks[name].result for _, sinks in engines[1:]]
            assert merge_partials(df_view, partials) == reference[name].result, name

    check()
    next_id = 100
    for n, ops in enumerate(script, start=1):
        changes: list[Change] = []
        for kind, pick, val in ops:
            oids, cids = list(orders), list(customers)
            if kind == "new_cust" or (kind in {"country", "toast", "del_cust"} and not cids):
                customers[next_id] = {"id": next_id, "country": "ABC"[val % 3], "bio": f"b{val}"}
                changes.append(RowInsert(CUSTOMERS, Lsn(0), dict(customers[next_id])))
            elif kind in {"country", "toast", "del_cust"}:
                cid = cids[pick % len(cids)]
                if kind == "del_cust":
                    del customers[cid]
                    changes.append(RowDelete(CUSTOMERS, Lsn(0), "key", {"id": cid}))
                else:
                    customers[cid]["country"] = "ABC"[val % 3]
                    new = dict(customers[cid])
                    if kind == "toast":
                        new["bio"] = UNCHANGED_TOAST  # customers never move shard
                    changes.append(RowUpdate(CUSTOMERS, Lsn(0), None, None, new))
            elif kind == "new_order" or not oids:
                orders[next_id] = {"id": next_id, "cid": pick % 9, "amt": Decimal(val)}
                changes.append(RowInsert(ORDERS, Lsn(0), dict(orders[next_id])))
            else:
                oid = oids[pick % len(oids)]
                if kind == "del_order":
                    del orders[oid]
                    changes.append(RowDelete(ORDERS, Lsn(0), "key", {"id": oid}))
                else:
                    if kind == "move":  # the shard key changes: the row changes shard
                        orders[oid]["cid"] = val + 3
                    else:
                        orders[oid]["amt"] = Decimal(val)
                    changes.append(RowUpdate(ORDERS, Lsn(0), None, None, dict(orders[oid])))
            next_id += 1
        commit = Lsn(CP + 0x100 * n)
        txn = Transaction(
            n, commit, Lsn(commit + 8), datetime(2026, 1, 1, tzinfo=UTC), None, tuple(changes)
        )
        for engine, _ in engines:
            engine.apply(txn)
        check()


def test_each_row_lives_in_exactly_one_shard(state_store: StateStore) -> None:
    orders = [Row({"id": i, "cid": i % 7, "amt": Decimal(1)}) for i in range(50)]
    sizes = []
    for shard in range(SHARDS):
        df, _ = registry(SHARDS)
        engine = Engine(df, state_store, namespace=uuid.uuid4().hex, shard=shard)
        engine.install(O_SCHEMA, orders)
        engine.install(C_SCHEMA, [])
        sizes.append(len(engine.arrangements["public.orders"]))
    assert sum(sizes) == len(orders)
    assert all(sizes)


def test_moving_into_a_shard_with_an_unchanged_toast_value_is_refused(
    state_store: StateStore,
) -> None:
    # An order moves onto a shard that never had it, and the update leaves a
    # large column out: there is nothing to fill it in from.
    target = next(c for c in range(100) if shard_of(c, SHARDS) == 1)
    source = next(c for c in range(100) if shard_of(c, SHARDS) == 0)
    df, _ = registry(SHARDS)
    engine = Engine(df, state_store, namespace=uuid.uuid4().hex, shard=1)
    engine.install(O_SCHEMA, [Row({"id": 1, "cid": source, "amt": Decimal(1)})])
    engine.install(C_SCHEMA, [])
    engine.start(CP)
    moved = {"id": 1, "cid": target, "amt": UNCHANGED_TOAST}
    txn = Transaction(1, Lsn(CP + 0x100), Lsn(CP + 0x108), datetime(2026, 1, 1, tzinfo=UTC), None,
                      (RowUpdate(ORDERS, Lsn(0), None, None, moved),))  # fmt: skip
    with pytest.raises(SourceStateError, match="moved into shard"):
        engine.apply(txn)


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda df: df.view("v").source("public.other"), "no shard column"),
        (
            lambda df: (
                df.view("v").source("public.orders").join("public.customers", on=("id", "id"))
            ),
            "co-partitioned",
        ),
        (
            lambda df: (
                df.view("v")
                .source("public.orders")
                .map(lambda r: r)
                .join("public.customers", on=("cid", "id"))
            ),
            "map before a join",
        ),
        (
            lambda df: (
                df.view("v").source("public.orders").group_by("cid").agg(n=Count()).select("n")
            ),
            "must be the last step",
        ),
        (
            lambda df: df.view("v").source("public.orders").distinct().select("id"),
            "must be the last step",
        ),
    ],
)
def test_views_that_cannot_be_sharded_exactly_are_refused(build: Any, message: str) -> None:
    with pytest.raises(PlanError, match=message):
        build(Deltaflow(shards=SHARDS, shard_by=SHARD_BY))
