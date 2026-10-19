"""A join view through the whole engine: snapshots of both tables, then
transactions that change either or both sides at once (including the join
key itself), checked against a naive join of the model after every one."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from deltaflow.core import Frontier, Multiset, Row
from deltaflow.operators import JoinStateError
from deltaflow.planner import Deltaflow
from deltaflow.replication.changes import Change, RowDelete, RowInsert, RowUpdate, Transaction
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Column, Relation
from deltaflow.runtime import Engine
from deltaflow.sinks import MemorySink
from deltaflow.state import TableSchema
from deltaflow.state.store import StateStore

ORDERS = Relation(
    1, "public", "orders", "d",
    (Column(True, "id", 20, -1), Column(False, "cid", 20, -1), Column(False, "amt", 20, -1)),
)  # fmt: skip
CUSTOMERS = Relation(
    2, "public", "customers", "d",
    (Column(True, "id", 20, -1), Column(False, "country", 25, -1), Column(False, "bio", 25, -1)),
)  # fmt: skip
CP = Lsn(0x1000)

type Table = dict[int, dict[str, Any]]


def expected(orders: Table, customers: Table) -> Multiset[Row]:
    out: Multiset[Row] = Multiset()
    for o in orders.values():
        c = customers.get(o["cid"])
        if c is not None and o["amt"] > 0:
            out.add(Row({"order": o["id"], "country": c["country"], "bio": c["bio"]}))
    return out


op = st.tuples(
    st.sampled_from(
        ["new_order", "move_order", "amt", "del_order", "new_cust", "country", "toast", "del_cust"]
    ),
    st.integers(0, 1000),
    st.integers(-1, 3),
)


@given(st.lists(st.lists(op, max_size=6), max_size=20))
def test_join_view_matches_the_model_after_every_transaction(  # noqa: PLR0912 - one branch per op
    state_store: StateStore, script: list[list[tuple[str, int, int]]]
) -> None:
    customers: Table = {i: {"id": i, "country": f"C{i % 2}", "bio": f"bio{i}"} for i in range(3)}
    orders: Table = {i: {"id": i, "cid": i % 4, "amt": i} for i in range(5)}  # cid 3: no customer
    df = Deltaflow()
    sink = MemorySink()
    (
        df.view("v")
        .source("public.orders")
        .filter(lambda r: r["amt"] > 0)
        .join("public.customers", on=("cid", "id"))
        .map(lambda r: {"order": r["orders.id"], "country": r["country"], "bio": r["bio"]})
        .sink(sink)
    )
    engine = Engine(df, state_store, namespace=uuid.uuid4().hex)
    engine.install(
        TableSchema("public.orders", ("id", "cid", "amt"), ("id",)), map(Row, orders.values())
    )
    engine.install(
        TableSchema("public.customers", ("id", "country", "bio"), ("id",)),
        map(Row, customers.values()),
    )
    engine.start(CP)
    assert sink.result == expected(orders, customers)

    next_id = 100
    for n, ops in enumerate(script, start=1):
        changes: list[Change] = []
        for kind, pick, val in ops:
            oids, cids = list(orders), list(customers)
            if kind == "new_cust" or (kind in {"country", "toast", "del_cust"} and not cids):
                customers[next_id] = {"id": next_id, "country": f"C{val}", "bio": f"b{val}"}
                changes.append(RowInsert(CUSTOMERS, Lsn(0), dict(customers[next_id])))
            elif kind in {"country", "toast", "del_cust"}:
                cid = cids[pick % len(cids)]
                if kind == "del_cust":
                    del customers[cid]  # orders keep pointing at it: no FK in this model
                    changes.append(RowDelete(CUSTOMERS, Lsn(0), "key", {"id": cid}))
                else:
                    customers[cid]["country"] = f"C{val}"
                    new = dict(customers[cid])
                    if kind == "toast":
                        new["bio"] = UNCHANGED_TOAST
                    changes.append(RowUpdate(CUSTOMERS, Lsn(0), None, None, new))
            elif kind == "new_order" or not oids:
                orders[next_id] = {"id": next_id, "cid": pick % 5 + 98, "amt": val}
                changes.append(RowInsert(ORDERS, Lsn(0), dict(orders[next_id])))
            else:
                oid = oids[pick % len(oids)]
                if kind == "del_order":
                    del orders[oid]
                    changes.append(RowDelete(ORDERS, Lsn(0), "key", {"id": oid}))
                else:
                    if kind == "move_order":  # change the join key
                        orders[oid]["cid"] = cids[val % len(cids)] if cids else val
                    else:
                        orders[oid]["amt"] = val
                    changes.append(RowUpdate(ORDERS, Lsn(0), None, None, dict(orders[oid])))
            next_id += 1
        commit = Lsn(CP + 0x100 * n)
        at = datetime(2026, 1, 1, tzinfo=UTC)
        engine.apply(Transaction(n, commit, Lsn(commit + 8), at, None, tuple(changes)))
        assert sink.frontier == Frontier.at(Lsn(commit + 8))
        assert sink.result == expected(orders, customers)


def _left_join_engine(state_store: StateStore) -> tuple[Engine, MemorySink]:
    df = Deltaflow()
    sink = MemorySink()
    (
        df.view("orders_left_customers")
        .source("public.orders")
        .join("public.customers", on=("cid", "id"), how="left")
        .select("orders.id", "country")
        .sink(sink)
    )
    return Engine(df, state_store, namespace=uuid.uuid4().hex), sink


ORDERS_SCHEMA = TableSchema("public.orders", ("id", "cid", "amt"), ("id",))
CUSTOMERS_SCHEMA = TableSchema("public.customers", ("id", "country", "bio"), ("id",))


def test_a_left_join_loads_its_left_table_first_once_schemas_are_declared(
    state_store: StateStore,
) -> None:
    """The snapshot loader installs tables in source order: the left table
    first, while the right index is still empty, so every left row is padded
    and the right table's install then retracts the padding. That needs the
    right table's columns before its rows: load_snapshot declares every
    schema first."""
    engine, sink = _left_join_engine(state_store)
    engine.declare(CUSTOMERS_SCHEMA)
    engine.install(
        ORDERS_SCHEMA, [Row({"id": 1, "cid": 7, "amt": 5}), Row({"id": 2, "cid": 8, "amt": 5})]
    )
    engine.install(CUSTOMERS_SCHEMA, [Row({"id": 7, "country": "NL", "bio": None})])
    engine.start(CP)
    assert sink.result == Multiset.of(
        [Row({"orders.id": 1, "country": "NL"}), Row({"orders.id": 2, "country": None})]
    )


def test_a_left_join_refuses_to_guess_the_right_columns(state_store: StateStore) -> None:
    engine, _ = _left_join_engine(state_store)
    with pytest.raises(JoinStateError, match="declare every table's schema"):
        engine.install(ORDERS_SCHEMA, [Row({"id": 1, "cid": 7, "amt": 5})])
