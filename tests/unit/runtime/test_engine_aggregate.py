from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from deltaflow.core import Multiset, Row
from deltaflow.planner import Count, Deltaflow, Max, Sum
from deltaflow.replication.changes import RowDelete, RowInsert, RowUpdate, Transaction
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import Column, Relation
from deltaflow.runtime import Engine
from deltaflow.sinks import MemorySink
from deltaflow.state import TableSchema
from deltaflow.state.store import StateStore

ORDERS = Relation(
    1, "public", "orders", "d",
    (Column(True, "id", 20, -1), Column(False, "cid", 20, -1), Column(False, "amt", 1700, -1)),
)  # fmt: skip
CUSTOMERS = Relation(
    2, "public", "customers", "d", (Column(True, "id", 20, -1), Column(False, "country", 25, -1))
)
O_SCHEMA = TableSchema("public.orders", ("id", "cid", "amt"), ("id",))
C_SCHEMA = TableSchema("public.customers", ("id", "country"), ("id",))
CP = Lsn(0x1000)


def txn(n: int, *changes: RowInsert | RowUpdate | RowDelete) -> Transaction:
    commit = Lsn(CP + 0x100 * n)
    return Transaction(n, commit, Lsn(commit + 8), datetime(2026, 1, 1, tzinfo=UTC), None, changes)


def test_a_global_aggregate_over_an_empty_table_has_one_row(state_store: StateStore) -> None:
    df = Deltaflow()
    sink = MemorySink()
    df.view("totals").source("public.orders").agg(n=Count(), total=Sum("amt")).sink(sink)
    engine = Engine(df, state_store, namespace=uuid.uuid4().hex)
    engine.install(O_SCHEMA, [])
    engine.start(CP)
    assert sink.result == Multiset.of([Row({"n": 0, "total": None})])  # SELECT count(*), sum(..)
    engine.apply(txn(1, RowInsert(ORDERS, Lsn(0), {"id": 1, "cid": 1, "amt": Decimal("2.50")})))
    assert sink.result == Multiset.of([Row({"n": 1, "total": Decimal("2.50")})])
    engine.apply(txn(2, RowDelete(ORDERS, Lsn(0), "key", {"id": 1})))
    assert sink.result == Multiset.of([Row({"n": 0, "total": None})])


def test_aggregate_after_a_join_follows_a_customer_moving_country(
    state_store: StateStore,
) -> None:
    df = Deltaflow()
    sink = MemorySink()
    (
        df.view("per_country")
        .source("public.orders")
        .join("public.customers", on=("cid", "id"))
        .group_by("customers.country")
        .agg(orders=Count(), revenue=Sum("amt"), biggest=Max("amt"))
        .sink(sink)
    )
    engine = Engine(df, state_store, namespace=uuid.uuid4().hex)
    engine.install(
        O_SCHEMA,
        [Row({"id": i, "cid": i % 2, "amt": Decimal(i)}) for i in range(1, 5)],
    )
    engine.install(C_SCHEMA, [Row({"id": 0, "country": "NL"}), Row({"id": 1, "country": "DE"})])
    engine.start(CP)
    assert sink.result == Multiset.of(
        [
            Row(
                {
                    "customers.country": "NL",
                    "orders": 2,
                    "revenue": Decimal(6),
                    "biggest": Decimal(4),
                }
            ),
            Row(
                {
                    "customers.country": "DE",
                    "orders": 2,
                    "revenue": Decimal(4),
                    "biggest": Decimal(3),
                }
            ),
        ]
    )
    # One customer change moves two orders, and the max, to another group.
    engine.apply(txn(1, RowUpdate(CUSTOMERS, Lsn(0), None, None, {"id": 0, "country": "DE"})))
    assert sink.result == Multiset.of(
        [
            Row(
                {
                    "customers.country": "DE",
                    "orders": 4,
                    "revenue": Decimal(10),
                    "biggest": Decimal(4),
                }
            )
        ]
    )
