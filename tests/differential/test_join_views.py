"""M3 acceptance, part 1: two-table join views equal the SQL join after
every commit, across 100k randomized operations, with operator state read
back from disk.

The workload's weights are tuned so the tables hover around a steady size
(about 300 customers and 1,500 orders) instead of growing for 100k
operations, which keeps a full SQL recompute after *every* commit
affordable. Operations that stress the join:

* an order moves to another customer (the join key changes);
* a customer changes country (one change fans out to all of its orders);
* a customer is deleted (the cascade retracts its orders from both sides
  of the join, in one transaction);
* an UPDATE leaves a TOASTed bio untouched (the join output must still
  carry the real bio);
* an order's primary key changes.

The state store gets a 1 MiB budget, the minimum, so state is evicted and
read back from SST files throughout. The memory-budget part of the
acceptance is `test_memory_budget.py`.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.core import Frontier, Row
from deltaflow.devtools.loadgen import LoadStats, load_state
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import OpKind, Workload, WorkloadConfig
from deltaflow.planner import Deltaflow
from deltaflow.runtime import Runner
from deltaflow.sinks import MemorySink
from deltaflow.state.store import StoreStats
from tests.differential.lockstep import Sql, execute, until_marker
from tests.sql_oracle import first_difference, query_multiset

#: The spec's number; DELTAFLOW_JOIN_OPS overrides it for quick local runs.
TARGET_OPS = int(os.environ.get("DELTAFLOW_JOIN_OPS", "100000"))
BUDGET = 1 << 20
TIMEOUT_S = 3600

#: Insert and delete rates balance (customers), or balance against the
#: cascade (orders: 28 inserts vs 18 deletes + 2 customer deletes x ~5 orders).
STEADY = WorkloadConfig(
    max_statements_per_txn=24,
    toast_probability=0.3,
    weights={
        OpKind.INSERT_CUSTOMER: 2,
        OpKind.UPDATE_CUSTOMER: 14,
        OpKind.DELETE_CUSTOMER: 2,
        OpKind.INSERT_ORDER: 28,
        OpKind.UPDATE_ORDER: 30,
        OpKind.REKEY_ORDER: 2,
        OpKind.DELETE_ORDER: 18,
    },
)


def md5(text: str | None) -> str | None:
    return None if text is None else hashlib.md5(text.encode()).hexdigest()


def order_with_customer(r: Row) -> Mapping[str, Any]:
    return {
        "id": r["orders.id"],
        "customer_id": r["customer_id"],
        "status": r["status"],
        "amount": r["amount"],
        "name": r["name"],
        "country": r["country"],
        "bio_md5": md5(r["bio"]),
    }


def pending(r: Row) -> bool:
    return bool(r["status"] == "pending")


def european(r: Row) -> bool:
    # After the join: an unqualified name, resolved because it is unambiguous.
    return r["country"] in ("NL", "DE", "FR")


@dataclass(frozen=True)
class Case:
    register: Callable[[Deltaflow, MemorySink], object]
    sql: str


CASES: dict[str, Case] = {
    "orders_with_customer": Case(
        lambda df, s: (
            df.view("orders_with_customer")
            .source("public.orders")
            .join("public.customers", on=("customer_id", "id"))
            .map(order_with_customer)
            .sink(s)
        ),
        # md5 once per customer rather than once per joined row; same result.
        "WITH c AS MATERIALIZED (SELECT id, name, country, md5(bio) AS bio_md5 FROM customers)"
        " SELECT o.id, o.customer_id, o.status, o.amount, c.name, c.country, c.bio_md5"
        " FROM orders o JOIN c ON c.id = o.customer_id",
    ),
    # Filters on both sides of the join, and a projection that is not
    # injective (two pending orders of one customer give equal rows).
    "pending_in_europe": Case(
        lambda df, s: (
            df.view("pending_in_europe")
            .source("public.orders")
            .filter(pending)
            .join("public.customers", on=("customer_id", "id"))
            .filter(european)
            .select("customers.id", "customers.name", "country")
            .sink(s)
        ),
        'SELECT c.id AS "customers.id", c.name AS "customers.name", c.country'
        " FROM orders o JOIN customers c ON c.id = o.customer_id"
        " WHERE o.status = 'pending' AND c.country IN ('NL', 'DE', 'FR')",
    ),
}


@dataclass
class Report:
    load: LoadStats = field(default_factory=LoadStats)
    checkpoints: int = 0
    sizes: dict[str, tuple[int, int]] = field(default_factory=dict)
    counters: Counter[str] = field(default_factory=Counter)
    store: StoreStats | None = None


def test_join_views_equal_sql_after_every_commit(dsn: str, workload_seed: int) -> None:
    report = run(_scenario(dsn, workload_seed))
    total = sum(report.load.statements.values())
    print(
        f"load: committed={report.load.committed} rolled_back={report.load.rolled_back} "
        f"statements={total} {dict(report.load.statements)}"
    )
    print(f"checkpoints compared: {report.checkpoints}; engine: {dict(report.counters)}")
    print(f"state store: {report.store}")
    for name, (lo, hi) in report.sizes.items():
        print(f"  {name:22} rows {lo}..{hi}")

    assert total >= TARGET_OPS
    assert report.checkpoints >= report.load.committed + report.load.rolled_back
    assert report.load.rolled_back > 0
    assert report.load.statements[OpKind.REKEY_ORDER] > 0
    assert report.load.statements[OpKind.DELETE_CUSTOMER] > 0
    assert report.counters["unchanged_toast_resolved"] > 0
    assert report.store is not None
    # State really lived on disk, not in the 1 MiB of cache.
    assert report.store.sst_bytes > 2 * BUDGET, report.store
    for name, (_, hi) in report.sizes.items():
        assert hi > 0, f"{name} was never non-empty"


async def _scenario(dsn: str, seed: int) -> Report:
    report = Report()
    context = f"seed={seed} (replay: DELTAFLOW_SEED={seed} make differential)"
    await seed_database(dsn, seed=seed, customers=300, orders=1_500)
    df = Deltaflow()
    sinks = {name: MemorySink() for name in CASES}
    for name, case in CASES.items():
        case.register(df, sinks[name])

    async def compare(sql: Sql, frontier: Frontier, where: str) -> None:
        for name, case in CASES.items():
            sink = sinks[name]
            assert sink.frontier == frontier, f"{name} frontier {sink.frontier} != {frontier}"
            expected = await query_multiset(sql, case.sql)
            if sink.result != expected:
                raise AssertionError(
                    f"{name} != SQL {where}: {first_difference(sink.result, expected)}; {context}"
                )
            lo, hi = report.sizes.get(name, (expected.size(), expected.size()))
            report.sizes[name] = (min(lo, expected.size()), max(hi, expected.size()))
        report.checkpoints += 1

    async with (
        asyncio.timeout(TIMEOUT_S),
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql,
        Runner(
            df, dsn, publication="deltaflow_diff_join", status_interval=1, memory_budget=BUDGET
        ) as runner,
    ):
        await compare(sql, runner.engine.frontier, "after the snapshot")
        workload = Workload(seed, await load_state(sql), STEADY)
        i = 0
        while sum(report.load.statements.values()) < TARGET_OPS:
            if await execute(sql, workload, report.load, marker=str(i)):
                txn = await until_marker(runner, str(i))
                await compare(sql, Frontier.at(txn.end_lsn), f"after transaction #{i}")
            else:
                await compare(sql, runner.engine.frontier, f"after rolled-back #{i}")
            i += 1
        assert await load_state(sql) == workload.state, f"database != workload model; {context}"
        report.counters.update(runner.engine.counters)
        for arrangement in runner.engine.arrangements.values():
            report.counters.update(arrangement.counters)
        report.store = runner.store.stats()
    return report
