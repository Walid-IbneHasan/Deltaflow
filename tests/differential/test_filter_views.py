"""M2 acceptance: filtered and mapped views equal their SQL recompute at
every frontier, over a randomized workload.

Each view is a pair: the Deltaflow pipeline, and the plain SQL that defines
what it should contain. The test has two phases.

1. Lockstep. Every committed transaction carries a numbered logical
   message. After running a transaction, the test waits until the engine
   has applied that numbered transaction, then compares every view's
   result with its SQL. The load is paused, so the database is exactly at
   the view's frontier. The comparison is of multisets: same rows, same
   multiplicities. A rolled-back transaction is compared too; it must not
   change anything.
2. Concurrent burst. The load runs while the engine consumes the stream, so
   transactions arrive back-to-back and the stream exerts backpressure.
   After an end marker, everything is compared once more.

Python predicates and SQL disagree about NULL (three-valued logic), so each
predicate below says explicitly what it does with NULLs. The SQL is written
to match, which is exactly what a user porting a WHERE clause has to do.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.core import Delta, Frontier, Row
from deltaflow.devtools.loadgen import LoadStats, load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import TOAST_VALUE_MIN_LEN, OpKind, Workload
from deltaflow.planner import Deltaflow
from deltaflow.runtime import Runner
from deltaflow.sinks import MemorySink
from deltaflow.sinks.memory import OnChange
from tests.differential.lockstep import MARKER, execute, until_marker
from tests.sql_oracle import first_difference, query_multiset

LOCKSTEP_TXNS = 1_000
BURST_TXNS = 600
TIMEOUT_S = 900

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]


def md5(text: str | None) -> str | None:
    return None if text is None else hashlib.md5(text.encode()).hexdigest()


def big_active(r: Row) -> bool:
    return bool(r["amount"] > 2500 and r["status"] != "cancelled")


def noted(r: Row) -> bool:
    return r["note"] is not None


def unnoted_or_cancelled(r: Row) -> bool:
    return r["note"] is None or r["status"] == "cancelled"


def note_length(r: Row) -> Mapping[str, Any]:
    return {"id": r["id"], "note": r["note"], "note_len": len(r["note"])}


def bio_digest(r: Row) -> Mapping[str, Any]:
    bio = r["bio"]
    return {
        "id": r["id"],
        "name": r["name"],
        "email": r["email"],
        "bio_md5": md5(bio),
        "bio_len": None if bio is None else len(bio),
    }


def toasted(r: Row) -> bool:
    return r["bio"] is not None and len(r["bio"]) >= TOAST_VALUE_MIN_LEN


@dataclass(frozen=True)
class Case:
    register: Callable[[Deltaflow, MemorySink], object]
    sql: str


CASES: dict[str, Case] = {
    "orders_all": Case(
        lambda df, s: df.view("orders_all").source("public.orders").sink(s),
        "SELECT * FROM orders",
    ),
    "orders_big_active": Case(
        lambda df, s: (
            df.view("orders_big_active")
            .source("public.orders")
            .filter(big_active)
            .select("id", "customer_id", "status", "amount")
            .sink(s)
        ),
        "SELECT id, customer_id, status, amount FROM orders"
        " WHERE amount > 2500 AND status <> 'cancelled'",
    ),
    "orders_noted": Case(
        lambda df, s: (
            df.view("orders_noted").source("public.orders").filter(noted).map(note_length).sink(s)
        ),
        "SELECT id, note, length(note) AS note_len FROM orders WHERE note IS NOT NULL",
    ),
    "orders_unnoted_or_cancelled": Case(
        lambda df, s: (
            df.view("orders_unnoted_or_cancelled")
            .source("public.orders")
            .filter(unnoted_or_cancelled)
            .select("id", "status")
            .sink(s)
        ),
        "SELECT id, status FROM orders WHERE note IS NULL OR status = 'cancelled'",
    ),
    # A projection that is not injective: a bag with large multiplicities.
    "order_statuses": Case(
        lambda df, s: df.view("order_statuses").source("public.orders").select("status").sink(s),
        "SELECT status FROM orders",
    ),
    "customer_countries": Case(
        lambda df, s: (
            df.view("customer_countries").source("public.customers").select("country").sink(s)
        ),
        "SELECT country FROM customers",
    ),
    # Every bio, checked by digest: an unresolved TOAST placeholder or a
    # blanked value shows up here.
    "customer_bios": Case(
        lambda df, s: df.view("customer_bios").source("public.customers").map(bio_digest).sink(s),
        "SELECT id, name, email, md5(bio) AS bio_md5, length(bio) AS bio_len FROM customers",
    ),
    "toasted_customers": Case(
        lambda df, s: (
            df.view("toasted_customers")
            .source("public.customers")
            .filter(toasted)
            .select("id", "country")
            .sink(s)
        ),
        f"SELECT id, country FROM customers WHERE length(bio) >= {TOAST_VALUE_MIN_LEN}",
    ),
}


@dataclass
class Report:
    load: LoadStats = field(default_factory=LoadStats)
    checkpoints: int = 0
    #: smallest/largest result size seen per view, to show views were exercised
    sizes: dict[str, tuple[int, int]] = field(default_factory=dict)
    changes: Counter[str] = field(default_factory=Counter)
    counters: Counter[str] = field(default_factory=Counter)


def test_filter_and_map_views_equal_sql_at_every_frontier(dsn: str, workload_seed: int) -> None:
    report = run(_scenario(dsn, workload_seed))
    print(
        f"load: committed={report.load.committed} rolled_back={report.load.rolled_back} "
        f"statements={sum(report.load.statements.values())} {dict(report.load.statements)}"
    )
    print(f"checkpoints compared: {report.checkpoints}; engine: {dict(report.counters)}")
    for name in CASES:
        lo, hi = report.sizes[name]
        print(f"  {name:28} rows {lo}..{hi}, {report.changes[name]} frontier advances with changes")

    # The comparisons only mean something if the hard cases happened.
    assert report.checkpoints >= LOCKSTEP_TXNS
    assert report.load.rolled_back > 0
    assert report.load.statements[OpKind.REKEY_ORDER] > 0
    assert report.load.statements[OpKind.DELETE_CUSTOMER] > 0
    assert report.counters["unchanged_toast_resolved"] > 0
    for name in CASES:
        lo, hi = report.sizes[name]
        assert hi > 0, f"{name} was never non-empty"
        assert report.changes[name] > 10, f"{name} barely changed"


async def _scenario(dsn: str, seed: int) -> Report:
    report = Report()
    context = f"seed={seed} (replay: DELTAFLOW_SEED={seed} make differential)"
    await seed_database(dsn, seed=seed, customers=200, orders=1_000)

    def counting(name: str) -> OnChange:
        def on_change(frontier: Frontier, batch: list[Delta[Row]]) -> None:
            report.changes[name] += 1

        return on_change

    df = Deltaflow()
    sinks: dict[str, MemorySink] = {}
    for name, case in CASES.items():
        sinks[name] = MemorySink(on_change=counting(name))
        case.register(df, sinks[name])

    async def compare(sql: Sql, frontier: Frontier, where: str) -> None:
        for name, case in CASES.items():
            sink = sinks[name]
            assert sink.frontier == frontier, f"{name} frontier {sink.frontier} != {frontier}"
            expected = await query_multiset(sql, case.sql)
            if sink.result != expected:
                raise AssertionError(
                    f"{name} != `{case.sql}` {where}: "
                    f"{first_difference(sink.result, expected)}; {context}"
                )
            lo, hi = report.sizes.get(name, (expected.size(), expected.size()))
            report.sizes[name] = (min(lo, expected.size()), max(hi, expected.size()))
        report.checkpoints += 1

    async with (
        asyncio.timeout(TIMEOUT_S),
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql,
        Runner(df, dsn, publication="deltaflow_diff_views", status_interval=1) as runner,
    ):
        await compare(sql, runner.engine.frontier, "after the snapshot")
        workload = Workload(seed, await load_state(sql))

        # Phase 1: lockstep, compare after every transaction.
        for i in range(LOCKSTEP_TXNS):
            committed = await execute(sql, workload, report.load, marker=str(i))
            if committed:
                txn = await until_marker(runner, str(i))
                await compare(sql, Frontier.at(txn.end_lsn), f"after transaction #{i}")
            else:
                await compare(sql, runner.engine.frontier, f"after rolled-back #{i}")

        # Phase 2: load and consumption concurrently.
        async def load() -> None:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                await run_load(conn, workload, txns=BURST_TXNS, stats=report.load)
                await conn.execute("SELECT pg_logical_emit_message(true, %s, 'end')", (MARKER,))

        _, last = await asyncio.gather(load(), until_marker(runner, "end"))
        await compare(sql, Frontier.at(last.end_lsn), "after the concurrent burst")

        assert await load_state(sql) == workload.state, f"database != workload model; {context}"
        report.counters.update(runner.engine.counters)
        for arrangement in runner.engine.arrangements.values():
            report.counters.update(arrangement.counters)
    return report
