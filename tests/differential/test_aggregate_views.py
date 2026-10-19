"""M4 acceptance: every aggregate equals its SQL after every commit, including
under delete-heavy workloads.

Eight views cover:
- the spec's example (join, filter, group_by country, count, sum);
- min/max over numbers, timestamps and text;
- count(column) and sum over a nullable value;
- a global aggregate;
- distinct, before and after a join;
- an aggregate of an aggregate.

The workload runs in phases:

    steady -> drain -> refill -> drain -> refill

*Drain* is delete-heavy, running until almost every order and most
customers are gone, so groups shrink to nothing and disappear and the
global aggregate falls back to (0, NULL...). During the drains the test
also deletes, on purpose, the row holding a group's current minimum or
maximum: the order with a status's extreme amount, or the customer with a
country's extreme name. That is the retraction min/max must survive. Every
view is compared with its SQL, as a multiset, after every commit.

Text min/max is compared with `COLLATE "C"`: Deltaflow orders text by code
point, which is what the C collation does; the database default here is
en_US.utf8, which orders differently.
"""

from __future__ import annotations

import asyncio
import random
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.core import Frontier, Row
from deltaflow.devtools.loadgen import LoadStats, load_state
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import COUNTRIES, STATUSES, OpKind, Workload, WorkloadConfig
from deltaflow.planner import Avg, Count, CountDistinct, Deltaflow, Max, Min, Sum
from deltaflow.runtime import Runner
from deltaflow.sinks import MemorySink
from tests.differential.lockstep import MARKER, Sql, execute, until_marker
from tests.sql_oracle import first_difference, query_multiset

TIMEOUT_S = 3600
STEADY_OPS = 4_000
MAX_PHASE_OPS = 20_000


def _weights(**w: int) -> dict[OpKind, int]:
    return {OpKind(k): v for k, v in w.items()}


STEADY = WorkloadConfig(max_statements_per_txn=12)
DRAIN = WorkloadConfig(
    max_statements_per_txn=12,
    weights=_weights(
        insert_customer=1, update_customer=5, delete_customer=10,
        insert_order=5, update_order=10, rekey_order=2, delete_order=40,
    ),
)  # fmt: skip
REFILL = WorkloadConfig(
    max_statements_per_txn=12,
    weights=_weights(
        insert_customer=10, update_customer=5, delete_customer=1,
        insert_order=40, update_order=10, rekey_order=1, delete_order=5,
    ),
)  # fmt: skip


def not_cancelled(r: Row) -> bool:
    return bool(r["status"] != "cancelled")


def with_note_length(r: Row) -> Mapping[str, Any]:
    note = r["note"]
    return {
        "customer_id": r["customer_id"],
        "created_at": r["created_at"],
        "note": note,
        "note_len": None if note is None else len(note),
    }


@dataclass(frozen=True)
class Case:
    register: Callable[[Deltaflow, MemorySink], object]
    sql: str


CASES: dict[str, Case] = {
    # The spec's example view.
    "orders_per_customer": Case(
        lambda df, s: (
            df.view("orders_per_customer")
            .source("public.orders")
            .join("public.customers", on=("customer_id", "id"))
            .filter(not_cancelled)
            .group_by("customers.country")
            .agg(count=Count(), revenue=Sum("orders.amount"))
            .sink(s)
        ),
        'SELECT c.country AS "customers.country", count(*) AS count, sum(o.amount) AS revenue'
        " FROM orders o JOIN customers c ON c.id = o.customer_id"
        " WHERE o.status <> 'cancelled' GROUP BY c.country",
    ),
    "status_extremes": Case(
        lambda df, s: (
            df.view("status_extremes")
            .source("public.orders")
            .group_by("status")
            .agg(n=Count(), total=Sum("amount"), lo=Min("amount"), hi=Max("amount"))
            .sink(s)
        ),
        "SELECT status, count(*) AS n, sum(amount) AS total, min(amount) AS lo,"
        " max(amount) AS hi FROM orders GROUP BY status",
    ),
    # Timestamps, count(column) and a sum over a nullable value.
    "customer_activity": Case(
        lambda df, s: (
            df.view("customer_activity")
            .source("public.orders")
            .map(with_note_length)
            .group_by("customer_id")
            .agg(
                first=Min("created_at"),
                last=Max("created_at"),
                noted=Count("note"),
                note_chars=Sum("note_len"),
                avg_note=Avg("note_len"),
            )
            .sink(s)
        ),
        "SELECT customer_id, min(created_at) AS first, max(created_at) AS last,"
        " count(note) AS noted, sum(length(note)) AS note_chars, avg(length(note)) AS avg_note"
        " FROM orders GROUP BY customer_id",
    ),
    # Text min/max (code-point order) and a nullable text max.
    "country_names": Case(
        lambda df, s: (
            df.view("country_names")
            .source("public.customers")
            .group_by("country")
            .agg(
                n=Count(),
                emails=Count("email"),
                first=Min("name"),
                last=Max("name"),
                top_email=Max("email"),
            )
            .sink(s)
        ),
        'SELECT country, count(*) AS n, count(email) AS emails, min(name COLLATE "C") AS first,'
        ' max(name COLLATE "C") AS last, max(email COLLATE "C") AS top_email'
        " FROM customers GROUP BY country",
    ),
    # avg as Postgres computes it (its numeric scale included), and
    # count(DISTINCT ...) over a key and over a nullable text.
    "status_averages": Case(
        lambda df, s: (
            df.view("status_averages")
            .source("public.orders")
            .group_by("status")
            .agg(mean=Avg("amount"), customers=CountDistinct("customer_id"))
            .sink(s)
        ),
        "SELECT status, avg(amount) AS mean, count(DISTINCT customer_id) AS customers"
        " FROM orders GROUP BY status",
    ),
    "distinct_emails": Case(
        lambda df, s: (
            df.view("distinct_emails")
            .source("public.customers")
            .agg(emails=CountDistinct("email"), countries=CountDistinct("country"))
            .sink(s)
        ),
        "SELECT count(DISTINCT email) AS emails, count(DISTINCT country) AS countries"
        " FROM customers",
    ),
    "totals": Case(
        lambda df, s: (
            df.view("totals")
            .source("public.orders")
            .agg(n=Count(), total=Sum("amount"), lo=Min("amount"), hi=Max("amount"))
            .sink(s)
        ),
        "SELECT count(*) AS n, sum(amount) AS total, min(amount) AS lo, max(amount) AS hi"
        " FROM orders",
    ),
    "customer_statuses": Case(
        lambda df, s: (
            df.view("customer_statuses")
            .source("public.orders")
            .select("customer_id", "status")
            .distinct()
            .sink(s)
        ),
        "SELECT DISTINCT customer_id, status FROM orders",
    ),
    "countries_with_orders": Case(
        lambda df, s: (
            df.view("countries_with_orders")
            .source("public.orders")
            .join("public.customers", on=("customer_id", "id"))
            .select("country")
            .distinct()
            .sink(s)
        ),
        "SELECT DISTINCT c.country FROM orders o JOIN customers c ON c.id = o.customer_id",
    ),
    # How many countries have n customers: an aggregate of an aggregate.
    "country_size_histogram": Case(
        lambda df, s: (
            df.view("country_size_histogram")
            .source("public.customers")
            .group_by("country")
            .agg(n=Count())
            .group_by("n")
            .agg(countries=Count())
            .sink(s)
        ),
        "SELECT n, count(*) AS countries"
        " FROM (SELECT country, count(*) AS n FROM customers GROUP BY country) g GROUP BY n",
    ),
}


@dataclass
class Report:
    load: LoadStats = field(default_factory=LoadStats)
    checkpoints: int = 0
    sizes: dict[str, tuple[int, int]] = field(default_factory=dict)
    phases: list[str] = field(default_factory=list)
    extreme_deletes: Counter[str] = field(default_factory=Counter)
    smallest_tables: tuple[int, int] = (10**9, 10**9)
    counters: Counter[str] = field(default_factory=Counter)


def test_aggregates_equal_sql_after_every_commit_with_heavy_deletes(
    dsn: str, workload_seed: int
) -> None:
    report = run(_scenario(dsn, workload_seed))
    print(
        f"load: committed={report.load.committed} rolled_back={report.load.rolled_back} "
        f"statements={sum(report.load.statements.values())} {dict(report.load.statements)}"
    )
    print(f"phases: {' -> '.join(report.phases)}")
    print(
        f"checkpoints compared: {report.checkpoints}; targeted extreme deletes: "
        f"{dict(report.extreme_deletes)}; smallest (customers, orders): {report.smallest_tables}"
    )
    for name, (lo, hi) in report.sizes.items():
        print(f"  {name:24} rows {lo}..{hi}")

    customers, orders = report.smallest_tables
    assert orders <= 5, "the drain did not empty the orders table"
    assert customers <= 10, "the drain did not empty the customers table"
    for target in ("order min", "order max", "customer min", "customer max"):
        assert report.extreme_deletes[target] > 20, target
    assert report.load.statements[OpKind.DELETE_CUSTOMER] > 100
    # Groups really vanished and came back.
    assert report.sizes["status_extremes"][0] < len(STATUSES)
    assert report.sizes["country_names"][0] < len(COUNTRIES)
    assert report.sizes["customer_activity"][0] < 10 < report.sizes["customer_activity"][1]
    assert report.sizes["totals"] == (1, 1)  # one row always, empty or not


async def _scenario(dsn: str, seed: int) -> Report:  # noqa: PLR0915 - one scenario, read top to bottom
    report = Report()
    context = f"seed={seed} (replay: DELTAFLOW_SEED={seed} make differential)"
    rng = random.Random(seed)
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
            lo, hi = report.sizes.get(name, (len(expected), len(expected)))
            report.sizes[name] = (min(lo, len(expected)), max(hi, len(expected)))
        report.checkpoints += 1

    async with (
        asyncio.timeout(TIMEOUT_S),
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql,
        Runner(df, dsn, publication="deltaflow_diff_agg", status_interval=1) as runner,
    ):
        await compare(sql, runner.engine.frontier, "after the snapshot")
        n = 0
        last: list[Workload] = []

        async def phase(
            name: str, config: WorkloadConfig, done: Callable[[Workload], bool]
        ) -> None:
            nonlocal n
            workload = Workload(seed + len(report.phases), await load_state(sql), config)
            last[:] = [workload]
            start = sum(report.load.statements.values())
            while (
                not done(workload) and sum(report.load.statements.values()) - start < MAX_PHASE_OPS
            ):
                if await execute(sql, workload, report.load, marker=str(n)):
                    txn = await until_marker(runner, str(n))
                    await compare(sql, Frontier.at(txn.end_lsn), f"after transaction #{n} ({name})")
                else:
                    await compare(sql, runner.engine.frontier, f"after rolled-back #{n} ({name})")
                n += 1
                state = workload.state
                report.smallest_tables = (
                    min(report.smallest_tables[0], len(state.customers)),
                    min(report.smallest_tables[1], len(state.orders)),
                )
                if name == "drain" and rng.random() < 0.3:
                    await delete_extreme(workload, name)
            report.phases.append(
                f"{name} ({sum(report.load.statements.values()) - start} ops, "
                f"{len(workload.state.customers)} customers, {len(workload.state.orders)} orders)"
            )

        async def delete_extreme(workload: Workload, name: str) -> None:
            """Delete the row holding a group's current min or max: the order
            with a status's extreme amount, or the customer with a country's
            extreme name (cascading to its orders)."""
            nonlocal n
            which = rng.choice(("min", "max"))
            order = "ASC" if which == "min" else "DESC"
            if rng.random() < 0.5:
                kind, table, column, group = "order", "orders", "amount", "status"
                value: str = rng.choice(STATUSES)
            else:
                kind, table, column, group = "customer", "customers", 'name COLLATE "C"', "country"
                value = rng.choice(COUNTRIES)
            async with sql.transaction(), sql.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {table} WHERE id = (SELECT id FROM {table} WHERE {group} = %s"
                    f" ORDER BY {column} {order}, id LIMIT 1) RETURNING id",
                    (value,),
                )
                deleted = await cur.fetchone()
                await cur.execute("SELECT pg_logical_emit_message(true, %s, %s)", (MARKER, str(n)))
            if deleted is not None:  # else: that group is empty right now
                victim = int(str(deleted[0]))
                state = workload.state
                if kind == "order":
                    del state.orders[victim]
                else:
                    del state.customers[victim]
                    for oid in [o.id for o in state.orders.values() if o.customer_id == victim]:
                        del state.orders[oid]
                report.extreme_deletes[f"{kind} {which}"] += 1
            # The marker commits either way, so it is consumed either way.
            txn = await until_marker(runner, str(n))
            await compare(sql, Frontier.at(txn.end_lsn), f"after deleting a {which} ({name})")
            n += 1

        await phase("steady", STEADY, lambda w: sum(report.load.statements.values()) >= STEADY_OPS)
        for _ in range(2):
            await phase(
                "drain", DRAIN, lambda w: len(w.state.orders) <= 3 and len(w.state.customers) <= 10
            )
            await phase("refill", REFILL, lambda w: len(w.state.orders) >= 1_000)
        assert await load_state(sql) == last[0].state, f"database != workload model; {context}"
        report.counters.update(runner.engine.counters)
    return report
