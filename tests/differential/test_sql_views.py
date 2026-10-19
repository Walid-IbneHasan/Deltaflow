"""Views written in SQL equal the same SQL run by Postgres, after every commit.

Each view below is registered from its SQL text alone (`Deltaflow.sql`),
maintained through the real snapshot and replication path, and compared,
after every committed and every rolled-back transaction, with Postgres
running the very same SELECT. Nothing is restated by hand: the view
definition is the oracle.

They cover what the SQL front end translates:
- joins, inner and left, with aggregates over a left join (count(column)
  skipping the padded NULLs);
- WHERE with BETWEEN, IN, LIKE, IS NULL, NOT and typed timestamp literals;
- GROUP BY expressions, HAVING, and expressions over aggregates;
- count(*), count(x), count(DISTINCT x), sum, min, max and avg, also of
  expressions, and min over text with COLLATE "C";
- numeric division and rounding;
- CASE and COALESCE;
- DISTINCT, and a global aggregate.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field

import psycopg

from deltaflow.aio import run
from deltaflow.core import Frontier
from deltaflow.devtools.loadgen import LoadStats, load_state
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import Workload
from deltaflow.planner import Deltaflow
from deltaflow.runtime import Runner
from deltaflow.sinks import MemorySink
from tests.differential.lockstep import Sql, execute, until_marker
from tests.differential.test_join_views import STEADY
from tests.sql_oracle import first_difference, query_multiset

#: DELTAFLOW_SQL_VIEW_OPS overrides it for a quick local run, or a soak.
TARGET_OPS = int(os.environ.get("DELTAFLOW_SQL_VIEW_OPS", "20000"))
TIMEOUT_S = 3600

VIEWS: dict[str, str] = {
    "revenue_by_country": """
        SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue, avg(o.amount) AS mean
        FROM orders o JOIN customers c ON c.id = o.customer_id
        WHERE o.status <> 'cancelled'
        GROUP BY c.country""",
    "status_summary": """
        SELECT status, count(*) AS n, count(DISTINCT customer_id) AS customers,
               min(amount) AS lo, max(amount) AS hi
        FROM orders GROUP BY status HAVING count(*) > 2""",
    "priced_orders": """
        SELECT o.id, o.amount * 2 AS doubled, round(o.amount / 3, 2) AS third,
               CASE WHEN o.amount > 1000 THEN 'big' ELSE 'small' END AS size,
               COALESCE(o.note, 'none') AS note
        FROM orders o
        WHERE o.amount BETWEEN 100 AND 5000 AND o.status IN ('paid', 'shipped')""",
    "customers_and_orders": """
        SELECT c.id AS customer, o.id AS order_id, o.status
        FROM customers c LEFT JOIN orders o ON o.customer_id = c.id""",
    "spend_per_customer": """
        SELECT c.id, count(o.id) AS orders, sum(o.amount) AS spent, max(o.created_at) AS latest
        FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
        GROUP BY c.id""",
    "name_buckets": """
        SELECT length(name) % 5 AS bucket, count(*) AS n, min(name COLLATE "C") AS first,
               count(email) AS emails
        FROM customers GROUP BY length(name) % 5""",
    "per_status_ratios": """
        SELECT status, sum(amount) / count(*) AS per_order, count(*) * 2 AS twice,
               avg(amount * 2) AS mean_doubled
        FROM orders GROUP BY status""",
    "countries_with_orders": """
        SELECT DISTINCT c.country
        FROM orders o JOIN customers c ON c.id = o.customer_id""",
    "reachable_customers": """
        SELECT id, email FROM customers
        WHERE email LIKE '%@%' AND NOT (country = 'NL') AND bio IS NOT NULL""",
    "recent_orders": """
        SELECT id, status FROM orders
        WHERE created_at >= timestamptz '2020-01-01 00:00+00'
          AND amount > 50""",
    # HAVING that groups cross both ways under load: customers gain and lose
    # their fifth order
    "frequent_customers": """
        SELECT customer_id, count(*) AS n, sum(amount) AS spent
        FROM orders GROUP BY customer_id HAVING count(*) >= 5""",
    "totals": """
        SELECT count(*) AS n, sum(amount) AS total, avg(amount) AS mean, min(amount) AS lo
        FROM orders""",
}


@dataclass
class Report:
    load: LoadStats = field(default_factory=LoadStats)
    checkpoints: int = 0
    sizes: dict[str, tuple[int, int]] = field(default_factory=dict)


def test_sql_views_equal_postgres_after_every_commit(dsn: str, workload_seed: int) -> None:
    report = run(_scenario(dsn, workload_seed))
    total = sum(report.load.statements.values())
    print(f"load: committed={report.load.committed} rolled_back={report.load.rolled_back} "
          f"statements={total}; checkpoints compared: {report.checkpoints}")  # fmt: skip
    for name, (lo, hi) in report.sizes.items():
        print(f"  {name:24} rows {lo}..{hi}")
    assert total >= TARGET_OPS
    assert report.checkpoints >= report.load.committed + report.load.rolled_back
    for name, (_, hi) in report.sizes.items():
        assert hi > 0, f"{name} was never non-empty: the view tests nothing"


async def _scenario(dsn: str, seed: int) -> Report:
    report = Report()
    context = f"seed={seed} (replay: DELTAFLOW_SEED={seed})"
    await seed_database(dsn, seed=seed, customers=300, orders=1_500)
    df = Deltaflow()
    sinks = {name: MemorySink() for name in VIEWS}
    for name, sql in VIEWS.items():
        df.sql(sql, name=name, sink=sinks[name])

    async def compare(sql: Sql, frontier: Frontier, where: str) -> None:
        for name, query in VIEWS.items():
            sink = sinks[name]
            assert sink.frontier == frontier, f"{name} frontier {sink.frontier} != {frontier}"
            expected = await query_multiset(sql, query)
            if sink.result != expected:
                raise AssertionError(
                    f"{name} != Postgres {where}: {first_difference(sink.result, expected)}; "
                    f"{context}"
                )
            lo, hi = report.sizes.get(name, (expected.size(), expected.size()))
            report.sizes[name] = (min(lo, expected.size()), max(hi, expected.size()))
        report.checkpoints += 1

    async with (
        asyncio.timeout(TIMEOUT_S),
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn,
        Runner(df, dsn, publication="deltaflow_diff_sql", status_interval=1) as runner,
    ):
        await compare(conn, runner.engine.frontier, "after the snapshot")
        workload = Workload(seed, await load_state(conn), STEADY)
        i = 0
        while sum(report.load.statements.values()) < TARGET_OPS:
            if await execute(conn, workload, report.load, marker=str(i)):
                txn = await until_marker(runner, str(i))
                await compare(conn, Frontier.at(txn.end_lsn), f"after transaction #{i}")
            else:
                await compare(conn, runner.engine.frontier, f"after rolled-back #{i}")
            i += 1
    return report
