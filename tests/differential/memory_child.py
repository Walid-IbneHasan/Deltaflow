"""Child process for test_memory_budget: run a join view with a memory
budget, follow the stream to the end marker, compare with SQL, and report
memory as one JSON line on stdout.

    python -m tests.differential.memory_child DSN BUDGET_BYTES

Running in its own process means the OS peak-RSS counter covers exactly the
engine's lifetime. The test runner's own memory (Hypothesis, the seeding of
a large dataset) is not part of the measurement.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

import psycopg

from deltaflow.aio import run
from deltaflow.observability.memory import rss
from deltaflow.planner import Deltaflow
from deltaflow.replication.wire import ReplicationConnection
from deltaflow.runtime import Runner
from deltaflow.sinks import MemorySink
from tests.differential.lockstep import until_marker
from tests.sql_oracle import first_difference, query_multiset

PUBLICATION = "deltaflow_diff_memory"
VIEW_SQL = (
    'SELECT o.id AS "orders.id", c.country FROM orders o JOIN customers c ON c.id = o.customer_id'
    " WHERE o.amount > 4500"
)


async def main(dsn: str, budget: int) -> dict[str, object]:
    df = Deltaflow()
    sink = MemorySink()
    (
        df.view("big_orders_by_country")
        .source("public.orders")
        .filter(lambda r: r["amount"] > 4500)
        .join("public.customers", on=("customer_id", "id"))
        .select("orders.id", "country")
        .sink(sink)
    )
    # The process's own floor: everything imported, and one connection of
    # each kind opened and closed. The first connection initialises a TLS
    # library, once per process and independent of any state: libpq's
    # OpenSSL for psycopg, Python's for the replication connection (about
    # 6 MiB together, measured; nothing more for later connections).
    await (await psycopg.AsyncConnection.connect(dsn)).close()
    await (await ReplicationConnection.connect(dsn)).close()
    baseline = rss().current
    started = time.perf_counter()
    async with Runner(
        df, dsn, publication=PUBLICATION, memory_budget=budget, status_interval=1
    ) as runner:
        snapshot_s = time.perf_counter() - started
        after_snapshot = rss()
        print("ready", flush=True)
        await until_marker(runner, "end")
        store = runner.store.stats()
        transactions = runner.engine.counters["transactions"]
        chunks = runner.engine.counters["snapshot_chunks"]
        rows = {t: len(a) for t, a in runner.engine.arrangements.items()}
    final = rss()
    async with await psycopg.AsyncConnection.connect(dsn) as sql:
        expected = await query_multiset(sql, VIEW_SQL)
    return {
        "budget": budget,
        "baseline_rss": baseline,
        "after_snapshot_rss": after_snapshot.current,
        "peak_rss": final.peak,
        "snapshot_seconds": round(snapshot_s, 1),
        "snapshot_chunks": chunks,
        "transactions": transactions,
        "source_rows": rows,
        "sst_bytes": store.sst_bytes,
        "cache_usage": store.cache_usage,
        "view_rows": sink.result.size(),
        "matches_sql": sink.result == expected,
        "difference": "" if sink.result == expected else first_difference(sink.result, expected),
    }


if __name__ == "__main__":
    dsn, budget = sys.argv[1], int(sys.argv[2])
    report = run(asyncio.wait_for(main(dsn, budget), 1800))
    print(json.dumps(report), flush=True)
