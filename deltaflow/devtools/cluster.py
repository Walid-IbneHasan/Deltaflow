"""A cluster worker: the seed views, sharded, shared with other workers.

    python -m deltaflow.devtools.cluster --worker-id w1 [--shards 6]
    python -m deltaflow.devtools.cluster --reset          # drop slots, state, leases

Start several (with different --worker-id). They divide the shards among
themselves through Postgres (runtime/cluster.py), and re-divide them when
one is killed or a new one joins. Results are exactly-once Postgres views
in the `analytics_cluster` schema. Each is a merge view over a partial
table (`<view>__shards`, one partition of rows per shard), and VIEW_SQL
holds its plain-SQL definition.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import os
import signal
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import psycopg

from deltaflow import aio
from deltaflow.config import pg_dsn
from deltaflow.core import Row
from deltaflow.observability import logs
from deltaflow.planner import Count, Deltaflow, Max, Min, Sum
from deltaflow.runtime.cluster import CLUSTER_DDL, ClusterWorker, prepare_cluster
from deltaflow.sinks import PostgresSink
from deltaflow.sinks.postgres import ensure_watermark_table

SCHEMA = "analytics_cluster"
SHARD_BY = {"public.orders": "customer_id", "public.customers": "id"}
PARTIAL_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};
CREATE TABLE IF NOT EXISTS {SCHEMA}.orders_per_country__shards (
    shard int, country text, orders bigint NOT NULL, revenue numeric NOT NULL,
    PRIMARY KEY (shard, country));
CREATE TABLE IF NOT EXISTS {SCHEMA}.order_details__shards (
    shard int, order_id bigint, customer_id bigint NOT NULL, status text NOT NULL,
    amount numeric NOT NULL, customer_name text NOT NULL, country text NOT NULL,
    PRIMARY KEY (shard, order_id));
CREATE TABLE IF NOT EXISTS {SCHEMA}.order_totals__shards (
    shard int PRIMARY KEY, orders bigint NOT NULL, revenue numeric, smallest numeric,
    largest numeric);
CREATE TABLE IF NOT EXISTS {SCHEMA}.customer_statuses__shards (
    shard int, customer_id bigint, status text, PRIMARY KEY (shard, customer_id, status));
"""

VIEW_SQL: dict[str, str] = {
    f"{SCHEMA}.orders_per_country": (
        "SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue"
        " FROM orders o JOIN customers c ON c.id = o.customer_id"
        " WHERE o.status <> 'cancelled' GROUP BY c.country"
    ),
    f"{SCHEMA}.order_details": (
        "SELECT o.id AS order_id, o.customer_id, o.status, o.amount,"
        " c.name AS customer_name, c.country FROM orders o JOIN customers c ON c.id = o.customer_id"
    ),
    f"{SCHEMA}.order_totals": (
        "SELECT count(*) AS orders, sum(amount) AS revenue, min(amount) AS smallest,"
        " max(amount) AS largest FROM orders"
    ),
    f"{SCHEMA}.customer_statuses": "SELECT DISTINCT customer_id, status FROM orders",
}
SETUP_LOCK = 0x0DE1F11


def not_cancelled(r: Row) -> bool:
    return bool(r["status"] != "cancelled")


def order_details(r: Row) -> Mapping[str, Any]:
    return {
        "order_id": r["orders.id"],
        "customer_id": r["customer_id"],
        "status": r["status"],
        "amount": r["amount"],
        "customer_name": r["name"],
        "country": r["country"],
    }


def views(shards: int) -> Deltaflow:
    df = Deltaflow(shards=shards, shard_by=SHARD_BY)
    (
        df.view("orders_per_country")
        .source("public.orders")
        .filter(not_cancelled)
        .join("public.customers", on=("customer_id", "id"))
        .group_by(country="customers.country")
        .agg(orders=Count(), revenue=Sum("orders.amount"))
        .sink(PostgresSink(f"{SCHEMA}.orders_per_country"))
    )
    (
        df.view("order_details")
        .source("public.orders")
        .join("public.customers", on=("customer_id", "id"))
        .map(order_details)
        .sink(PostgresSink(f"{SCHEMA}.order_details"))
    )
    (
        df.view("order_totals")
        .source("public.orders")
        .agg(orders=Count(), revenue=Sum("amount"), smallest=Min("amount"), largest=Max("amount"))
        .sink(PostgresSink(f"{SCHEMA}.order_totals"))
    )
    (
        df.view("customer_statuses")
        .source("public.orders")
        .select("customer_id", "status")
        .distinct()
        .sink(PostgresSink(f"{SCHEMA}.customer_statuses"))
    )
    return df


async def setup(dsn: str, shards: int, publication: str) -> None:
    """Partial tables, then what every sharded registry needs (merge views,
    publication, coordination tables): once, whoever gets there first."""
    async with (
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn,
        conn.transaction(),
    ):
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (SETUP_LOCK,))
        await conn.execute(PARTIAL_DDL)
    await prepare_cluster(dsn, views(shards), publication)


async def _main(args: argparse.Namespace) -> None:
    await setup(args.dsn, args.shards, args.publication)
    worker = ClusterWorker(
        lambda: views(args.shards),
        args.dsn,
        worker_id=args.worker_id,
        state_root=Path(args.state_root),
        slot_prefix=args.slot_prefix,
        publication=args.publication,
        heartbeat=args.heartbeat,
        ttl=args.ttl,
        runner_options={
            "flush_interval": args.flush_interval,
            "checkpoint_interval": args.checkpoint_interval,
            "memory_budget": args.memory_budget << 20,
            "status_interval": 2,
        },
    )
    stop = asyncio.Event()
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
    print(f"cluster worker {args.worker_id} pid={os.getpid()} shards={args.shards}", flush=True)
    await worker.run(stop)


async def _reset(args: argparse.Namespace) -> None:
    import shutil  # noqa: PLC0415

    async with await psycopg.AsyncConnection.connect(args.dsn, autocommit=True) as conn:
        await conn.execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots"
            " WHERE slot_name LIKE %s AND active_pid IS NOT NULL",
            (f"{args.slot_prefix}_s%",),
        )
        await asyncio.sleep(0.5)
        await conn.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
            " WHERE slot_name LIKE %s",
            (f"{args.slot_prefix}_s%",),
        )
        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await conn.execute(CLUSTER_DDL)
        await ensure_watermark_table(conn)
        await conn.execute("DELETE FROM _deltaflow.workers")
        await conn.execute("DELETE FROM _deltaflow.shard_leases")
        await conn.execute("DELETE FROM _deltaflow.watermarks WHERE view LIKE '%@%'")
    shutil.rmtree(args.state_root, ignore_errors=True)
    print(f"reset: slots {args.slot_prefix}_s*, schema {SCHEMA}, leases, state {args.state_root}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=pg_dsn())
    parser.add_argument("--worker-id", default=f"w{os.getpid()}")
    parser.add_argument("--shards", type=int, default=6)
    parser.add_argument("--state-root", default=".deltaflow/cluster")
    parser.add_argument("--slot-prefix", default="deltaflow_cluster")
    parser.add_argument("--publication", default="deltaflow_cluster")
    parser.add_argument("--heartbeat", type=float, default=0.5, help="seconds")
    # Must exceed the slowest checkpoint: rocksdict holds the GIL during one,
    # freezing heartbeats (docs/operations.md). The tests use 3 s on small state.
    parser.add_argument("--ttl", type=float, default=10.0, help="seconds without heartbeat = dead")
    parser.add_argument("--flush-interval", type=float, default=0.1)
    parser.add_argument("--checkpoint-interval", type=float, default=5.0)
    parser.add_argument("--memory-budget", type=int, default=32, help="MiB per shard")
    parser.add_argument("--log-format", choices=("json", "text"), default="json")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args(argv)
    logs.configure(args.log_format)
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(errors="backslashreplace")
    with contextlib.suppress(KeyboardInterrupt):
        aio.run(_reset(args) if args.reset else _main(args))


if __name__ == "__main__":
    main()
