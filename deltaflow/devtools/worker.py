"""A durable Deltaflow worker over the seed tables, writing to Postgres.

    python -m deltaflow.devtools.worker [--state-dir DIR] [--slot NAME] [--reset]

Maintains four views in the `analytics` schema, exactly once, surviving
`kill -9`:

- orders_per_customer: the spec's example (join, filter, group_by country,
  count, sum);
- order_details: orders joined with their customer, one row per order;
- order_totals: a global aggregate, always one row;
- customer_statuses: the distinct (customer, status) pairs.

The sink tables are created if missing. `VIEW_SQL` holds each view's
definition as plain SQL, which is what the chaos suite compares against.
Stop with Ctrl-C (a clean stop checkpoints); or kill it, and the next start
resumes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import logging
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
from deltaflow.planner import Count, Deltaflow, Max, Min, Sum
from deltaflow.runtime import DurableRunner
from deltaflow.sinks import PostgresSink

SINK_DDL = """
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS analytics.orders_per_customer (
    country text PRIMARY KEY, orders bigint NOT NULL, revenue numeric NOT NULL);
CREATE TABLE IF NOT EXISTS analytics.order_details (
    order_id bigint PRIMARY KEY, customer_id bigint NOT NULL, status text NOT NULL,
    amount numeric NOT NULL, customer_name text NOT NULL, country text NOT NULL);
CREATE TABLE IF NOT EXISTS analytics.order_totals (
    orders bigint NOT NULL, revenue numeric, smallest numeric, largest numeric);
CREATE TABLE IF NOT EXISTS analytics.customer_statuses (
    customer_id bigint, status text, PRIMARY KEY (customer_id, status));
"""

VIEW_SQL: dict[str, str] = {
    "analytics.orders_per_customer": (
        "SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue"
        " FROM orders o JOIN customers c ON c.id = o.customer_id"
        " WHERE o.status <> 'cancelled' GROUP BY c.country"
    ),
    "analytics.order_details": (
        "SELECT o.id AS order_id, o.customer_id, o.status, o.amount,"
        " c.name AS customer_name, c.country FROM orders o JOIN customers c ON c.id = o.customer_id"
    ),
    "analytics.order_totals": (
        "SELECT count(*) AS orders, sum(amount) AS revenue, min(amount) AS smallest,"
        " max(amount) AS largest FROM orders"
    ),
    "analytics.customer_statuses": "SELECT DISTINCT customer_id, status FROM orders",
}


def not_cancelled(r: Row) -> bool:
    return bool(r["status"] != "cancelled")


def country_totals(r: Row) -> Mapping[str, Any]:
    return {"country": r["customers.country"], "orders": r["orders"], "revenue": r["revenue"]}


def order_details(r: Row) -> Mapping[str, Any]:
    return {
        "order_id": r["orders.id"],
        "customer_id": r["customer_id"],
        "status": r["status"],
        "amount": r["amount"],
        "customer_name": r["name"],
        "country": r["country"],
    }


def views() -> Deltaflow:
    df = Deltaflow()
    (
        df.view("orders_per_customer")
        .source("public.orders")
        .join("public.customers", on=("customer_id", "id"))
        .filter(not_cancelled)
        .group_by("customers.country")
        .agg(orders=Count(), revenue=Sum("orders.amount"))
        .map(country_totals)
        .sink(PostgresSink("analytics.orders_per_customer"))
    )
    (
        df.view("order_details")
        .source("public.orders")
        .join("public.customers", on=("customer_id", "id"))
        .map(order_details)
        .sink(PostgresSink("analytics.order_details"))
    )
    (
        df.view("order_totals")
        .source("public.orders")
        .agg(orders=Count(), revenue=Sum("amount"), smallest=Min("amount"), largest=Max("amount"))
        .sink(PostgresSink("analytics.order_totals"))
    )
    (
        df.view("customer_statuses")
        .source("public.orders")
        .select("customer_id", "status")
        .distinct()
        .sink(PostgresSink("analytics.customer_statuses"))
    )
    return df


async def _main(args: argparse.Namespace) -> None:
    async with await psycopg.AsyncConnection.connect(args.dsn, autocommit=True) as conn:
        await conn.execute(SINK_DDL)
    runner = DurableRunner(
        views(),
        args.dsn,
        state_dir=Path(args.state_dir),
        slot=args.slot,
        publication=args.publication,
        memory_budget=args.memory_budget << 20,
        flush_interval=args.flush_interval,
        checkpoint_interval=args.checkpoint_interval,
        status_interval=2,
    )
    stop = asyncio.Event()
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
    await runner.start()
    how = "resumed" if runner.counters["resumes"] else "bootstrapped"
    print(
        f"worker pid={os.getpid()} {how}; checkpoint {runner.checkpointed_lsn}, "
        f"sinks written through {runner.flushed_lsn}",
        flush=True,
    )
    try:
        await runner.run(stop)
    finally:
        await runner.close()
        print(f"stopped cleanly: {dict(runner.counters)}", flush=True)


async def _reset(args: argparse.Namespace) -> None:
    from deltaflow.replication.slot import drop_slot, slot_exists  # noqa: PLC0415
    from deltaflow.replication.wire import ReplicationConnection  # noqa: PLC0415
    from deltaflow.state.durable import StateDirectory  # noqa: PLC0415

    async with await psycopg.AsyncConnection.connect(args.dsn, autocommit=True) as conn:
        exists = await slot_exists(conn, args.slot)
    if exists:
        repl = await ReplicationConnection.connect(args.dsn)
        try:
            await drop_slot(repl, args.slot)
        finally:
            await repl.close()
    StateDirectory(Path(args.state_dir)).reset()
    print(f"dropped slot {args.slot} (if any) and state in {args.state_dir}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=pg_dsn())
    parser.add_argument("--state-dir", default=".deltaflow/state")
    parser.add_argument("--slot", default="deltaflow_worker")
    parser.add_argument("--publication", default="deltaflow_worker")
    parser.add_argument("--memory-budget", type=int, default=64, help="MiB")
    parser.add_argument("--flush-interval", type=float, default=0.1, help="seconds")
    parser.add_argument("--checkpoint-interval", type=float, default=5.0, help="seconds")
    parser.add_argument("--reset", action="store_true", help="drop the slot and state, then exit")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(errors="backslashreplace")
    with contextlib.suppress(KeyboardInterrupt):
        aio.run(_reset(args) if args.reset else _main(args))


if __name__ == "__main__":
    main()
