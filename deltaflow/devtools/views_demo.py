"""Maintain a filtered view, a join view and an aggregate over the seed tables; print changes.

    python -m deltaflow.devtools.views_demo [--dsn DSN]

Run `make load` in another terminal and watch the views follow it. Each line
is one frontier advance, i.e. one committed transaction that changed the
view: how many rows it added and retracted, and the view's size afterwards.
For the aggregate (the spec's orders_per_customer), each line is a group's
new value.
State is rebuilt on every start (temporary slot, fresh state store), so
each start re-reads the snapshot.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import sys
import time
from decimal import Decimal

from deltaflow import aio
from deltaflow.config import pg_dsn
from deltaflow.core import Delta, Frontier, Row
from deltaflow.planner import Count, Deltaflow, Sum
from deltaflow.runtime import Runner
from deltaflow.sinks import MemorySink

BIG = Decimal(4000)


def big_open_order(r: Row) -> bool:
    return bool(r["amount"] >= BIG and r["status"] != "cancelled")


def dutch(r: Row) -> bool:
    return bool(r["country"] == "NL")


class _Printer:
    """on_change callback that prints one line per frontier advance, once live."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.sink = MemorySink(on_change=self)
        self.live = False

    def __call__(self, frontier: Frontier, batch: list[Delta[Row]]) -> None:
        if not self.live:
            return  # the snapshot itself is summarised once, not printed as a change
        added = sum(d.diff for d in batch if d.diff > 0)
        removed = -sum(d.diff for d in batch if d.diff < 0)
        print(
            f"[{frontier}] {self.name:16} +{added:<3} -{removed:<3} "
            f"-> {self.sink.result.size()} rows",
            flush=True,
        )


class _GroupPrinter:
    """Prints each changed group's new row (and groups that disappear)."""

    def __init__(self, name: str, key: str) -> None:
        self.name = name
        self.key = key
        self.sink = MemorySink(on_change=self)
        self.live = False

    def __call__(self, frontier: Frontier, batch: list[Delta[Row]]) -> None:
        if not self.live:
            return
        added = {d.row[self.key]: d.row for d in batch if d.diff > 0}
        gone = {d.row[self.key] for d in batch if d.diff < 0} - added.keys()
        for key, row in added.items():
            values = ", ".join(f"{k}={v}" for k, v in row.items() if k != self.key)
            print(f"[{frontier}] {self.name} {key}: {values}", flush=True)
        for key in gone:
            print(f"[{frontier}] {self.name} {key}: (group gone)", flush=True)


async def _main(dsn: str) -> None:
    df = Deltaflow()
    big, nl = _Printer("big_open_orders"), _Printer("dutch_big_orders")
    (
        df.view(big.name)
        .source("public.orders")
        .filter(big_open_order)
        .select("id", "customer_id", "amount")
        .sink(big.sink)
    )
    (
        df.view(nl.name)
        .source("public.orders")
        .filter(big_open_order)
        .join("public.customers", on=("customer_id", "id"))
        .filter(dutch)
        .select("orders.id", "customers.name", "amount")
        .sink(nl.sink)
    )
    per_country = _GroupPrinter("orders_per_customer", "customers.country")
    (
        df.view(per_country.name)
        .source("public.orders")
        .join("public.customers", on=("customer_id", "id"))
        .filter(lambda r: r["status"] != "cancelled")
        .group_by("customers.country")
        .agg(count=Count(), revenue=Sum("orders.amount"))
        .sink(per_country.sink)
    )
    started = time.perf_counter()
    async with Runner(df, dsn, status_interval=2) as runner:
        print(
            f"snapshot loaded in {time.perf_counter() - started:.2f}s at {runner.engine.frontier}: "
            f"{big.name}={big.sink.result.size()} rows, {nl.name}={nl.sink.result.size()} rows",
            flush=True,
        )
        for country in sorted(per_country.sink.rows(), key=lambda r: str(r["customers.country"])):
            print(f"  {per_country.name} {dict(country)}", flush=True)
        big.live = nl.live = per_country.live = True
        print("following the stream; Ctrl-C to stop", flush=True)
        await runner.run()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=pg_dsn())
    args = parser.parse_args(argv)
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(errors="backslashreplace")
    with contextlib.suppress(KeyboardInterrupt):
        aio.run(_main(args.dsn))


if __name__ == "__main__":
    main()
