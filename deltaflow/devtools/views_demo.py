"""Maintain a filtered view and a join view over the seed tables and print their changes.

    python -m deltaflow.devtools.views_demo [--dsn DSN]

Run `make load` in another terminal and watch the views follow it. Each line
is one frontier advance, i.e. one committed transaction that changed the
view: how many rows it added and retracted, and the view's size afterwards.
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
from deltaflow.planner import Deltaflow
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
    started = time.perf_counter()
    async with Runner(df, dsn, status_interval=2) as runner:
        print(
            f"snapshot loaded in {time.perf_counter() - started:.2f}s at {runner.engine.frontier}: "
            f"{big.name}={big.sink.result.size()} rows, {nl.name}={nl.sink.result.size()} rows",
            flush=True,
        )
        big.live = nl.live = True
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
