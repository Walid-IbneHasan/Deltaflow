"""Randomized insert/update/delete load against the seed schema.

    python -m deltaflow.devtools.loadgen [--seed N] [--txns N | --txns 0] [--rate TPS]

The seed is always printed. Re-running with the same seed from the same
starting state (`make seed` with the same arguments) replays the exact same
transactions.
"""

from __future__ import annotations

import argparse
import asyncio
import secrets
import time
from collections import Counter
from dataclasses import dataclass, field

import psycopg
from psycopg.rows import class_row

from deltaflow import aio
from deltaflow.config import pg_dsn
from deltaflow.devtools.workload import Customer, OpKind, Order, TableState, Workload


class ModelDivergedError(RuntimeError):
    """A statement affected a different number of rows than the model predicted."""


@dataclass(slots=True)
class LoadStats:
    committed: int = 0
    rolled_back: int = 0
    statements: Counter[OpKind] = field(default_factory=Counter)


async def load_state(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> TableState:
    """Read both tables into a `TableState`, ordered by primary key."""
    async with conn.cursor(row_factory=class_row(Customer)) as ccur:
        await ccur.execute("SELECT id, name, country, email, bio FROM customers ORDER BY id")
        customers = {c.id: c async for c in ccur}
    async with conn.cursor(row_factory=class_row(Order)) as ocur:
        await ocur.execute(
            "SELECT id, customer_id, status, amount, note, created_at FROM orders ORDER BY id"
        )
        orders = {o.id: o async for o in ocur}
    return TableState(customers, orders)


async def run_load(
    conn: psycopg.AsyncConnection[tuple[object, ...]],
    workload: Workload,
    txns: int,
    rate: float | None = None,
    stats: LoadStats | None = None,
) -> LoadStats:
    """Execute `txns` transactions (0 = until cancelled) on an autocommit connection.

    Committed transactions are applied to `workload.state` by the planner, so
    after this returns the database must equal `workload.state`.
    """
    stats = stats if stats is not None else LoadStats()
    interval = 1.0 / rate if rate else 0.0
    next_due = time.monotonic()
    done = 0
    while txns == 0 or done < txns:
        txn = workload.next_transaction()
        async with conn.transaction(force_rollback=not txn.commit), conn.cursor() as cur:
            for stmt in txn.statements:
                await cur.execute(stmt.sql, stmt.params)
                if cur.rowcount != stmt.expected_rowcount:
                    raise ModelDivergedError(
                        f"{stmt.kind} affected {cur.rowcount} rows, expected "
                        f"{stmt.expected_rowcount}: {stmt.sql} {stmt.params!r}"
                    )
                stats.statements[stmt.kind] += 1
        if txn.commit:
            stats.committed += 1
        else:
            stats.rolled_back += 1
        done += 1
        if interval:
            next_due += interval
            await asyncio.sleep(max(0.0, next_due - time.monotonic()))
    return stats


async def _main(dsn: str, seed: int, txns: int, rate: float | None, stats: LoadStats) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        workload = Workload(seed, await load_state(conn))
        print(
            f"start customers={len(workload.state.customers)} orders={len(workload.state.orders)}",
            flush=True,
        )
        await run_load(conn, workload, txns=txns, rate=rate, stats=stats)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=pg_dsn())
    parser.add_argument("--seed", type=int, default=None, help="default: random, printed")
    parser.add_argument("--txns", type=int, default=2000, help="0 runs until interrupted")
    parser.add_argument("--rate", type=float, default=None, help="target transactions/second")
    args = parser.parse_args(argv)

    seed = args.seed if args.seed is not None else secrets.randbelow(2**32)
    print(
        f"loadgen seed={seed}  (replay: make seed && make load LOAD_ARGS='--seed {seed} "
        f"--txns {args.txns}')",
        flush=True,
    )
    stats = LoadStats()
    started = time.perf_counter()
    try:
        aio.run(_main(args.dsn, seed, args.txns, args.rate, stats))
    except KeyboardInterrupt:
        print("interrupted", flush=True)
    elapsed = time.perf_counter() - started
    total = stats.committed + stats.rolled_back
    print(
        f"done txns={total} committed={stats.committed} rolled_back={stats.rolled_back} "
        f"in {elapsed:.2f}s ({total / elapsed:.0f} txn/s)"
    )
    for kind in OpKind:
        print(f"  {kind:<16} {stats.statements[kind]}")


if __name__ == "__main__":
    main()
