"""Recreate the seed schema and bulk-load deterministic initial data.

python -m deltaflow.devtools.seed [--seed N] [--customers N] [--orders N]
"""

from __future__ import annotations

import argparse
import time
from importlib.resources import files

import psycopg

from deltaflow import aio
from deltaflow.config import pg_dsn
from deltaflow.devtools.workload import TableState, initial_state

SCHEMA_SQL = files("deltaflow.devtools").joinpath("schema.sql").read_text(encoding="utf-8")


async def seed_database(dsn: str, seed: int, customers: int, orders: int) -> TableState:
    """Drop and recreate the tables, load them, and return what was loaded."""
    state = initial_state(seed=seed, customers=customers, orders=orders)
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        async with conn.transaction():
            await conn.execute(SCHEMA_SQL)
            async with conn.cursor() as cur:
                async with cur.copy(
                    "COPY customers (id, name, country, email, bio) FROM STDIN"
                ) as copy:
                    for c in state.customers.values():
                        await copy.write_row((c.id, c.name, c.country, c.email, c.bio))
                async with cur.copy(
                    "COPY orders (id, customer_id, status, amount, note, created_at) FROM STDIN"
                ) as copy:
                    for o in state.orders.values():
                        await copy.write_row(
                            (o.id, o.customer_id, o.status, o.amount, o.note, o.created_at)
                        )
        await conn.execute("ANALYZE customers, orders")
    return state


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=pg_dsn())
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--customers", type=int, default=1_000)
    parser.add_argument("--orders", type=int, default=10_000)
    args = parser.parse_args(argv)

    started = time.perf_counter()
    state = aio.run(seed_database(args.dsn, args.seed, args.customers, args.orders))
    print(
        f"seeded customers={len(state.customers)} orders={len(state.orders)} "
        f"seed={args.seed} in {time.perf_counter() - started:.2f}s"
    )


if __name__ == "__main__":
    main()
