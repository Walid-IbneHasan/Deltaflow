"""Print the decoded change stream of the seed tables.

    python -m deltaflow.replication.tail [--slot NAME] [--publication NAME]
    python -m deltaflow.replication.tail --drop     # remove the slot again

Creates the publication and a *persistent* slot if they are missing, then
prints every committed transaction. A transaction is acknowledged only after
it has been printed, so after Ctrl-C the next run resumes where this one
stopped. A persistent slot retains WAL while nobody reads it, so drop it when
you are done.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import sys
from typing import Any

import psycopg

from deltaflow import aio
from deltaflow.config import pg_dsn
from deltaflow.replication.changes import (
    Row,
    RowDelete,
    RowInsert,
    RowUpdate,
    TableTruncate,
    Transaction,
)
from deltaflow.replication.pgoutput import UNCHANGED_TOAST, Message
from deltaflow.replication.slot import create_publication, create_slot, drop_slot, slot_exists
from deltaflow.replication.stream import ReplicationStream
from deltaflow.replication.wire import ReplicationConnection

TABLES = ("public.customers", "public.orders")


def _value(v: Any) -> str:
    if v is UNCHANGED_TOAST:
        return "<unchanged toast>"
    if isinstance(v, str) and len(v) > 40:
        return f"<text {len(v)} chars>"
    return repr(v)


def _row(row: Row) -> str:
    return "{" + ", ".join(f"{k}: {_value(v)}" for k, v in row.items()) + "}"


def format_transaction(txn: Transaction) -> str:
    lines = [
        f"BEGIN xid={txn.xid} commit_lsn={txn.commit_lsn} end_lsn={txn.end_lsn} "
        f"at {txn.commit_time.isoformat()}"
    ]
    for c in txn.changes:
        match c:
            case RowInsert():
                lines.append(f"  {c.lsn} INSERT {c.relation.qualified_name} {_row(c.new)}")
            case RowUpdate():
                lines.append(
                    f"  {c.lsn} UPDATE {c.relation.qualified_name} key={_row(c.key)} "
                    f"-> {_row(c.new)}"
                )
            case RowDelete():
                lines.append(f"  {c.lsn} DELETE {c.relation.qualified_name} key={_row(c.key)}")
            case TableTruncate():
                names = ", ".join(r.qualified_name for r in c.relations)
                lines.append(f"  {c.lsn} TRUNCATE {names}")
            case Message():
                lines.append(f"  {c.lsn} MESSAGE {c.prefix}: {c.content!r}")
    lines.append("COMMIT")
    return "\n".join(lines)


async def _tail(dsn: str, slot: str, publication: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (publication,))
            if await cur.fetchone() is None:
                await create_publication(conn, publication, TABLES)
                print(f"created publication {publication} for {', '.join(TABLES)}")
        exists = await slot_exists(conn, slot)
    repl = await ReplicationConnection.connect(dsn)
    if not exists:
        info = await create_slot(repl, slot)
        print(f"created slot {slot} at {info.consistent_point}")
    stream = ReplicationStream(repl, slot=slot, publications=[publication], status_interval=2)
    await stream.start()
    print(f"streaming slot {slot}; Ctrl-C to stop", flush=True)
    try:
        async for event in stream:
            if isinstance(event, Transaction):
                print(format_transaction(event), flush=True)
                stream.ack(event.end_lsn)
            else:
                print(f"MESSAGE {event.prefix}: {event.content!r} (non-transactional)")
    except asyncio.CancelledError:
        pass
    finally:
        await stream.close()
        print(f"stopped; confirmed through {stream.flushed_lsn}")


async def _drop(dsn: str, slot: str) -> None:
    repl = await ReplicationConnection.connect(dsn)
    try:
        await drop_slot(repl, slot)
    finally:
        await repl.close()
    print(f"dropped slot {slot}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=pg_dsn())
    parser.add_argument("--slot", default="deltaflow_tail")
    parser.add_argument("--publication", default="deltaflow")
    parser.add_argument("--drop", action="store_true", help="drop the slot and exit")
    args = parser.parse_args(argv)
    if isinstance(sys.stdout, io.TextIOWrapper):
        # Row values are arbitrary Unicode; a redirected Windows stdout is
        # cp1252. Escape what the console cannot show instead of crashing.
        sys.stdout.reconfigure(errors="backslashreplace")
    with contextlib.suppress(KeyboardInterrupt):
        aio.run(
            _drop(args.dsn, args.slot)
            if args.drop
            else _tail(args.dsn, args.slot, args.publication)
        )


if __name__ == "__main__":
    main()
