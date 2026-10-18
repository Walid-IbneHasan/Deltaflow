"""Shared machinery for lockstep differential tests: run one workload
transaction, then wait until the engine has applied exactly that one."""

from __future__ import annotations

from typing import Any

import psycopg

from deltaflow.devtools.loadgen import LoadStats, ModelDivergedError
from deltaflow.devtools.workload import Workload
from deltaflow.replication.changes import Transaction
from deltaflow.replication.pgoutput import Message
from deltaflow.runtime import Runner

MARKER = "deltaflow.test"

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]


async def execute(sql: Sql, workload: Workload, stats: LoadStats, *, marker: str) -> bool:
    """Run the workload's next transaction. Committed ones end with a
    numbered marker message, so the consumer can tell when it has seen it.
    Returns whether it committed."""
    txn = workload.next_transaction()
    async with sql.transaction(force_rollback=not txn.commit), sql.cursor() as cur:
        for stmt in txn.statements:
            await cur.execute(stmt.sql, stmt.params)
            if cur.rowcount != stmt.expected_rowcount:
                raise ModelDivergedError(f"{stmt.kind} affected {cur.rowcount} rows: {stmt.sql}")
            stats.statements[stmt.kind] += 1
        await cur.execute("SELECT pg_logical_emit_message(true, %s, %s)", (MARKER, marker))
    if txn.commit:
        stats.committed += 1
    else:
        stats.rolled_back += 1
    return txn.commit


async def until_marker(runner: Runner, content: str) -> Transaction:
    """Apply transactions until the one carrying marker `content`."""
    expected = content.encode()
    while True:
        txn = await runner.next_transaction()
        for change in txn.changes:
            if isinstance(change, Message) and change.prefix == MARKER:
                if change.content == expected:
                    return txn
                if content != "end":
                    raise AssertionError(f"marker {change.content!r} while waiting for {content}")
