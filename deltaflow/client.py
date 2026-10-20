"""Read-your-writes, for applications: wait until views reflect a write.

    with psycopg.connect(dsn) as conn:
        conn.execute("INSERT INTO orders ...")
        conn.commit()
        assert wait_for(conn, views=["order_totals"])  # the view has the order
        conn.execute("SELECT ... FROM analytics.order_totals")

Without `lsn`, the target is the server's current WAL position, so
everything the session has committed. Both call `_deltaflow.wait_for`
(docs/read-your-writes.md), which any language can call in SQL too.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg

from deltaflow.replication.lsn import Lsn

_QUERY = (
    "SELECT _deltaflow.wait_for(COALESCE(%s::pg_lsn, pg_current_wal_lsn()), %s,"
    " make_interval(secs => %s))"
)


def _params(lsn: Lsn | str | None, views: Sequence[str] | None, timeout: float) -> tuple[Any, ...]:
    return (None if lsn is None else str(lsn), None if views is None else list(views), timeout)


def wait_for(
    conn: psycopg.Connection[Any],
    lsn: Lsn | str | None = None,
    *,
    views: Sequence[str] | None = None,
    timeout: float = 10.0,
) -> bool:
    """True once every view (or each of `views`) reflects everything
    committed up to `lsn`; False after `timeout` seconds. Call it outside a
    REPEATABLE READ or SERIALIZABLE transaction: it must see the watermark
    move."""
    with conn.cursor() as cur:
        cur.execute(_QUERY, _params(lsn, views, timeout))
        row = cur.fetchone()
    return bool(row and row[0])


async def await_for(
    conn: psycopg.AsyncConnection[Any],
    lsn: Lsn | str | None = None,
    *,
    views: Sequence[str] | None = None,
    timeout: float = 10.0,
) -> bool:
    """`wait_for` on an async connection."""
    async with conn.cursor() as cur:
        await cur.execute(_QUERY, _params(lsn, views, timeout))
        row = await cur.fetchone()
    return bool(row and row[0])
