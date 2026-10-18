"""Publications and logical replication slots.

Slots are created and dropped with replication-protocol commands on a
`ReplicationConnection`. That matters for `export_snapshot`: the exported
snapshot is the exact database state as of the slot's consistent point, valid
only until the next command on that connection. Publications are ordinary SQL
and use a psycopg connection.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

import psycopg
from psycopg import sql

from deltaflow.replication.lsn import Lsn
from deltaflow.replication.wire import ReplicationConnection

_NAME = re.compile(r"[a-z_][a-z0-9_]{0,62}")


def check_name(name: str, what: str) -> str:
    """Slot and publication names are embedded in replication commands, which
    take no bind parameters; restrict them instead of quoting."""
    if not _NAME.fullmatch(name):
        raise ValueError(f"invalid {what} name {name!r}: use [a-z_][a-z0-9_]*, max 63 chars")
    return name


@dataclass(frozen=True, slots=True)
class SlotInfo:
    name: str
    #: Changes committed after this LSN are streamed; everything committed
    #: before it is in the exported snapshot (if one was requested).
    consistent_point: Lsn
    snapshot_name: str | None


async def create_slot(
    conn: ReplicationConnection,
    name: str,
    *,
    temporary: bool = False,
    export_snapshot: bool = False,
) -> SlotInfo:
    check_name(name, "slot")
    snapshot = "export" if export_snapshot else "nothing"
    temp = " TEMPORARY" if temporary else ""
    rows = await conn.simple_query(
        f"CREATE_REPLICATION_SLOT {name}{temp} LOGICAL pgoutput (SNAPSHOT '{snapshot}')"
    )
    [(slot_name, consistent_point, snapshot_name, _plugin)] = rows
    if slot_name is None or consistent_point is None:
        raise ValueError(f"CREATE_REPLICATION_SLOT returned {rows!r}")
    return SlotInfo(slot_name, Lsn.parse(consistent_point), snapshot_name)


async def drop_slot(conn: ReplicationConnection, name: str) -> None:
    """Drop a slot, waiting for any connection still using it to let go."""
    await conn.simple_query(f"DROP_REPLICATION_SLOT {check_name(name, 'slot')} WAIT")


async def create_publication(
    conn: psycopg.AsyncConnection[tuple[object, ...]], name: str, tables: Sequence[str]
) -> None:
    """(Re)create publication `name` covering exactly `tables` (schema.table)."""
    ident = sql.Identifier(check_name(name, "publication"))
    targets = sql.SQL(", ").join(sql.Identifier(*t.split(".", 1)) for t in tables)
    async with conn.transaction():
        await conn.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(ident))
        await conn.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(ident, targets))


async def slot_confirmed_flush(conn: psycopg.AsyncConnection[tuple[object, ...]], name: str) -> Lsn:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
            (name,),
        )
        row = await cur.fetchone()
    if row is None:
        raise LookupError(f"no replication slot {name!r}")
    return Lsn.parse(str(row[0]))


async def slot_exists(conn: psycopg.AsyncConnection[tuple[object, ...]], name: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (name,))
        return await cur.fetchone() is not None
