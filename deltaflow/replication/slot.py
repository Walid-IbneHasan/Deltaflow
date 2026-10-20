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
from dataclasses import dataclass, field

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
    failover: bool = False,
) -> SlotInfo:
    """With `failover` (Postgres 17+), standbys with `sync_replication_slots`
    keep a copy of the slot, so it survives a promotion."""
    check_name(name, "slot")
    snapshot = "export" if export_snapshot else "nothing"
    temp = " TEMPORARY" if temporary else ""
    options = f"SNAPSHOT '{snapshot}'" + (", FAILOVER true" if failover else "")
    rows = await conn.simple_query(
        f"CREATE_REPLICATION_SLOT {name}{temp} LOGICAL pgoutput ({options})"
    )
    [(slot_name, consistent_point, snapshot_name, _plugin)] = rows
    if slot_name is None or consistent_point is None:
        raise ValueError(f"CREATE_REPLICATION_SLOT returned {rows!r}")
    return SlotInfo(slot_name, Lsn.parse(consistent_point), snapshot_name)


async def set_failover(conn: ReplicationConnection, name: str) -> None:
    """Make an existing slot a failover slot (Postgres 17+)."""
    await conn.simple_query(f"ALTER_REPLICATION_SLOT {check_name(name, 'slot')} (FAILOVER true)")


@dataclass(frozen=True, slots=True)
class SystemInfo:
    """IDENTIFY_SYSTEM: which cluster, which timeline, how far its WAL goes."""

    system_id: str
    timeline: int
    xlogpos: Lsn
    #: (timeline, the LSN where it ended) for each ancestor (TIMELINE_HISTORY)
    history: tuple[tuple[int, Lsn], ...] = field(default=())


async def identify_system(conn: ReplicationConnection) -> SystemInfo:
    [(system_id, timeline, xlogpos, _db)] = await conn.simple_query("IDENTIFY_SYSTEM")
    assert system_id is not None
    assert timeline is not None
    assert xlogpos is not None
    tli = int(timeline)
    history: tuple[tuple[int, Lsn], ...] = ()
    if tli > 1:  # timeline 1 has no history file
        [(_name, content)] = await conn.simple_query(f"TIMELINE_HISTORY {tli}")
        history = _history(content or "")
    return SystemInfo(system_id, tli, Lsn.parse(xlogpos), history)


def _history(content: str) -> tuple[tuple[int, Lsn], ...]:
    """A timeline history file: "parent_tli<TAB>switchpoint<TAB>reason" lines
    (as bytea text, hex, when the server sends it so)."""
    if content.startswith("\\x"):
        content = bytes.fromhex(content[2:]).decode()
    entries = []
    for line in content.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0].isdigit():
            entries.append((int(fields[0]), Lsn.parse(fields[1])))
    return tuple(entries)


def diverged(checkpoint_lsn: Lsn, recorded: dict[str, object], now: SystemInfo) -> str | None:
    """Why a checkpoint taken on `recorded` (system id, timeline) cannot be
    resumed on `now`'s server: another cluster, or a timeline that ended
    before the checkpoint (a failover lost WAL the views applied)."""
    if not recorded:
        return None
    if recorded.get("id") != now.system_id:
        return (
            f"the server is another cluster (system identifier {now.system_id}, the checkpoint's "
            f"{recorded.get('id')})"
        )
    timeline = int(str(recorded.get("timeline")))
    if timeline == now.timeline:
        return None
    ends = dict(now.history)
    if timeline not in ends:
        return (
            f"timeline {timeline} of the checkpoint is not in the history of timeline "
            f"{now.timeline}"
        )
    if checkpoint_lsn > ends[timeline]:
        return (
            f"the checkpoint at {checkpoint_lsn} is past where timeline {timeline} ended "
            f"({ends[timeline]}): the failover lost WAL the views had applied"
        )
    return None


async def drop_slot(conn: ReplicationConnection, name: str) -> None:
    """Drop a slot, waiting for any connection still using it to let go."""
    await conn.simple_query(f"DROP_REPLICATION_SLOT {check_name(name, 'slot')} WAIT")


async def create_publication(
    conn: psycopg.AsyncConnection[tuple[object, ...]], name: str, tables: Sequence[str]
) -> None:
    """(Re)create publication `name` covering exactly `tables` (schema.table).
    A partitioned table is published via its root: its changes arrive under
    its own name, not its partitions'."""
    ident = sql.Identifier(check_name(name, "publication"))
    targets = sql.SQL(", ").join(sql.Identifier(*t.split(".", 1)) for t in tables)
    async with conn.transaction():
        await conn.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(ident))
        await conn.execute(
            sql.SQL(
                "CREATE PUBLICATION {} FOR TABLE {} WITH (publish_via_partition_root = true)"
            ).format(ident, targets)
        )


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
