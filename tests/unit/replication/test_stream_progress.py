"""Keepalives as progress: the position a stream may advance to between
transactions, and never inside one."""

from __future__ import annotations

import asyncio
import struct

from deltaflow.aio import run
from deltaflow.replication.changes import Transaction
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.stream import Event, Progress, ReplicationStream

from . import pgoutput_bytes as pb

ORDERS = pb.relation(100, "public", "orders", "d", [(1, "id", 20, -1)])


def xlog(start: int, payload: bytes) -> bytes:
    return b"w" + struct.pack(">QQq", start, start, 0) + payload


def keepalive(wal_end: int) -> bytes:
    return b"k" + struct.pack(">Qq?", wal_end, 0, False)


class FakeConnection:
    """Replays CopyData frames as a replication connection would deliver them."""

    def __init__(self, frames: list[bytes]) -> None:
        self._frames: asyncio.Queue[bytes | None] = asyncio.Queue()
        for frame in frames:
            self._frames.put_nowait(frame)
        self.sent: list[bytes] = []

    async def start_copy_both(self, sql: str) -> None:
        pass

    async def read_copy_data(self) -> bytes | None:
        return await self._frames.get()

    def send_copy_data(self, data: bytes) -> None:
        self.sent.append(data)

    async def drain(self) -> None:
        pass

    def send_copy_done(self) -> None:
        self._frames.put_nowait(None)

    async def finish_command(self) -> None:
        pass

    async def close(self) -> None:
        pass


#: one transaction, with a keepalive in the middle of it. The walsender
#: sends a transaction while decoding its commit record, so a keepalive
#: there reports a position *past* the commit: acknowledging it would tell
#: the server the half-received transaction is done.
FRAMES = [
    xlog(0x100, ORDERS),
    xlog(0x200, pb.begin(0x300, 0, 7)),
    xlog(0x210, pb.insert(100, ["1"])),
    keepalive(0x400),  # inside the transaction
    xlog(0x300, pb.commit(0x300, 0x380, 0)),
    keepalive(0x500),  # between transactions: progress
    keepalive(0x500),  # nothing new
    keepalive(0x480),  # behind: nothing new
    keepalive(0x600),
]


async def events(frames: list[bytes], *, progress: bool, start: int = 0) -> list[Event]:
    conn = FakeConnection(frames)
    stream = ReplicationStream(
        conn,  # type: ignore[arg-type]
        slot="s", publications=["p"], start_lsn=Lsn(start), progress=progress,
    )  # fmt: skip
    await stream.start()
    out: list[Event] = []
    try:
        async with asyncio.timeout(0.5):
            async for event in stream:
                out.append(event)
    except TimeoutError:
        pass
    await stream.close()
    return out


def test_keepalives_between_transactions_are_progress() -> None:
    got = run(events(FRAMES, progress=True))
    assert [type(e).__name__ for e in got] == ["Transaction", "Progress", "Progress"]
    txn, first, second = got
    assert isinstance(txn, Transaction)
    assert txn.end_lsn == Lsn(0x380)
    assert first == Progress(Lsn(0x500))
    assert second == Progress(Lsn(0x600))


def test_a_keepalive_inside_a_transaction_is_not_progress() -> None:
    """Only the transaction, then progress past it: never 0x400."""
    got = run(events(FRAMES[:5], progress=True))
    assert [type(e).__name__ for e in got] == ["Transaction"]


def test_progress_at_or_below_the_start_position_is_not_reported() -> None:
    got = run(events([keepalive(0x500), keepalive(0x700)], progress=True, start=0x600))
    assert got == [Progress(Lsn(0x700))]


def test_progress_is_opt_in() -> None:
    got = run(events(FRAMES, progress=False))
    assert [type(e).__name__ for e in got] == ["Transaction"]
