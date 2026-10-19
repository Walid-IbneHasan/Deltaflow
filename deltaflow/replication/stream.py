"""A running logical replication stream: committed transactions out, acknowledgements in.

Two background tasks share the connection:

* the reader decodes CopyData into `Transaction`s and puts them on a bounded
  queue. When the consumer falls behind, the queue fills, the reader stops
  reading, and TCP backpressure pauses the walsender;
* the heartbeat sends a standby status update every `status_interval`
  seconds, even while the reader is blocked. Without it a slow consumer would
  be disconnected after `wal_sender_timeout`.

Acknowledgement is explicit. `ack(lsn)` records that the consumer has durably
processed everything up to `lsn`, normally a transaction's `end_lsn`. Only
that position is reported as flushed, so it is the only position the server
may discard WAL up to. Transactions received but not acknowledged are
re-sent after a reconnect, which makes delivery at-least-once; exactly-once
is the sink's job (M5).

With `progress=True` the stream also yields `Progress(lsn)` events, from
keepalives that arrive *between* transactions. The walsender reports there
how far it has decoded, and every transaction committing before that point
has already been sent (or skipped, having no published changes). So once
everything before a Progress event is processed, its position may be
acknowledged. Without that, a slot whose tables are idle never advances
while the rest of the server writes WAL, and the server keeps all of it.
A keepalive *inside* a transaction is never progress: the walsender sends
a transaction while decoding its commit record, so the position it reports
is past a transaction the consumer has only partly received.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from deltaflow.replication.changes import Transaction, TransactionAssembler
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import Message, decode_message
from deltaflow.replication.protocol import PrimaryKeepalive, parse_copy_data, standby_status_update
from deltaflow.replication.slot import check_name
from deltaflow.replication.wire import ReplicationConnection

log = logging.getLogger(__name__)


class ReplicationEndedError(Exception):
    """The server ended the replication stream without being asked to."""


@dataclass(frozen=True, slots=True)
class Progress:
    """Every transaction committing before `lsn` has been delivered."""

    lsn: Lsn


_END: Final = object()
LSN_ZERO: Final = Lsn(0)
type Event = Transaction | Message | Progress


class ReplicationStream:
    def __init__(
        self,
        conn: ReplicationConnection,
        *,
        slot: str,
        publications: Sequence[str],
        start_lsn: Lsn = LSN_ZERO,
        messages: bool = True,
        status_interval: float = 10.0,
        max_pending: int = 32,
        progress: bool = False,
    ) -> None:
        self._conn = conn
        self.slot = check_name(slot, "slot")
        self._publications = [check_name(p, "publication") for p in publications]
        self._start_lsn = start_lsn
        self._messages = messages
        self._status_interval = status_interval
        self._queue: asyncio.Queue[Event | BaseException | object] = asyncio.Queue(max_pending)
        self._assembler = TransactionAssembler()
        self._received = start_lsn
        self._flushed = start_lsn
        self._progress = progress
        #: the highest position delivered: a transaction's end, or progress
        self._delivered = start_lsn
        self._closing = False
        self._reader: asyncio.Task[None] | None = None
        self._heartbeat: asyncio.Task[None] | None = None

    @property
    def flushed_lsn(self) -> Lsn:
        return self._flushed

    @property
    def received_lsn(self) -> Lsn:
        """Highest WAL position received from the server."""
        return self._received

    @property
    def queued(self) -> int:
        """Events decoded but not yet consumed. At `max_pending` the reader
        stops reading the socket: backpressure all the way to the walsender."""
        return self._queue.qsize()

    @property
    def max_pending(self) -> int:
        return self._queue.maxsize

    async def start(self) -> None:
        options = ["proto_version '1'", f"publication_names '{','.join(self._publications)}'"]
        if self._messages:
            options.append("messages 'true'")
        await self._conn.start_copy_both(
            f"START_REPLICATION SLOT {self.slot} LOGICAL {self._start_lsn} ({', '.join(options)})"
        )
        self._reader = asyncio.create_task(self._read_loop(), name=f"replication-read:{self.slot}")
        self._heartbeat = asyncio.create_task(
            self._heartbeat_loop(), name=f"replication-heartbeat:{self.slot}"
        )

    def __aiter__(self) -> ReplicationStream:
        return self

    async def __anext__(self) -> Event:
        item = await self._queue.get()
        if item is _END:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        assert isinstance(item, Transaction | Message | Progress)
        return item

    def ack(self, lsn: Lsn) -> None:
        """Everything up to `lsn` is durably processed and need not be resent."""
        self._flushed = max(self._flushed, lsn)

    async def send_status(self, *, reply_requested: bool = False) -> None:
        self._conn.send_copy_data(
            standby_status_update(
                self._received,
                self._flushed,
                self._flushed,
                datetime.now(UTC),
                reply_requested=reply_requested,
            )
        )
        await self._conn.drain()

    async def _read_loop(self) -> None:
        try:
            while (data := await self._conn.read_copy_data()) is not None:
                msg = parse_copy_data(data)
                if isinstance(msg, PrimaryKeepalive):
                    if msg.reply_requested:
                        await self.send_status()
                    if (
                        self._progress
                        and not self._assembler.in_transaction
                        and msg.wal_end > self._delivered
                        and not self._closing
                    ):
                        self._received = max(self._received, msg.wal_end)
                        self._delivered = msg.wal_end
                        await self._queue.put(Progress(msg.wal_end))
                    continue
                self._received = max(self._received, msg.wal_start)
                event = self._assembler.feed(msg.wal_start, decode_message(msg.payload))
                if event is None or self._closing:
                    continue
                if isinstance(event, Transaction):
                    if event.end_lsn <= self._start_lsn:
                        log.info("skipping already-acknowledged transaction %s", event.xid)
                        continue
                    self._delivered = max(self._delivered, event.end_lsn)
                await self._queue.put(event)
            # The server sent CopyDone: either our close() asked for it, or it
            # ended the stream on its own.
            await self._conn.finish_command()
            if not self._closing:
                raise ReplicationEndedError(f"server ended replication on slot {self.slot}")
            await self._queue.put(_END)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._closing:
                await self._queue.put(exc)

    async def _heartbeat_loop(self) -> None:
        with contextlib.suppress(ConnectionError, OSError):
            while True:
                await asyncio.sleep(self._status_interval)
                await self.send_status()

    async def close(self, timeout: float = 10.0) -> None:
        """Report the final flushed position, leave CopyBoth, and disconnect."""
        if self._closing:
            return
        self._closing = True
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat
        # Unblock a reader waiting on a full queue; undelivered events are
        # unacknowledged and will be re-sent on the next connection.
        while not self._queue.empty():
            self._queue.get_nowait()
        try:
            if self._reader is not None and not self._reader.done():
                await self.send_status()
                self._conn.send_copy_done()
                await self._conn.drain()
                async with asyncio.timeout(timeout):
                    await self._reader
        except (TimeoutError, ConnectionError, OSError) as exc:
            log.warning("unclean replication shutdown on slot %s: %r", self.slot, exc)
            if self._reader is not None:
                self._reader.cancel()
        finally:
            await self._conn.close()
