"""Connect an engine to Postgres: publication, slot, snapshot, stream.

M2 state lives in memory, so every start is a fresh start. The runner
creates a *temporary* slot that exports a snapshot, loads the snapshot, and
streams from the slot's consistent point. The slot disappears with the
connection. Resuming from a durable watermark instead is M5.
"""

from __future__ import annotations

import uuid
from types import TracebackType

import psycopg

from deltaflow.planner import Deltaflow
from deltaflow.replication.changes import Transaction
from deltaflow.replication.slot import create_publication, create_slot
from deltaflow.replication.stream import ReplicationStream
from deltaflow.replication.wire import ReplicationConnection
from deltaflow.runtime.engine import Engine
from deltaflow.runtime.snapshot import load_snapshot


class Runner:
    def __init__(
        self,
        registry: Deltaflow,
        dsn: str,
        *,
        publication: str = "deltaflow_views",
        manage_publication: bool = True,
        status_interval: float = 10.0,
    ) -> None:
        """With `manage_publication` the runner (re)creates `publication` to
        cover exactly the views' sources. Otherwise it must already exist,
        and it is checked."""
        self.engine = Engine(registry)
        self._dsn = dsn
        self.publication = publication
        self._manage_publication = manage_publication
        self._status_interval = status_interval
        self._stream: ReplicationStream | None = None

    async def start(self) -> None:
        if self._manage_publication:
            async with await psycopg.AsyncConnection.connect(self._dsn, autocommit=True) as admin:
                await create_publication(admin, self.publication, self.engine.sources)
        repl = await ReplicationConnection.connect(self._dsn, application_name="deltaflow")
        try:
            slot = await create_slot(
                repl, f"deltaflow_{uuid.uuid4().hex[:16]}", temporary=True, export_snapshot=True
            )
            assert slot.snapshot_name is not None
            async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
                await load_snapshot(
                    self.engine,
                    conn,
                    snapshot_name=slot.snapshot_name,
                    consistent_point=slot.consistent_point,
                    publication=self.publication,
                )
            self._stream = ReplicationStream(
                repl,
                slot=slot.name,
                publications=[self.publication],
                status_interval=self._status_interval,
            )
            await self._stream.start()
        except BaseException:
            await repl.close()
            raise

    async def next_transaction(self) -> Transaction:
        """Wait for the next committed transaction and apply it to every view."""
        if self._stream is None:
            raise RuntimeError("runner not started")
        async for event in self._stream:
            if isinstance(event, Transaction):
                self.engine.apply(event)
                # In-memory state: "durably processed" is as durable as this
                # process. The slot is temporary, so nothing outlives it anyway.
                self._stream.ack(event.end_lsn)
                return event
        raise RuntimeError("replication stream ended")

    async def run(self) -> None:
        while True:
            await self.next_transaction()

    async def close(self) -> None:
        if self._stream is not None:
            await self._stream.close()
            self._stream = None

    async def __aenter__(self) -> Runner:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
