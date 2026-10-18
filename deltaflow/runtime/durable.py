"""The durable runner: exactly-once views that survive `kill -9`.

Order of operations, which the exactly-once argument depends on
(docs/exactly-once.md):

1. apply committed transactions to the engine;
2. **flush**: write every Postgres sink's net changes and move every
   watermark to the engine's frontier W, in one Postgres transaction;
3. every `checkpoint_interval` seconds, right after a flush, **checkpoint**
   the state at C = W, then acknowledge C to the replication slot.

So at any instant: slot acknowledged ≤ C ≤ W. A crash anywhere loses at
most the work after C. On restart:

- the state is restored to C;
- the persistent slot replays everything after C (it was never
  acknowledged further);
- the sinks drop the replayed transactions at or below W.

Changes are neither lost (nothing after C was acknowledged) nor applied
twice (the watermark commits atomically with the rows it covers).

If there is no usable checkpoint, the runner bootstraps instead and says
so in the log and in `counters["bootstraps"]`. Reasons: first start, a
missing slot, changed views, missing watermarks. Bootstrapping means: drop
the slot, create it again with an exported snapshot, load the snapshot,
*replace* each sink table's contents in one transaction, and checkpoint.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from pathlib import Path
from types import TracebackType
from typing import Any

import psycopg

from deltaflow.faults import crash_point
from deltaflow.planner import Deltaflow, PlanError
from deltaflow.replication.changes import Transaction
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.slot import create_publication, create_slot, drop_slot, slot_exists
from deltaflow.replication.stream import ReplicationStream
from deltaflow.replication.wire import ReplicationConnection
from deltaflow.runtime.engine import Engine, fingerprint
from deltaflow.runtime.runner import DEFAULT_MEMORY_BUDGET
from deltaflow.runtime.snapshot import load_snapshot
from deltaflow.sinks.postgres import (
    PostgresSink,
    describe_sink_table,
    ensure_watermark_table,
    flush,
    load_watermark,
)
from deltaflow.state.durable import StateDirectory
from deltaflow.state.store import StateStore

log = logging.getLogger(__name__)

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]


class DurableRunner:
    def __init__(
        self,
        registry: Deltaflow,
        dsn: str,
        *,
        state_dir: Path,
        slot: str,
        publication: str = "deltaflow_views",
        manage_publication: bool = True,
        memory_budget: int = DEFAULT_MEMORY_BUDGET,
        flush_interval: float = 0.1,
        max_flush_transactions: int = 1_000,
        checkpoint_interval: float = 5.0,
        status_interval: float = 10.0,
        sink_dsn: str | None = None,
    ) -> None:
        for view in registry.views:
            if not isinstance(view.sink, PostgresSink):
                raise PlanError(
                    f"view {view.name!r}: a durable runner needs durable sinks (PostgresSink); "
                    f"{type(view.sink).__name__} cannot be restored after a crash"
                )
        self.registry = registry
        self.sinks = [v.sink for v in registry.views if isinstance(v.sink, PostgresSink)]
        self._views = {id(v.sink): v.name for v in registry.views}
        self._dsn = dsn
        self._sink_dsn = sink_dsn or dsn
        self.state = StateDirectory(state_dir)
        self.slot = slot
        self.publication = publication
        self._manage_publication = manage_publication
        self._memory_budget = memory_budget
        self._flush_interval = flush_interval
        self._max_flush = max_flush_transactions
        self._checkpoint_interval = checkpoint_interval
        self._status_interval = status_interval
        self.counters: Counter[str] = Counter()
        self.engine: Engine | None = None
        self.store: StateStore | None = None
        self._stream: ReplicationStream | None = None
        self._sql: Conn | None = None
        self.flushed_lsn: Lsn | None = None
        self.checkpointed_lsn: Lsn | None = None

    # --- start: resume or bootstrap ------------------------------------------

    async def start(self) -> None:
        self._sql = await psycopg.AsyncConnection.connect(self._sink_dsn, autocommit=True)
        await ensure_watermark_table(self._sql)
        shapes = {}
        for sink in self.sinks:
            shapes[sink.table] = await describe_sink_table(self._sql, sink.table)
        async with await psycopg.AsyncConnection.connect(self._dsn, autocommit=True) as admin:
            if self._manage_publication:
                registry_sources = self.registry.sources
                await self._ensure_publication(admin, registry_sources)
            have_slot = await slot_exists(admin, self.slot)
        repl = await ReplicationConnection.connect(self._dsn, application_name="deltaflow")
        try:
            checkpoint = self.state.latest()
            reason = await self._why_not_resume(checkpoint, have_slot)
            if reason is None and checkpoint is not None:
                start_lsn = await self._resume(checkpoint, shapes)
            else:
                log.warning("bootstrapping from a fresh snapshot: %s", reason)
                start_lsn = await self._bootstrap(repl, have_slot, shapes)
            self._stream = ReplicationStream(
                repl,
                slot=self.slot,
                publications=[self.publication],
                start_lsn=start_lsn,
                status_interval=self._status_interval,
            )
            await self._stream.start()
            self._stream.ack(start_lsn)
        except BaseException:
            await repl.close()
            raise

    async def _ensure_publication(self, admin: Conn, tables: tuple[str, ...]) -> None:
        async with admin.cursor() as cur:
            await cur.execute(
                "SELECT schemaname || '.' || tablename FROM pg_publication_tables"
                " WHERE pubname = %s",
                (self.publication,),
            )
            current = {str(r[0]) for r in await cur.fetchall()}
        if current != set(tables):
            # Re-creating a publication is safe for a slot that is not
            # streaming; a changed set of tables also changes the views, which
            # forces a bootstrap anyway.
            await create_publication(admin, self.publication, tables)

    async def _why_not_resume(self, checkpoint: Any, have_slot: bool) -> str | None:
        if checkpoint is None:
            return "no state checkpoint"
        if not have_slot:
            return f"replication slot {self.slot} is missing"
        if checkpoint.meta.get("fingerprint") != fingerprint(self.registry):
            return "the views changed since the checkpoint"
        assert self._sql is not None
        for sink in self.sinks:
            watermark = await load_watermark(self._sql, self._views[id(sink)])
            if watermark is None:
                return f"no watermark for view {self._views[id(sink)]}"
            if watermark < checkpoint.lsn:
                # Cannot happen when only this runner writes: flushes precede
                # checkpoints. Someone else touched the sink.
                return (
                    f"watermark of {self._views[id(sink)]} ({watermark}) is behind the checkpoint"
                )
        return None

    async def _resume(self, checkpoint: Any, shapes: dict[str, Any]) -> Lsn:
        assert self._sql is not None
        self.store = self.state.open_live(memory_budget=self._memory_budget, restore=checkpoint)
        for sink in self.sinks:
            columns, key = shapes[sink.table]
            view = self._views[id(sink)]
            sink.bind(
                view=view, columns=columns, key=key, watermark=await load_watermark(self._sql, view)
            )
        self.engine = Engine(self.registry, self.store, resume=checkpoint.meta)
        self.flushed_lsn = min(s.watermark for s in self.sinks if s.watermark is not None)
        self.checkpointed_lsn = checkpoint.lsn
        self.counters["resumes"] += 1
        log.info(
            "resumed from checkpoint at %s; sink watermarks %s",
            checkpoint.lsn,
            {s.view: str(s.watermark) for s in self.sinks},
        )
        return Lsn(checkpoint.lsn)

    async def _bootstrap(
        self, repl: ReplicationConnection, have_slot: bool, shapes: dict[str, Any]
    ) -> Lsn:
        assert self._sql is not None
        if have_slot:
            await drop_slot(repl, self.slot)
        self.state.reset()
        self.store = self.state.open_live(memory_budget=self._memory_budget)
        for sink in self.sinks:
            columns, key = shapes[sink.table]
            sink.bind(view=self._views[id(sink)], columns=columns, key=key, watermark=None)
        self.engine = Engine(self.registry, self.store)
        info = await create_slot(repl, self.slot, export_snapshot=True)
        assert info.snapshot_name is not None
        crash_point("bootstrap:after_slot")
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            await load_snapshot(
                self.engine,
                conn,
                snapshot_name=info.snapshot_name,
                consistent_point=info.consistent_point,
                publication=self.publication,
            )
        await flush(self._sql, self.sinks, info.consistent_point, replace=True)
        crash_point("bootstrap:after_sink")
        self.flushed_lsn = info.consistent_point
        await self._checkpoint()
        self.counters["bootstraps"] += 1
        log.info("bootstrapped slot %s at %s", self.slot, info.consistent_point)
        return info.consistent_point

    # --- the loop -------------------------------------------------------------

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        assert self._stream is not None
        assert self.engine is not None
        pending = 0
        first_pending = 0.0
        last_checkpoint = time.monotonic()
        while not stop.is_set():
            timeout = self._flush_interval if pending else 0.5
            event = await self._next(timeout)
            now = time.monotonic()
            if isinstance(event, Transaction):
                self.engine.apply(event)
                self.counters["transactions"] += 1
                if not pending:
                    first_pending = now
                pending += 1
            due = pending and (
                event is None
                or pending >= self._max_flush
                or now - first_pending >= self._flush_interval
            )
            if due:
                await self._flush()
                pending = 0
            if (
                not pending
                and self._state_lsn() != self.checkpointed_lsn
                and now - last_checkpoint >= self._checkpoint_interval
            ):
                await self._checkpoint()
                last_checkpoint = now

    async def _next(self, timeout: float) -> Transaction | None:
        assert self._stream is not None
        try:
            async with asyncio.timeout(timeout):
                while True:
                    event = await anext(self._stream)
                    if isinstance(event, Transaction):
                        return event
        except TimeoutError:
            return None

    def _state_lsn(self) -> Lsn:
        """Where the operator state is: the engine's frontier."""
        assert self.engine is not None
        lsn = self.engine.frontier.lower
        assert lsn is not None
        return lsn

    async def _flush(self) -> None:
        """Write the sinks through the engine's frontier. While replaying
        transactions the sinks already hold (frontier at or below every
        watermark), there is nothing to write, and writing the watermark would
        move it *backwards*, so that later replays would be applied twice.
        The chaos suite caught exactly that. So a replay flush is a no-op."""
        assert self._sql is not None
        lsn = self._state_lsn()
        if self.flushed_lsn is not None and lsn <= self.flushed_lsn:
            self.counters["replayed_flushes_skipped"] += 1
            return
        written = await flush(self._sql, self.sinks, lsn)
        self.flushed_lsn = lsn
        self.counters["flushes"] += 1
        self.counters["rows_written"] += written

    async def _checkpoint(self) -> None:
        """Checkpoint the state at the engine's frontier E. Valid only if the
        sinks are written through E or beyond (E <= W): after a restore, the
        sinks then drop the replay of (E, W] and nothing is applied twice."""
        assert self.engine is not None
        assert self.store is not None
        lsn = self._state_lsn()
        if self.flushed_lsn is None or lsn > self.flushed_lsn:
            raise AssertionError(
                f"checkpoint at {lsn} would be ahead of the sinks ({self.flushed_lsn})"
            )
        crash_point("checkpoint:before")
        # In a thread, so the stream reader and heartbeats keep running. The
        # loop awaits it, so the engine applies nothing meanwhile and the
        # state stays exactly at `lsn`.
        await asyncio.to_thread(self.state.checkpoint, self.store, lsn, self.engine.metadata())
        self.checkpointed_lsn = lsn
        crash_point("checkpoint:after")
        if self._stream is not None:
            self._stream.ack(lsn)
        self.counters["checkpoints"] += 1

    async def close(self, *, final_checkpoint: bool = True) -> None:
        """Graceful stop: flush and checkpoint what was applied, so the next
        start replays nothing. (A crash skips all of this, by definition.)"""
        try:
            if (
                final_checkpoint
                and self.engine is not None
                and self._sql is not None
                and self.store
            ):
                await self._flush()
                if self._state_lsn() != self.checkpointed_lsn:
                    await self._checkpoint()
        finally:
            if self._stream is not None:
                await self._stream.close()
                self._stream = None
            if self.store is not None:
                self.store.close()
                self.store = None
            if self._sql is not None:
                await self._sql.close()
                self._sql = None

    async def __aenter__(self) -> DurableRunner:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close(final_checkpoint=exc_type is None)
