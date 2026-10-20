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

**Idle sources.** When the views' tables are quiet, no transaction arrives,
so without more the slot would never advance while the rest of the server
writes WAL, and the server would keep all of it. The stream therefore
reports progress between transactions (stream.py): a position P before
which every transaction has been delivered. When a checkpoint is due and
nothing is pending, the runner advances the engine to P, flushes the
watermark P (no rows), checkpoints at P and acknowledges it: the same
order of operations, so the same argument. Every later transaction commits
at or after P, so the frontier still admits it.

If there is no usable checkpoint, the runner bootstraps instead and says
so in the log and in `counters["bootstraps"]`. Reasons: first start, a
missing slot, a view over a table the checkpoint has no copy of, missing
watermarks.

**Bootstrapping in chunks** (decision 0009, the default). The slot is
created without a snapshot and the stream starts at once. Every table is
read a primary-key page at a time, each page with its own exported
snapshot, while the stream's transactions go into the arrangements in
between. No transaction stays open for longer than a page, so VACUUM
keeps working. The load is checkpointed like live state and the slot
acknowledged up to it: a stopped or crashed load continues where it was.
When every page is in and the stream is past the last page's consistent
point, the views are built from the arrangements and every sink's
contents are replaced at that position, which is where they go live.

**Read-your-writes.** `_deltaflow.wait_for` (sinks/postgres.py) emits a
non-transactional logical message. Every transaction committing before it
has been delivered by the time it arrives, so it is progress, and the
runner moves its watermark there at once (flushing anything pending
first), instead of at the next checkpoint while the sources are idle.

**Failover** (docs/failover.md). On Postgres 17+ the slot is a failover
slot, so a promoted standby has it. Every checkpoint records the server's
system identifier and timeline. Resumed on a new timeline, the runner reads
its history: a checkpoint past the point where its own timeline ended has
applied WAL the new primary never had, and the runner bootstraps instead of
resuming on top of rows that no longer exist.

**Large transactions.** The stream delivers a transaction of more than
`max_transaction_changes` changes in parts (replication/changes.py), and
each sink spills its buffers to the state store past `sink_spill_after`
(sinks/postgres.py): memory stays bounded however large a transaction is.
A transaction applied in part is not a boundary, so no checkpoint is taken
until its end. A crash or stop inside it replays it whole from the last
checkpoint.

**Changed views.** Views added or changed since the checkpoint do not
need a bootstrap: the engine builds them from the checkpoint's source
rows, as of its position (engine.py, per-view state). The runner then
replaces only their sinks' contents, with watermark C, and checkpoints
again at C. That checkpoint replaces the old one at C atomically (state/
durable.py), and the slot, the other views' state and their sinks are
untouched. A crash anywhere in between restarts from the old checkpoint
and rebuilds again, which is deterministic. Bootstrapping means: drop
the slot, create it again with an exported snapshot, load the snapshot,
*replace* each sink table's contents in one transaction, and checkpoint.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import ssl
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Literal

import psycopg
from psycopg import sql

from deltaflow.config import with_connect_timeout
from deltaflow.core import Frontier
from deltaflow.faults import crash_point
from deltaflow.observability.memory import rss
from deltaflow.observability.metrics import Registry
from deltaflow.planner import Deltaflow, PlanError
from deltaflow.replication.changes import Transaction, TransactionPart
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import Message
from deltaflow.replication.slot import (
    SystemInfo,
    create_publication,
    create_slot,
    diverged,
    drop_slot,
    identify_system,
    set_failover,
    slot_exists,
)
from deltaflow.replication.stream import Progress, ReplicationEndedError, ReplicationStream
from deltaflow.replication.wire import (
    PgError,
    ReplicationConnection,
    TlsUnavailableError,
    WireProtocolError,
)
from deltaflow.runtime.engine import Engine
from deltaflow.runtime.runner import DEFAULT_MEMORY_BUDGET
from deltaflow.runtime.snapshot import (
    REREAD_BATCH,
    ChunkReader,
    PartitionsChangedError,
    check_publication,
    describe_table,
    load_snapshot,
    partitioned_sources,
    partitions,
    removed_partitions,
)
from deltaflow.sinks.postgres import (
    SHARD_COLUMN,
    WAIT_PREFIX,
    PostgresSink,
    SinkError,
    describe_sink_table,
    ensure_watermark_table,
    flush,
    load_watermark,
)
from deltaflow.state.durable import StateDirectory
from deltaflow.state.store import StateStore

log = logging.getLogger(__name__)

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]

#: SQLSTATEs (classes, or one code) a restart can outlive: connection
#: exceptions, insufficient resources (too many connections), operator
#: intervention (a server shutting down, restarting or starting up), and a
#: slot still held by a walsender that has not noticed its client died.
_TRANSIENT_SQLSTATES = ("08", "53", "57", "55006")


def is_transient(exc: BaseException) -> bool:
    """Whether waiting and restarting can fix `exc`. Restarting is *safe*
    after any failure (it resumes from the last checkpoint), but after a
    misconfiguration, a drifted sink, a fenced shard or a bug it would only
    repeat the failure, so those are raised instead."""
    if isinstance(exc, PgError):
        return exc.sqlstate.startswith(_TRANSIENT_SQLSTATES)
    if isinstance(exc, psycopg.Error):
        # sqlstate is None when the client lost the connection by itself
        return exc.sqlstate is None or exc.sqlstate.startswith(_TRANSIENT_SQLSTATES)
    if isinstance(exc, ssl.SSLCertVerificationError | TlsUnavailableError | WireProtocolError):
        return False
    if isinstance(exc, PartitionsChangedError):
        return True  # starting again bootstraps, which reads the table as it is now
    return isinstance(
        exc, OSError | asyncio.IncompleteReadError | ReplicationEndedError
    )  # OSError covers ConnectionError, TimeoutError and ssl.SSLError


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
        checkpoint_interval: float = 10.0,
        status_interval: float = 10.0,
        sink_dsn: str | None = None,
        shard: int | None = None,
        fence: tuple[int, int] | None = None,
        live_name: str = "live",
        application_name: str = "deltaflow",
        snapshot: Literal["chunked", "single"] = "chunked",
        snapshot_chunk_rows: int = 20_000,
        max_transaction_changes: int = 10_000,
        sink_spill_after: int = 100_000,
        failover: bool | None = None,
    ) -> None:
        """With `shard`, this runner maintains that shard of a sharded
        registry: its sinks write partial rows to `<table>__shards`, its
        watermarks are per (view, shard), and every flush is fenced by the
        lease `fence=(shard, epoch)` (M7, runtime/cluster.py).

        `snapshot` is how a bootstrap reads the tables: `"chunked"`, pages of
        `snapshot_chunk_rows` rows, each with its own snapshot (decision
        0009); or `"single"`, everything in one transaction, which holds
        back VACUUM on the whole server while it runs.

        A transaction of more than `max_transaction_changes` changes is
        applied in parts, and a sink buffering more than `sink_spill_after`
        deltas or keys moves them to the state store: memory stays bounded
        however large a transaction (or a view's initial contents) is.

        `failover`: make the slot a failover slot. None (the default) does so
        on Postgres 17 and later, which have them."""
        if snapshot not in ("chunked", "single"):
            raise ValueError(f"snapshot must be 'chunked' or 'single', not {snapshot!r}")
        if snapshot_chunk_rows < 1:
            raise ValueError("snapshot_chunk_rows must be at least 1")
        for view in registry.views:
            if not isinstance(view.sink, PostgresSink):
                raise PlanError(
                    f"view {view.name!r}: a durable runner needs durable sinks (PostgresSink); "
                    f"{type(view.sink).__name__} cannot be restored after a crash"
                )
        self.registry = registry
        self.sinks = [v.sink for v in registry.views if isinstance(v.sink, PostgresSink)]
        suffix = "" if shard is None else f"@{shard}"
        self._views = {id(v.sink): f"{v.name}{suffix}" for v in registry.views}
        self._view_names = {id(v.sink): v.name for v in registry.views}
        self.shard = shard
        self._fence = fence
        #: every connection is tagged with it, so a cluster can find (and
        #: terminate) all of a dead worker's sessions
        self._app = application_name
        self._dsn = with_connect_timeout(dsn)
        self._sink_dsn = with_connect_timeout(sink_dsn or dsn)
        self.state = StateDirectory(state_dir, live=live_name)
        self.slot = slot
        self.publication = publication
        self._manage_publication = manage_publication
        self._memory_budget = memory_budget
        self._flush_interval = flush_interval
        self._max_flush = max_flush_transactions
        self._checkpoint_interval = checkpoint_interval
        self._status_interval = status_interval
        self._snapshot = snapshot
        self._chunk_rows = snapshot_chunk_rows
        self._max_txn_changes = max_transaction_changes
        self._spill_after = sink_spill_after
        self._failover = failover
        #: the slot is (to be) a failover slot, as decided at start
        self._failover_on = False
        #: the server this session runs on (IDENTIFY_SYSTEM)
        self.system: SystemInfo | None = None
        self.counters: Counter[str] = Counter()
        self.metrics = RunnerMetrics()
        #: (end_lsn, commit_time) of applied transactions not yet flushed
        self._pending_commits: list[tuple[Lsn, datetime]] = []
        self.slot_lag_bytes: int | None = None
        self.engine: Engine | None = None
        self.store: StateStore | None = None
        self._stream: ReplicationStream | None = None
        self._sql: Conn | None = None
        self.flushed_lsn: Lsn | None = None
        self.checkpointed_lsn: Lsn | None = None
        #: how the last start() went: "resumed" or "bootstrapped"; "loading"
        #: when stopped during a chunked load (the next start continues it)
        self.started_by: str | None = None
        #: the latest progress the stream reported (idle sources)
        self._progress_lsn: Lsn | None = None
        #: partitioned source -> its partitions, as the state knows them
        self._partitions: dict[str, list[str]] = {}
        #: someone waits for its writes (`_deltaflow.wait_for`): flush now
        self._wake = False

    # --- start: resume or bootstrap ------------------------------------------

    async def start(self, stop: asyncio.Event | None = None) -> None:
        """Resume, or bootstrap. A chunked bootstrap loads the tables before
        this returns; if `stop` is set meanwhile, the load is checkpointed and
        start returns with `started_by == "loading"`."""
        # A session: a restart replays from the checkpoint, so progress and
        # commit times of the previous session no longer apply.
        self._progress_lsn = None
        self._pending_commits.clear()
        self._sql = await psycopg.AsyncConnection.connect(
            self._sink_dsn, autocommit=True, application_name=self._app
        )
        await ensure_watermark_table(self._sql)
        shapes = {}
        for sink in self.sinks:
            shapes[sink.table] = await self._shape(sink)
        async with await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True, application_name=self._app
        ) as admin:
            if self._manage_publication:
                registry_sources = self.registry.sources
                await self._ensure_publication(admin, registry_sources)
            have_slot = await slot_exists(admin, self.slot)
            self._partitions = await self._current_partitions(admin)
            failover = await self._failover_wanted(admin)
            failover_now = have_slot and failover and not await _is_failover_slot(admin, self.slot)
        repl = await ReplicationConnection.connect(self._dsn, application_name=self._app)
        loading = False
        try:
            self.system = await identify_system(repl)
            if failover_now:
                await set_failover(repl, self.slot)
            checkpoint = self.state.latest()
            reason = await self._why_not_resume(checkpoint, have_slot)
            if reason is None and checkpoint is not None and "loading" in checkpoint.meta:
                start_lsn = self._resume_load(checkpoint, shapes)
                loading = True
            elif reason is None and checkpoint is not None:
                start_lsn = await self._resume(checkpoint, shapes)
                self.started_by = "resumed"
            else:
                log.warning(
                    "bootstrapping from a fresh snapshot: %s", reason, extra={"reason": reason}
                )
                if self._snapshot == "single":
                    start_lsn = await self._bootstrap(repl, have_slot, shapes)
                    self.started_by = "bootstrapped"
                else:
                    start_lsn = await self._begin_load(repl, have_slot, shapes)
                    loading = True
            self._stream = ReplicationStream(
                repl,
                slot=self.slot,
                publications=[self.publication],
                start_lsn=start_lsn,
                status_interval=self._status_interval,
                progress=True,
                max_transaction_changes=self._max_txn_changes,
            )
            await self._stream.start()
            self._stream.ack(start_lsn)
        except BaseException:
            await repl.close()
            raise
        if loading:
            await self._load(stop)

    def _write_table(self, sink: PostgresSink) -> str:
        return sink.table if self.shard is None else f"{sink.table}__shards"

    async def _shape(self, sink: PostgresSink) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The view's columns and key as the sink table defines them. A
        sharded partial table leads with the shard column, in its primary
        key too; that is the runner's business, not the view's."""
        assert self._sql is not None
        table = self._write_table(sink)
        columns, key = await describe_sink_table(self._sql, table)
        if self.shard is None:
            return columns, key
        if not columns or columns[0] != SHARD_COLUMN or not key or key[0] != SHARD_COLUMN:
            raise SinkError(
                f"{table}: a partial table must start with a {SHARD_COLUMN!r} column that "
                "leads its primary key"
            )
        return columns[1:], key[1:]

    async def _ensure_publication(self, admin: Conn, tables: tuple[str, ...]) -> None:
        """The publication covers exactly the views' tables. An existing one
        is altered, never dropped and recreated: a slot with WAL still to
        replay decodes it against the publication as it was at each
        position, and a recreated publication did not exist back then."""
        async with admin.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s", (self.publication,)
            )
            exists = await cur.fetchone() is not None
            await cur.execute(
                "SELECT schemaname || '.' || tablename FROM pg_publication_tables"
                " WHERE pubname = %s",
                (self.publication,),
            )
            current = {str(r[0]) for r in await cur.fetchall()}
        if not exists:
            await create_publication(admin, self.publication, tables)
            return
        await admin.execute(
            sql.SQL("ALTER PUBLICATION {} SET (publish_via_partition_root = true)").format(
                sql.Identifier(self.publication)
            )
        )
        for table, verb in (*((t, "ADD") for t in sorted(set(tables) - current)),
                            *((t, "DROP") for t in sorted(current - set(tables)))):  # fmt: skip
            await admin.execute(
                sql.SQL("ALTER PUBLICATION {} " + verb + " TABLE {}").format(
                    sql.Identifier(self.publication), sql.Identifier(*table.split(".", 1))
                )
            )

    async def _why_not_resume(self, checkpoint: Any, have_slot: bool) -> str | None:
        if checkpoint is None:
            return "no state checkpoint"
        if not have_slot:
            return f"replication slot {self.slot} is missing"
        assert self.system is not None
        meta = checkpoint.meta
        reason = (
            Engine.cannot_resume(self.registry, meta)
            or removed_partitions(meta.get("partitions", {}), self._partitions)
            or diverged(checkpoint.lsn, meta.get("system", {}), self.system)
        )
        if reason is not None or "loading" in meta:
            return reason  # a load's sinks are replaced when it goes live
        return await self._watermark_problem(checkpoint)

    async def _watermark_problem(self, checkpoint: Any) -> str | None:
        """Whether every kept view's sink is written through the checkpoint."""
        assert self._sql is not None
        rebuild = self._to_rebuild(checkpoint.meta)
        for sink in self.sinks:
            if self._view_names[id(sink)] in rebuild:
                continue  # replaced on resume
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

    def _to_rebuild(self, meta: dict[str, Any]) -> set[str]:
        if "views" not in meta:
            return set()
        return {v.name for v in Engine.views_to_rebuild(self.registry, meta)}

    async def _resume(self, checkpoint: Any, shapes: dict[str, Any]) -> Lsn:
        assert self._sql is not None
        self.store = self.state.open_live(memory_budget=self._memory_budget, restore=checkpoint)
        rebuild = self._to_rebuild(checkpoint.meta)
        for sink in self.sinks:
            fresh = self._view_names[id(sink)] in rebuild
            watermark = None if fresh else await load_watermark(self._sql, self._views[id(sink)])
            self._bind(sink, shapes, watermark)
        self.engine = Engine(self.registry, self.store, resume=checkpoint.meta, shard=self.shard)
        at = Lsn(checkpoint.lsn)
        if self.engine.rebuilt:
            rebuilt = [s for s in self.sinks if self._view_names[id(s)] in self.engine.rebuilt]
            await flush(self._sql, rebuilt, at, replace=True, fence=self._fence)
            self.counters["views_rebuilt"] += len(rebuilt)
            log.info(
                "built views %s from the checkpoint's source rows at %s; dropped %s",
                list(self.engine.rebuilt), at, list(self.engine.dropped),
            )  # fmt: skip
        self.flushed_lsn = min(s.watermark for s in self.sinks if s.watermark is not None)
        self.checkpointed_lsn = checkpoint.lsn
        if self.engine.rebuilt or self.engine.dropped:
            await self._checkpoint()  # the new views' state, at the same position
        self.counters["resumes"] += 1
        log.info(
            "resumed from checkpoint at %s; sink watermarks %s",
            checkpoint.lsn,
            {s.view: str(s.watermark) for s in self.sinks},
        )
        return Lsn(checkpoint.lsn)

    def _bind(self, sink: PostgresSink, shapes: dict[str, Any], watermark: Lsn | None) -> None:
        assert self.store is not None
        columns, key = shapes[sink.table]
        view = self._views[id(sink)]
        sink.bind(
            view=view,
            columns=columns,
            key=key,
            watermark=watermark,
            shard=self.shard,
            write_table=self._write_table(sink),
            spill=self.store.keyspace(f"spill:{view}"),
            spill_after=self._spill_after,
        )

    def _bind_fresh(self, shapes: dict[str, Any]) -> None:
        """Bind every sink without a watermark: its contents are replaced."""
        for sink in self.sinks:
            self._bind(sink, shapes, None)

    async def _begin_load(
        self, repl: ReplicationConnection, have_slot: bool, shapes: dict[str, Any]
    ) -> Lsn:
        """A chunked bootstrap: the slot, without a snapshot, and an engine
        waiting for pages. The load itself is `_load`."""
        if have_slot:
            await drop_slot(repl, self.slot)
        self.state.reset()
        self.store = self.state.open_live(memory_budget=self._memory_budget)
        self._bind_fresh(shapes)
        self.engine = Engine(self.registry, self.store, shard=self.shard, chunked=True)
        info = await create_slot(repl, self.slot, failover=self._failover_on)
        crash_point("bootstrap:after_slot")
        async with await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True, application_name=self._app
        ) as conn:
            schemas = []
            for table in self.engine.sources:
                schema = await describe_table(conn, table)
                await check_publication(conn, self.publication, schema)
                schemas.append(schema)
        self.engine.begin_load(schemas, info.consistent_point)
        self.flushed_lsn = None
        await self._checkpoint()  # from here on, a restart continues the load
        self.counters["loads"] += 1
        log.info(
            "loading %s in chunks; slot %s created at %s",
            list(self.engine.sources), self.slot, info.consistent_point,
        )  # fmt: skip
        return info.consistent_point

    def _resume_load(self, checkpoint: Any, shapes: dict[str, Any]) -> Lsn:
        self.store = self.state.open_live(memory_budget=self._memory_budget, restore=checkpoint)
        self._bind_fresh(shapes)
        self.engine = Engine(self.registry, self.store, resume=checkpoint.meta, shard=self.shard)
        self.flushed_lsn = None
        self.checkpointed_lsn = checkpoint.lsn
        self.counters["load_resumes"] += 1
        log.info("continuing the load from its checkpoint at %s", checkpoint.lsn)
        return Lsn(checkpoint.lsn)

    async def _load(self, stop: asyncio.Event | None) -> None:
        """Read pages and re-reads, applying the stream in between, until the
        engine is ready; then go live: build the views, replace every sink's
        contents at the frontier, and checkpoint."""
        engine = self.engine
        assert engine is not None
        assert self._stream is not None
        assert self._sql is not None
        last_checkpoint = time.monotonic()
        async with ChunkReader(self._dsn, slot=self.slot, application_name=self._app) as reader:
            while not engine.ready():
                await self._drain()
                if stop is not None and stop.is_set() and not engine.partial:
                    # Pages change the state without moving the frontier.
                    await self._checkpoint()
                    self.started_by = "loading"
                    log.info("load stopped at %s; the next start continues it", self._state_lsn())
                    return
                if (page := engine.next_page()) is not None:
                    schema, after = page
                    chunk = await reader.page(schema, after, self._chunk_rows)
                    engine.load_chunk(schema.table, chunk.rows, chunk.cp, after=chunk.after)
                    crash_point("load:after_chunk")
                elif rereads := engine.rereads():
                    for table, keys in rereads.items():
                        schema = engine.arrangements[table].schema
                        for i in range(0, len(keys), REREAD_BATCH):
                            batch = keys[i : i + REREAD_BATCH]
                            again = await reader.rows_of(schema, batch)
                            engine.reread(table, batch, again.rows, again.cp)
                else:
                    # Every page is in. Wait for the stream to pass the last
                    # page's consistent point, asking the server for its
                    # position in case the tables are idle.
                    await self._stream.send_status(reply_requested=True)
                    if (event := await self._next(0.5)) is not None:
                        self._apply_loading(event)
                    if not engine.partial:
                        self._advance_idle()
                due = time.monotonic() - last_checkpoint >= self._checkpoint_interval
                if due and not engine.partial:
                    self._advance_idle()
                    await self._checkpoint()
                    last_checkpoint = time.monotonic()
        lsn = self._state_lsn()
        engine.go_live()
        await flush(self._sql, self.sinks, lsn, replace=True, fence=self._fence)
        crash_point("bootstrap:after_sink")
        self.flushed_lsn = lsn
        await self._checkpoint()
        self.counters["bootstraps"] += 1
        self.started_by = "bootstrapped"
        log.info(
            "loaded %d rows in %d chunks; live at %s",
            engine.counters["snapshot_rows"], engine.counters["snapshot_chunks"], lsn,
        )  # fmt: skip

    async def _drain(self, budget: float = 0.5) -> None:
        """Apply what the stream has queued, for up to `budget` seconds."""
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            event = await self._next(0)
            if event is None:
                return
            self._apply_loading(event)

    def _apply_loading(self, event: Transaction | TransactionPart) -> None:
        assert self.engine is not None
        self.metrics.changes.inc(len(event.changes))
        if isinstance(event, TransactionPart):
            self.engine.apply_part(event)
            return
        self.engine.apply(event)
        self.counters["transactions"] += 1
        self.metrics.transactions.inc()

    async def _bootstrap(
        self, repl: ReplicationConnection, have_slot: bool, shapes: dict[str, Any]
    ) -> Lsn:
        assert self._sql is not None
        if have_slot:
            await drop_slot(repl, self.slot)
        self.state.reset()
        self.store = self.state.open_live(memory_budget=self._memory_budget)
        self._bind_fresh(shapes)
        self.engine = Engine(
            self.registry, self.store, shard=self.shard, snapshot_chunk_rows=self._chunk_rows
        )
        info = await create_slot(repl, self.slot, export_snapshot=True, failover=self._failover_on)
        assert info.snapshot_name is not None
        crash_point("bootstrap:after_slot")
        async with await psycopg.AsyncConnection.connect(
            self._dsn, application_name=self._app
        ) as conn:
            await load_snapshot(
                self.engine,
                conn,
                snapshot_name=info.snapshot_name,
                consistent_point=info.consistent_point,
                publication=self.publication,
            )
        await flush(self._sql, self.sinks, info.consistent_point, replace=True, fence=self._fence)
        crash_point("bootstrap:after_sink")
        self.flushed_lsn = info.consistent_point
        await self._checkpoint()
        self.counters["bootstraps"] += 1
        log.info("bootstrapped slot %s at %s", self.slot, info.consistent_point)
        return info.consistent_point

    # --- the loop -------------------------------------------------------------

    async def serve(
        self,
        stop: asyncio.Event,
        *,
        first_delay: float = 0.5,
        max_delay: float = 30.0,
        stable_after: float = 60.0,
        on_start: Callable[[], None] | None = None,
    ) -> None:
        """Start (unless started) and run until `stop`, restarting after
        transient failures (`is_transient`): a dropped connection, a server
        restarting, a slot not yet released. Each restart resumes from the
        last checkpoint, which keeps the results exactly-once (the argument
        at the top of this module). The delay doubles from `first_delay` up
        to `max_delay`, and starts over after a run of `stable_after`
        seconds. Every other failure is raised. `on_start` is called after
        every successful start."""
        delay = first_delay
        while not stop.is_set():
            began = time.monotonic()
            try:
                if self._stream is None:
                    await self.start()
                    if on_start is not None:
                        on_start()
                await self.run(stop)
                return
            except Exception as exc:
                if not is_transient(exc):
                    raise
                # Close what is left of the failed session; that may fail
                # too, on connections already gone, and must not mask `exc`.
                with contextlib.suppress(Exception):
                    await self.close(final_checkpoint=False)
                if time.monotonic() - began >= stable_after:
                    delay = first_delay
                self.counters["restarts"] += 1
                log.warning(
                    "pipeline interrupted (%r); restarting in %.1f s",
                    exc, delay, extra={"error": repr(exc), "delay": delay},
                )  # fmt: skip
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(delay):
                        await stop.wait()
                delay = min(delay * 2, max_delay)

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        monitor = asyncio.create_task(self._watch_slot(stop), name="deltaflow-slot-lag")
        try:
            await self._run(stop)
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)

    async def _watch_slot(self, stop: asyncio.Event, every: float = 2.0) -> None:
        """Slot lag: WAL the server retains for this slot, polled on its own
        connection so it never interleaves with a flush transaction."""
        async with await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True, application_name=self._app
        ) as conn:
            while not stop.is_set():
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint"
                        " FROM pg_replication_slots WHERE slot_name = %s",
                        (self.slot,),
                    )
                    row = await cur.fetchone()
                self.slot_lag_bytes = None if row is None or row[0] is None else int(str(row[0]))
                await asyncio.sleep(every)

    def stats(self) -> dict[str, Any]:
        """Everything the console shows, JSON-able."""
        engine, store, stream = self.engine, self.store, self._stream
        frontier = engine.frontier.lower if engine else None
        received = stream.received_lsn if stream else None
        lag = self.metrics.lag
        return {
            "time": datetime.now(UTC).isoformat(),
            "slot": self.slot,
            "frontier": str(frontier) if frontier is not None else None,
            "watermark": str(self.flushed_lsn) if self.flushed_lsn is not None else None,
            "checkpoint": str(self.checkpointed_lsn) if self.checkpointed_lsn is not None else None,
            # bytes of WAL received but not yet applied: the engine's backlog
            "apply_lag_bytes": int(received) - int(frontier)
            if received is not None and frontier is not None and received > frontier
            else 0,
            "slot_lag_bytes": self.slot_lag_bytes,
            "queued": stream.queued if stream else 0,
            "max_queued": stream.max_pending if stream else 0,
            "freshness_seconds": {
                "count": lag.count,
                "p50": lag.quantile(0.5) if lag.count else None,
                "p99": lag.quantile(0.99) if lag.count else None,
                "max": lag.max if lag.count else None,
            },
            "counters": dict(self.counters),
            "loading": engine.load_progress() if engine and engine.loading else None,
            "state": dataclasses.asdict(store.stats()) if store else {},
            "rss_bytes": rss().current,
            "graph": engine.graph() if engine else {"nodes": []},
        }

    def collect(self) -> Registry:
        """Bring the gauges up to date and return the registry (for /metrics)."""
        m = self.metrics
        stats = self.stats()
        for name in ("frontier", "watermark", "checkpoint"):
            if stats[name] is not None:
                m.lsn.labels(position=name).set(int(Lsn.parse(stats[name])))
        m.apply_lag.set(stats["apply_lag_bytes"])
        if stats["slot_lag_bytes"] is not None:
            m.slot_lag.set(stats["slot_lag_bytes"])
        m.queued.set(stats["queued"])
        m.rss.set(stats["rss_bytes"])
        for key in ("cache_usage", "memtable_bytes", "sst_bytes"):
            if key in stats["state"]:
                m.state.labels(kind=key).set(stats["state"][key])
        for node in stats["graph"]["nodes"]:
            if node["kind"] == "sink":
                continue
            m.op_deltas.labels(operator=node["id"], direction="in").value = node["deltas_in"]
            m.op_deltas.labels(operator=node["id"], direction="out").value = node["deltas_out"]
            m.op_seconds.labels(operator=node["id"]).value = node["seconds"]
        for name in (
            "bootstraps",
            "resumes",
            "checkpoints",
            "flushes",
            "idle_advances",
            "restarts",
            "views_rebuilt",
            "loads",
            "load_resumes",
            "transaction_parts",
        ):
            m.events.labels(event=name).value = self.counters[name]
        return m.registry

    async def _run(self, stop: asyncio.Event) -> None:
        assert self._stream is not None
        assert self.engine is not None
        if self.engine.loading:
            if stop.is_set():
                return
            raise RuntimeError("the load was stopped before it went live; start() continues it")
        pending = 0
        first_pending = 0.0
        last_checkpoint = time.monotonic()
        while not stop.is_set():
            timeout = self._flush_interval if pending else 0.5
            event = await self._next(timeout)
            now = time.monotonic()
            if isinstance(event, TransactionPart):
                # A large transaction, still arriving: nothing of it is
                # complete, so a flush leaves it out. Flushing earlier
                # transactions meanwhile is fine; a checkpoint is not.
                self.engine.apply_part(event)
                self.counters["transaction_parts"] += 1
                self.metrics.changes.inc(len(event.changes))
            elif isinstance(event, Transaction):
                self.engine.apply(event)
                self.counters["transactions"] += 1
                self.metrics.transactions.inc()
                self.metrics.changes.inc(len(event.changes))
                self._pending_commits.append((event.end_lsn, event.commit_time))
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
            if self._wake and not pending and not self.engine.partial:
                self._wake = False
                if self._advance_idle():
                    await self._flush()
                    self.counters["wakes"] += 1
            if (
                not pending
                and not self.engine.partial
                and now - last_checkpoint >= self._checkpoint_interval
            ):
                if self._advance_idle():
                    await self._flush()
                if self._state_lsn() != self.checkpointed_lsn:
                    await self._checkpoint()
                    last_checkpoint = now

    async def _next(self, timeout: float) -> Transaction | TransactionPart | None:
        assert self._stream is not None
        try:
            async with asyncio.timeout(timeout):
                while True:
                    event = await anext(self._stream)
                    if isinstance(event, Transaction | TransactionPart):
                        return event
                    if isinstance(event, Progress):
                        self._progress_lsn = event.lsn
                    elif (
                        isinstance(event, Message)
                        and not event.transactional
                        and event.prefix == WAIT_PREFIX
                    ):
                        # Every transaction committing before it has been
                        # delivered: progress, and a reason to flush now.
                        self._progress_lsn = max(self._progress_lsn or event.lsn, event.lsn)
                        self._wake = True
                        return None
        except TimeoutError:
            return None

    def _advance_idle(self) -> bool:
        """Advance the engine to the latest reported progress, if that is
        ahead of it. Called with nothing pending: every transaction before
        that position has been applied (the stream delivers in order), and
        any applied after it moved the frontier past it already."""
        assert self.engine is not None
        lsn = self._progress_lsn
        if lsn is None or lsn <= self._state_lsn():
            return False
        self.engine.advance(Frontier.at(lsn))
        self.counters["idle_advances"] += 1
        return True

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
        The chaos suite caught exactly that. So a replay flush is a no-op.

        Sinks need not agree: views rebuilt at the checkpoint (or loaded
        anew) are behind the others. A flush between the two writes only
        the sinks behind it; the others dropped those deltas as replays."""
        assert self._sql is not None
        lsn = self._state_lsn()
        if self.flushed_lsn is not None and lsn <= self.flushed_lsn:
            self.counters["replayed_flushes_skipped"] += 1
            self._pending_commits.clear()  # replays: already visible long ago
            return
        started = time.perf_counter()
        before = self.flushed_lsn
        behind = [s for s in self.sinks if s.watermark is None or s.watermark < lsn]
        written = await flush(self._sql, behind, lsn, fence=self._fence)
        self.metrics.flush_seconds.observe(time.perf_counter() - started)
        # End-to-end freshness of every transaction this flush made visible:
        # from its commit on the source to the sink's commit. Replayed
        # transactions at or below the previous watermark were visible long
        # before this flush, so they are not freshness samples.
        visible = datetime.now(UTC)
        for end, committed in self._pending_commits:
            if before is None or end > before:
                self.metrics.lag.observe(max(0.0, (visible - committed).total_seconds()))
        self._pending_commits.clear()
        self.metrics.rows_written.inc(written)
        self.flushed_lsn = lsn
        self.counters["flushes"] += 1
        self.counters["rows_written"] += written

    async def _checkpoint(self) -> None:
        """Checkpoint the state at the engine's frontier E. Valid only if the
        sinks are written through E or beyond (E <= W): after a restore, the
        sinks then drop the replay of (E, W] and nothing is applied twice."""
        assert self.engine is not None
        assert self.store is not None
        if self.engine.partial:
            raise AssertionError("checkpoint inside a transaction applied in part")
        lsn = self._state_lsn()
        # While loading, the sinks are not involved: they are replaced when
        # the load goes live.
        if not self.engine.loading and (self.flushed_lsn is None or lsn > self.flushed_lsn):
            raise AssertionError(
                f"checkpoint at {lsn} would be ahead of the sinks ({self.flushed_lsn})"
            )
        crash_point("checkpoint:before")
        # The run loop awaits this, so the engine applies nothing meanwhile and
        # the state stays at `lsn`. The worker thread does NOT keep the process
        # responsive: rocksdict holds the GIL for the whole checkpoint
        # (measured: the main thread made no progress during a 1.1 s
        # checkpoint), so heartbeats wait for it too. Timeouts (wal_sender,
        # cluster lease TTL) must therefore exceed the slowest checkpoint the
        # host produces; docs/operations.md.
        await self._check_partitions()
        started = time.perf_counter()
        assert self.system is not None
        meta = {
            **self.engine.metadata(),
            "partitions": self._partitions,
            "system": {"id": self.system.system_id, "timeline": self.system.timeline},
        }
        await asyncio.to_thread(self.state.checkpoint, self.store, lsn, meta)
        seconds = time.perf_counter() - started
        self.metrics.checkpoint_seconds.observe(seconds)
        log.info("checkpoint", extra={"lsn": str(lsn), "seconds": round(seconds, 3)})
        self.checkpointed_lsn = lsn
        crash_point("checkpoint:after")
        if self._stream is not None:
            self._stream.ack(lsn)
        self.counters["checkpoints"] += 1

    async def _failover_wanted(self, admin: Conn) -> bool:
        """Whether the slot should be a failover slot: asked for, or by
        default where the server has them. Warn when a failover could
        still lose the slot's position: without synchronized_standby_slots,
        the slot may get ahead of what a standby has received."""
        version = admin.info.server_version // 10000
        wanted = version >= 17 if self._failover is None else self._failover
        if wanted and version < 17:
            raise PlanError(f"failover slots need Postgres 17 or later; the server is {version}")
        if wanted:
            async with admin.cursor() as cur:
                await cur.execute("SHOW synchronized_standby_slots")
                row = await cur.fetchone()
            if not (row and row[0]):
                log.warning(
                    "synchronized_standby_slots is not set: a standby may lag behind what the "
                    "slot has sent, and after a failover the runner then bootstraps "
                    "(docs/failover.md)"
                )
        self._failover_on = wanted
        return wanted

    async def _current_partitions(self, conn: Conn) -> dict[str, list[str]]:
        tables = await partitioned_sources(conn, self.registry.sources)
        return {t: await partitions(conn, t) for t in tables}

    async def _check_partitions(self) -> None:
        """Before every checkpoint: a partition of a source detached or dropped
        since start took its rows away unseen. Stop; the next start bootstraps."""
        if not self._partitions:
            return
        assert self._sql is not None
        now = await self._current_partitions(self._sql)
        if (gone := removed_partitions(self._partitions, now)) is not None:
            raise PartitionsChangedError(gone)
        self._partitions = now  # added partitions: their rows arrive as inserts

    async def close(self, *, final_checkpoint: bool = True) -> None:
        """Graceful stop: flush and checkpoint what was applied, so the next
        start replays nothing. (A crash skips all of this, by definition.)"""
        try:
            # Inside a transaction applied in part, the state is no boundary:
            # the next start replays the transaction whole.
            if (
                final_checkpoint
                and self.engine is not None
                and not self.engine.partial
                and self._sql is not None
                and self.store
            ):
                if not self.engine.loading:
                    await self._flush()
                # A load's pages change the state without moving the frontier.
                if self.engine.loading or self._state_lsn() != self.checkpointed_lsn:
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


#: Freshness buckets, seconds: fine enough to read a p99 off them.
LAG_BUCKETS = (
    0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75,
    1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 30.0, 60.0,
)  # fmt: skip


async def _is_failover_slot(conn: Conn, slot: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute("SELECT failover FROM pg_replication_slots WHERE slot_name = %s", (slot,))
        row = await cur.fetchone()
    return bool(row and row[0])


class RunnerMetrics:
    """The runner's Prometheus metrics (served at /metrics)."""

    def __init__(self) -> None:
        r = self.registry = Registry()
        self.transactions = r.counter(
            "deltaflow_transactions_total", "Committed source transactions applied"
        )
        self.changes = r.counter(
            "deltaflow_source_changes_total", "Row changes (and messages) in applied transactions"
        )
        self.rows_written = r.counter("deltaflow_sink_rows_written_total", "Rows written to sinks")
        self.events = r.counter(
            "deltaflow_runner_events_total", "Bootstraps, resumes, checkpoints, flushes",
            labels=("event",),
        )  # fmt: skip
        self.lag = r.histogram(
            "deltaflow_end_to_end_lag_seconds",
            "Source commit to sink commit, per transaction (freshness)",
            buckets=LAG_BUCKETS,
        )
        self.flush_seconds = r.histogram("deltaflow_sink_flush_seconds", "Duration of sink flushes")
        self.checkpoint_seconds = r.histogram(
            "deltaflow_checkpoint_seconds", "Duration of state checkpoints"
        )
        self.lsn = r.gauge(
            "deltaflow_lsn",
            "Engine frontier, sink watermark and checkpoint LSN",
            labels=("position",),
        )
        self.apply_lag = r.gauge(
            "deltaflow_apply_lag_bytes", "WAL received but not yet applied by the engine"
        )
        self.slot_lag = r.gauge(
            "deltaflow_slot_lag_bytes", "WAL the server retains for the slot (current - confirmed)"
        )
        self.queued = r.gauge(
            "deltaflow_stream_queued", "Decoded transactions waiting for the engine"
        )
        self.state = r.gauge("deltaflow_state_bytes", "State store size", labels=("kind",))
        self.rss = r.gauge("deltaflow_resident_memory_bytes", "Resident set size of the process")
        self.op_deltas = r.counter(
            "deltaflow_operator_deltas_total", "Deltas into / out of each operator",
            labels=("operator", "direction"),
        )  # fmt: skip
        self.op_seconds = r.counter(
            "deltaflow_operator_seconds_total", "Time spent in each operator", labels=("operator",)
        )
