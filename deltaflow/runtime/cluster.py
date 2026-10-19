"""Scale-out: workers share N shards, coordinated through Postgres.

Every shard is an independent durable pipeline (DurableRunner) with its own
replication slot, checkpointed state and sink watermarks. A worker runs the
pipelines of the shards it owns. Ownership works as follows:

* **Membership.** Each worker upserts a heartbeat into `_deltaflow.workers`
  every `heartbeat` seconds, from a dedicated thread with its own
  connection. Shard pipelines share the worker's event loop, and a
  CPU-bound snapshot load can hold it for seconds; a heartbeat on that loop
  made busy workers look dead. A worker is live while its heartbeat is
  younger than `ttl`.
* **Assignment.** Shard s should be owned by `owner(s, live workers)`
  (rendezvous hashing, sharding.py). Every worker computes this on its own,
  from the same table, so they agree without a leader.
* **Mutual exclusion.** Owning shard s means holding the session-level
  advisory lock (LOCK_CLASS, s) on the worker's lock connection. Postgres
  releases it the moment that connection dies, which a `kill -9` causes
  immediately.
* **Fencing.** Taking a shard bumps its epoch in `_deltaflow.shard_leases`.
  Every sink flush of the shard checks the epoch under a share lock on that
  row (sinks/postgres.py), in the same transaction as its rows. A worker
  that lost a shard, but still runs because it is hung or partitioned, can
  therefore never commit another write.
* **Zombies.** If a shard's lock is held by a worker whose heartbeat has
  expired, the would-be owner terminates *all* of that worker's sessions.
  They are tagged `application_name = deltaflow:<worker>`. Terminating only
  the lock session is not enough: a flush the dead worker left open still
  holds its share lock on the lease row, and the epoch bump would wait for
  it. The epoch bump also runs under a lock timeout, so the coordinator
  loop never blocks on one. A shard's replication slot is taken from a
  stale holder the same way. Fencing covers anything still in flight.
* **Failures.** A shard pipeline that stops on its own (a lost
  connection, a missing sink table, a fenced flush) is released and taken
  again, at a new epoch, after a delay that doubles from 0.5 s up to 30 s
  and starts over once a pipeline has run for a minute. Without the delay,
  a lasting cause would bump the epoch and reconnect on every heartbeat.

Why not Raft: docs/decisions/0007-scale-out.md.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

from deltaflow.planner import Deltaflow
from deltaflow.replication.wire import PgError
from deltaflow.runtime.durable import DurableRunner
from deltaflow.sharding import owner

log = logging.getLogger(__name__)

type Conn = psycopg.AsyncConnection[tuple[Any, ...]]

#: advisory lock namespace (first key of pg_advisory_lock(int, int))
LOCK_CLASS = 0x0DE1F10

CLUSTER_DDL = """
CREATE SCHEMA IF NOT EXISTS _deltaflow;
CREATE TABLE IF NOT EXISTS _deltaflow.workers (
    worker text PRIMARY KEY,
    heartbeat_at timestamptz NOT NULL,
    lock_pid int NOT NULL
);
CREATE TABLE IF NOT EXISTS _deltaflow.shard_leases (
    shard int PRIMARY KEY,
    owner text NOT NULL,
    epoch bigint NOT NULL,
    since timestamptz NOT NULL
);
"""


_HEARTBEAT = (
    "INSERT INTO _deltaflow.workers (worker, heartbeat_at, lock_pid) VALUES (%s, now(), %s)"
    " ON CONFLICT (worker) DO UPDATE SET heartbeat_at = now(), lock_pid = EXCLUDED.lock_pid"
)


@dataclass
class _Owned:
    epoch: int
    stop: asyncio.Event
    task: asyncio.Task[None]
    started: float  # loop time


#: retry delays after a shard pipeline stopped on its own, seconds
RETRY_FIRST, RETRY_MAX, RETRY_STABLE_AFTER = 0.5, 30.0, 60.0


@dataclass
class ClusterStats:
    acquired: Counter[int] = field(default_factory=Counter)
    released: Counter[int] = field(default_factory=Counter)
    runner_failures: Counter[int] = field(default_factory=Counter)
    zombies_terminated: int = 0


class ClusterWorker:
    def __init__(
        self,
        make_registry: Callable[[], Deltaflow],
        dsn: str,
        *,
        worker_id: str,
        state_root: Path,
        slot_prefix: str,
        publication: str,
        heartbeat: float = 0.5,
        ttl: float = 3.0,
        runner_options: dict[str, Any] | None = None,
    ) -> None:
        """`make_registry` builds a fresh, sharded registry (one per shard
        pipeline, since sinks hold per-pipeline state)."""
        probe = make_registry()
        if probe.shards is None:
            raise ValueError(
                "a cluster needs a sharded registry: Deltaflow(shards=..., shard_by=...)"
            )
        self.shards = probe.shards
        self._make_registry = make_registry
        self._dsn = dsn
        self.worker_id = worker_id
        self._state_root = state_root
        self._slot_prefix = slot_prefix
        self._publication = publication
        self._heartbeat = heartbeat
        self._ttl = ttl
        self._runner_options = runner_options or {}
        self.owned: dict[int, _Owned] = {}
        #: shard -> (loop time before which it is not taken again, last delay)
        self._retry: dict[int, tuple[float, float]] = {}
        self.stats = ClusterStats()
        self._lock_conn: Conn | None = None
        self._lock_pid = 0
        self._app = f"deltaflow:{worker_id}"
        self._conn: Conn | None = None

    async def run(self, stop: asyncio.Event) -> None:
        self._conn = await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True, application_name=self._app
        )
        await self._conn.execute(CLUSTER_DDL)
        await self._connect_lock()
        beating = threading.Event()
        heartbeat = threading.Thread(
            target=self._beat, args=(beating,), name=f"heartbeat-{self.worker_id}", daemon=True
        )
        heartbeat.start()
        try:
            while not stop.is_set():
                try:
                    await self._tick()
                except psycopg.OperationalError as exc:
                    # Our lock connection was terminated: someone took us for
                    # dead. Every lease we held may be someone else's now,
                    # and fencing stops our pipelines from writing. Give them
                    # all up and start over.
                    log.warning(
                        "lock connection lost; dropping all shards",
                        extra={"worker": self.worker_id, "error": repr(exc)},
                    )
                    for shard in list(self.owned):
                        await self._release(shard, reason="lock connection lost")
                    await self._connect_lock()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self._heartbeat)
        finally:
            beating.set()
            await asyncio.to_thread(heartbeat.join)
            for shard in list(self.owned):
                await self._release(shard)
            with contextlib.suppress(psycopg.Error):
                # Leaving cleanly: the others need not wait for the TTL.
                await self._conn.execute(
                    "DELETE FROM _deltaflow.workers WHERE worker = %s", (self.worker_id,)
                )
            await self._conn.close()
            if self._lock_conn is not None:
                await self._lock_conn.close()

    async def _connect_lock(self) -> None:
        if self._lock_conn is not None:
            with contextlib.suppress(Exception):
                await self._lock_conn.close()
        self._lock_conn = await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True, application_name=self._app
        )
        self._lock_pid = self._lock_conn.info.backend_pid
        await self._conn_checked().execute(_HEARTBEAT, (self.worker_id, self._lock_pid))

    def _conn_checked(self) -> Conn:
        assert self._conn is not None
        return self._conn

    def _beat(self, stop: threading.Event) -> None:
        """The heartbeat thread: its own connection, independent of the loop."""
        conn: psycopg.Connection[tuple[Any, ...]] | None = None
        while not stop.wait(self._heartbeat):
            try:
                if conn is None or conn.closed:
                    conn = psycopg.connect(self._dsn, autocommit=True, application_name=self._app)
                conn.execute(_HEARTBEAT, (self.worker_id, self._lock_pid))
            except psycopg.Error as exc:
                log.warning(
                    "heartbeat failed", extra={"worker": self.worker_id, "error": repr(exc)}
                )
                conn = None
        if conn is not None:
            conn.close()

    async def _tick(self) -> None:
        assert self._conn is not None
        assert self._lock_conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute(
                "SELECT worker FROM _deltaflow.workers"
                " WHERE heartbeat_at > now() - make_interval(secs => %s)",
                (self._ttl,),
            )
            live = {str(r[0]) for r in await cur.fetchall()}
        desired = {s for s in range(self.shards) if owner(s, live) == self.worker_id}
        for shard, owned in list(self.owned.items()):
            if owned.task.done():  # the pipeline stopped on its own: failed, or fenced
                self.stats.runner_failures[shard] += 1
                error = None if owned.task.cancelled() else owned.task.exception()
                log.warning(
                    "shard pipeline stopped",
                    extra={"worker": self.worker_id, "shard": shard, "error": repr(error)},
                    exc_info=error,
                )
                await self._release(shard, reason="pipeline stopped")
                self._back_off(shard, owned)
        now = asyncio.get_running_loop().time()
        for shard in sorted(desired - set(self.owned)):
            if self._retry.get(shard, (0.0, 0.0))[0] <= now:
                await self._acquire(shard)
        for shard in sorted(set(self.owned) - desired):
            await self._release(
                shard, reason=f"now owned by {owner(shard, live)} (live {sorted(live)})"
            )

    def _back_off(self, shard: int, owned: _Owned) -> None:
        now = asyncio.get_running_loop().time()
        _, last = self._retry.get(shard, (0.0, 0.0))
        stable = now - owned.started >= RETRY_STABLE_AFTER
        delay = RETRY_FIRST if stable or not last else min(last * 2, RETRY_MAX)
        self._retry[shard] = (now + delay, delay)
        log.info(
            "retrying shard after a delay",
            extra={"worker": self.worker_id, "shard": shard, "delay": delay},
        )

    async def _acquire(self, shard: int) -> None:
        assert self._conn is not None
        assert self._lock_conn is not None
        async with self._lock_conn.cursor() as cur:
            await cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (LOCK_CLASS, shard))
            row = await cur.fetchone()
        if not (row and row[0]):
            await self._evict_stale_holder(shard)
            return  # try again on the next tick
        try:
            epoch = await self._bump_epoch(shard)
        except psycopg.errors.LockNotAvailable:
            # A flush of the previous owner still holds the lease row. Let go
            # of the lock and try again on the next tick, rather than block
            # the whole loop behind it.
            await self._lock_conn.execute("SELECT pg_advisory_unlock(%s, %s)", (LOCK_CLASS, shard))
            log.info("lease row busy, retrying", extra={"worker": self.worker_id, "shard": shard})
            await self._evict_previous_owner(shard)
            return
        stop = asyncio.Event()
        task = asyncio.create_task(self._run_shard(shard, epoch, stop), name=f"shard-{shard}")
        self.owned[shard] = _Owned(epoch, stop, task, asyncio.get_running_loop().time())
        self.stats.acquired[shard] += 1
        log.info("acquired shard", extra={"worker": self.worker_id, "shard": shard, "epoch": epoch})

    async def _evict_previous_owner(self, shard: int) -> None:
        """The lease row is held by a session of the shard's previous owner
        (a flush it never finished). If that owner is not live, terminate
        all of its sessions."""
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute(
                "SELECT pg_terminate_backend(a.pid) FROM pg_stat_activity a"
                " JOIN _deltaflow.shard_leases l ON a.application_name = 'deltaflow:' || l.owner"
                " WHERE l.shard = %s AND l.owner <> %s AND NOT EXISTS ("
                "   SELECT 1 FROM _deltaflow.workers w WHERE w.worker = l.owner"
                "   AND w.heartbeat_at > now() - make_interval(secs => %s))",
                (shard, self.worker_id, self._ttl),
            )
            terminated = len(await cur.fetchall())
        if terminated:
            self.stats.zombies_terminated += 1
            log.warning(
                "terminated sessions of the shard's previous owner",
                extra={"worker": self.worker_id, "shard": shard, "sessions": terminated},
            )

    async def _bump_epoch(self, shard: int) -> int:
        assert self._conn is not None
        async with self._conn.transaction(), self._conn.cursor() as cur:
            # The exclusive row lock this takes waits for any flush still
            # holding the old epoch's share lock, and every later flush of
            # the old owner sees the new epoch.
            await cur.execute("SET LOCAL lock_timeout = '2s'")
            await cur.execute(
                "INSERT INTO _deltaflow.shard_leases (shard, owner, epoch, since)"
                " VALUES (%s, %s, 1, now()) ON CONFLICT (shard) DO UPDATE"
                " SET owner = EXCLUDED.owner, epoch = _deltaflow.shard_leases.epoch + 1,"
                " since = now() RETURNING epoch",
                (shard, self.worker_id),
            )
            epoch_row = await cur.fetchone()
        assert epoch_row is not None
        return int(str(epoch_row[0]))

    async def _evict_stale_holder(self, shard: int) -> None:
        """Terminate every session of a lock holder whose heartbeat expired."""
        assert self._conn is not None
        async with self._conn.cursor() as cur:
            await cur.execute(
                "SELECT l.pid, w.worker FROM pg_locks l"
                " LEFT JOIN _deltaflow.workers w"
                "   ON w.lock_pid = l.pid AND w.heartbeat_at > now() - make_interval(secs => %s)"
                " WHERE l.locktype = 'advisory' AND l.classid = %s AND l.objid = %s"
                "   AND l.objsubid = 2 AND l.granted",
                (self._ttl, LOCK_CLASS, shard),
            )
            holder = await cur.fetchone()
            if holder is None or holder[1] is not None:
                return  # free by now, or held by a live worker
            # All of the stale worker's sessions, not just the lock: a flush
            # it left open would hold the lease row.
            await cur.execute(
                "SELECT pg_terminate_backend(a.pid) FROM pg_stat_activity a"
                " WHERE a.pid <> pg_backend_pid() AND (a.pid = %s OR (a.application_name LIKE"
                " 'deltaflow:%%' AND a.application_name ="
                " (SELECT application_name FROM pg_stat_activity WHERE pid = %s)))",
                (holder[0], holder[0]),
            )
            terminated = len(await cur.fetchall())
            self.stats.zombies_terminated += 1
            log.warning(
                "terminated stale holder of shard",
                extra={
                    "worker": self.worker_id,
                    "shard": shard,
                    "pid": holder[0],
                    "sessions": terminated,
                },
            )

    def _runner(self, shard: int, epoch: int) -> DurableRunner:
        return DurableRunner(
            self._make_registry(),
            self._dsn,
            state_dir=self._state_root / f"shard-{shard}",
            slot=f"{self._slot_prefix}_s{shard}",
            publication=self._publication,
            manage_publication=False,
            shard=shard,
            fence=(shard, epoch),
            live_name=f"live-{self.worker_id}",
            application_name=self._app,
            **self._runner_options,
        )

    async def _run_shard(self, shard: int, epoch: int, stop: asyncio.Event) -> None:
        for attempt in range(40):
            runner = self._runner(shard, epoch)
            try:
                await runner.start()
            except PgError as exc:
                await runner.close(final_checkpoint=False)
                # 55006: the slot is still held by the previous owner's
                # walsender, which may not have noticed its client died.
                if exc.sqlstate != "55006" or attempt == 39:
                    raise
                await self._take_slot(f"{self._slot_prefix}_s{shard}")
                await asyncio.sleep(0.25)
                continue
            clean = False
            try:
                await runner.run(stop)
                clean = True
            except Exception:
                log.exception(
                    "shard pipeline failed", extra={"worker": self.worker_id, "shard": shard}
                )
                raise
            finally:
                with contextlib.suppress(Exception):
                    await runner.close(final_checkpoint=clean)
            return

    async def _take_slot(self, slot: str) -> None:
        assert self._conn is not None
        await self._conn.execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots"
            " WHERE slot_name = %s AND active_pid IS NOT NULL",
            (slot,),
        )

    async def _release(self, shard: int, reason: str = "stopping") -> None:
        owned = self.owned.pop(shard)
        owned.stop.set()
        with contextlib.suppress(Exception):
            await owned.task
        assert self._lock_conn is not None
        with contextlib.suppress(psycopg.Error):
            await self._lock_conn.execute("SELECT pg_advisory_unlock(%s, %s)", (LOCK_CLASS, shard))
        self.stats.released[shard] += 1
        log.info(
            "released shard", extra={"worker": self.worker_id, "shard": shard, "reason": reason}
        )
