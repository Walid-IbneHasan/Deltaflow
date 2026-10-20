"""Fencing and zombie eviction: the parts of scale-out a `kill -9` cannot
show, because a killed process cannot attempt anything afterwards."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.core import Delta, Frontier, Row
from deltaflow.devtools.cluster import PARTIAL_DDL, SCHEMA, setup, views
from deltaflow.devtools.seed import seed_database
from deltaflow.replication.lsn import Lsn
from deltaflow.runtime.cluster import CLUSTER_DDL, LOCK_CLASS, ClusterWorker
from deltaflow.sinks import PostgresSink
from deltaflow.sinks.postgres import FencedError, ensure_watermark_table, flush, load_watermark
from tests.integration.test_durable_runner import until

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]


def test_a_flush_with_a_stale_epoch_is_refused_and_writes_nothing(dsn: str) -> None:
    async def go() -> tuple[Lsn | None, int]:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await ensure_watermark_table(sql)
            await sql.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            await sql.execute(PARTIAL_DDL)
            await sql.execute(CLUSTER_DDL)
            await sql.execute("DELETE FROM _deltaflow.shard_leases WHERE shard = 900")
            await sql.execute("DELETE FROM _deltaflow.watermarks WHERE view = 'fence@900'")
            await sql.execute(
                "INSERT INTO _deltaflow.shard_leases VALUES (900, 'new-owner', 2, now())"
            )
            sink = PostgresSink(f"{SCHEMA}.customer_statuses")
            sink.bind(
                view="fence@900",
                columns=("customer_id", "status"),
                key=("customer_id", "status"),
                watermark=None,
                shard=900,
                write_table=f"{SCHEMA}.customer_statuses__shards",
            )
            sink.accept([Delta(Row({"customer_id": 1, "status": "paid"}), Lsn(1), 1)])
            sink.advance(Frontier.at(Lsn(2)))
            with pytest.raises(FencedError, match="epoch 1"):
                await flush(sql, [sink], Lsn(2), fence=(900, 1))  # the old owner's epoch
            async with sql.cursor() as cur:
                await cur.execute(f"SELECT count(*) FROM {SCHEMA}.customer_statuses__shards")
                row = await cur.fetchone()
            return await load_watermark(sql, "fence@900"), int(str(row[0])) if row else -1

    watermark, rows = run(go())
    assert watermark is None
    assert rows == 0


def test_a_hung_lock_holder_is_evicted_and_its_shard_taken_over(dsn: str, tmp_path: Path) -> None:
    """A "zombie" holds shard 0's advisory lock but has stopped heartbeating.
    A live worker that should own shard 0 terminates the zombie's lock
    backend and takes the shard, at a higher epoch, which fences anything
    the zombie might still try to write."""
    prefix = f"it_cl_{uuid.uuid4().hex[:6]}"

    async def go() -> dict[str, Any]:
        await seed_database(dsn, seed=1, customers=20, orders=60)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            await setup(dsn, 1, "deltaflow_it_cluster")
            await sql.execute("DELETE FROM _deltaflow.workers")
            await sql.execute("DELETE FROM _deltaflow.shard_leases")
            zombie = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
            await zombie.execute("SELECT pg_advisory_lock(%s, 0)", (LOCK_CLASS,))
            await sql.execute(
                "INSERT INTO _deltaflow.workers VALUES ('zombie', now() - interval '1 minute', %s)",
                (zombie.info.backend_pid,),
            )
            await sql.execute("INSERT INTO _deltaflow.shard_leases VALUES (0, 'zombie', 5, now())")
            worker = ClusterWorker(
                lambda: views(1), dsn, worker_id="live", state_root=tmp_path, slot_prefix=prefix,
                publication="deltaflow_it_cluster", heartbeat=0.2, ttl=1.0,
            )  # fmt: skip
            stop = asyncio.Event()
            task = asyncio.create_task(worker.run(stop))
            try:
                async with asyncio.timeout(60):
                    while True:
                        await asyncio.sleep(0.2)
                        async with sql.cursor() as cur:
                            await cur.execute("SELECT owner, epoch FROM _deltaflow.shard_leases")
                            lease = await cur.fetchone()
                        if lease and lease[0] == "live":
                            break
                zombie_alive = True
                try:
                    await zombie.execute("SELECT 1")
                except psycopg.OperationalError:
                    zombie_alive = False
                return {
                    "lease": lease,
                    "zombie_alive": zombie_alive,
                    "evictions": worker.stats.zombies_terminated,
                }
            finally:
                stop.set()
                await task
                await zombie.close()
                await sql.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
                    " WHERE slot_name LIKE %s AND NOT active",
                    (f"{prefix}%",),
                )

    outcome = run(go())
    assert outcome["lease"] == ("live", 6)  # one past the zombie's epoch: it is fenced
    assert outcome["zombie_alive"] is False
    assert outcome["evictions"] == 1


def test_a_failing_shard_is_retried_with_backoff(dsn: str, tmp_path: Path) -> None:
    """A shard whose pipeline cannot start (here its partial sink table is
    missing) is retried, but with a doubling delay: not on every heartbeat,
    which would bump its epoch and reconnect several times a second for as
    long as the cause lasts."""
    prefix = f"it_cl_{uuid.uuid4().hex[:6]}"

    async def go() -> tuple[int, int]:
        await seed_database(dsn, seed=1, customers=20, orders=60)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            await setup(dsn, 1, "deltaflow_it_cluster")
            await sql.execute("DELETE FROM _deltaflow.workers")
            await sql.execute("DELETE FROM _deltaflow.shard_leases")
            await sql.execute(f"DROP TABLE {SCHEMA}.order_totals__shards CASCADE")
            worker = ClusterWorker(
                lambda: views(1), dsn, worker_id="live", state_root=tmp_path, slot_prefix=prefix,
                publication="deltaflow_it_cluster", heartbeat=0.1, ttl=1.0,
            )  # fmt: skip
            stop = asyncio.Event()
            task = asyncio.create_task(worker.run(stop))
            try:
                # Watch from the first failure: getting there takes longer on
                # a loaded machine, and is not what is tested.
                await until(lambda: worker.stats.runner_failures[0] >= 1)
                await asyncio.sleep(4.0)
            finally:
                stop.set()
                await task
                await sql.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
                    " WHERE slot_name LIKE %s AND NOT active",
                    (f"{prefix}%",),
                )
        return worker.stats.acquired[0], worker.stats.runner_failures[0]

    acquired, failures = run(go())
    assert failures >= 2  # it kept trying
    # retries 0.5, 1 and 2 s apart fit in the 4 s after the first failure; one try
    # per 0.1 s heartbeat would be ~40
    assert acquired <= 5, acquired
