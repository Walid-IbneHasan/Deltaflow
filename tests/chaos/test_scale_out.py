"""M7 acceptance: killing a worker mid-load redistributes its shards, and the
result still matches the full recompute.

Three cluster workers (`deltaflow.devtools.cluster`, real subprocesses)
share six shards under a sustained load. After every shard has an owner
and every view's watermark exists on every shard, the worker owning the
most shards is killed with SIGKILL (TerminateProcess on Windows). The test
then requires that:

* its shards are leased again by the survivors, at a higher epoch, so the
  dead owner is fenced;
* after the load stops, every (view, shard) watermark passes a marker
  committed after the load;
* every merged view equals its plain-SQL recompute, as a multiset;
* every partial row sits on the shard its shard key hashes to.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.devtools.cluster import SCHEMA, VIEW_SQL
from deltaflow.devtools.loadgen import LoadStats
from deltaflow.devtools.seed import seed_database
from deltaflow.replication.lsn import Lsn
from deltaflow.sharding import shard_of
from tests.chaos.test_kill9 import sustained_load
from tests.sql_oracle import first_difference, query_multiset
from tests.worker_harness import emit_marker

SHARDS = 6
WORKERS = ("w1", "w2", "w3")
VIEWS = ("orders_per_country", "order_details", "order_totals", "customer_statuses")

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]


def cluster_cmd(dsn: str, prefix: str, state: Path, *extra: str) -> list[str]:
    return [
        sys.executable, "-m", "deltaflow.devtools.cluster", "--dsn", dsn,
        "--shards", str(SHARDS), "--slot-prefix", prefix,
        "--publication", "deltaflow_chaos_cluster",
        "--state-root", str(state), "--ttl", "3", "--heartbeat", "0.5",
        "--checkpoint-interval", "1", "--flush-interval", "0.05", "--memory-budget", "16",
        *extra,
    ]  # fmt: skip


async def leases(sql: Sql) -> dict[int, tuple[str, int]]:
    async with sql.cursor() as cur:
        await cur.execute("SELECT shard, owner, epoch FROM _deltaflow.shard_leases")
        return {int(s): (str(o), int(e)) for s, o, e in await cur.fetchall()}


async def watermarks(sql: Sql) -> dict[str, Lsn]:
    async with sql.cursor() as cur:
        await cur.execute("SELECT view, lsn::text FROM _deltaflow.watermarks WHERE view LIKE '%@%'")
        return {str(v): Lsn.parse(str(lsn)) for v, lsn in await cur.fetchall()}


async def wait_for(what: str, check: Any, timeout: float = 120) -> float:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if await check():
            return time.monotonic() - started
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def test_killing_a_worker_redistributes_its_shards_and_the_result_matches(  # noqa: PLR0915
    dsn: str, chaos_seed: int, tmp_path: Path
) -> None:
    prefix = f"chaos_cl_{uuid.uuid4().hex[:6]}"
    state = tmp_path / "cluster"
    log = tmp_path / "workers.log"
    stats = LoadStats()
    report: dict[str, Any] = {}

    def spawn(worker: str) -> subprocess.Popen[bytes]:
        with open(log, "ab") as out:
            return subprocess.Popen(
                cluster_cmd(dsn, prefix, state, "--worker-id", worker),
                stdout=out,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )

    async def go() -> None:
        await seed_database(dsn, seed=chaos_seed, customers=300, orders=1_500)
        await asyncio.to_thread(
            subprocess.run,
            cluster_cmd(dsn, prefix, state, "--reset"),
            check=True,
            capture_output=True,
        )
        procs = {w: spawn(w) for w in WORKERS}
        stop = asyncio.Event()
        load = asyncio.create_task(sustained_load(dsn, chaos_seed, stop, stats))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:

                async def all_owned() -> bool:
                    current = await leases(sql)
                    marks = await watermarks(sql)
                    return len(current) == SHARDS and len(marks) == SHARDS * len(VIEWS)

                report["startup_s"] = round(await wait_for("all shards running", all_owned), 1)
                await asyncio.sleep(2)  # some load with everything running
                before = await leases(sql)
                owners = [o for o, _ in before.values()]
                victim = max(WORKERS, key=owners.count)
                lost = sorted(s for s, (o, _) in before.items() if o == victim)
                report.update(victim=victim, victim_shards=lost, owners_before=before)

                procs[victim].kill()  # SIGKILL / TerminateProcess
                await asyncio.to_thread(procs[victim].wait)
                killed_at = time.monotonic()

                async def taken_over() -> bool:
                    now = await leases(sql)
                    return all(now[s][0] != victim and now[s][1] > before[s][1] for s in lost)

                await wait_for("the victim's shards to be leased again", taken_over)
                report["failover_s"] = round(time.monotonic() - killed_at, 1)
                report["owners_after"] = await leases(sql)

                await asyncio.sleep(3)  # load continues on the survivors
                stop.set()
                await load
                marker = await emit_marker(sql)

                async def caught_up() -> bool:
                    marks = await watermarks(sql)
                    return len(marks) == SHARDS * len(VIEWS) and min(marks.values()) > marker

                report["catch_up_s"] = round(
                    await wait_for("all shards past the marker", caught_up), 1
                )
                problems = []
                for table, query in VIEW_SQL.items():
                    actual = await query_multiset(sql, f"SELECT * FROM {table}")
                    expected = await query_multiset(sql, query)
                    if actual != expected:
                        problems.append(f"{table}: {first_difference(actual, expected)}")
                    report.setdefault("rows", {})[table] = expected.size()
                async with sql.cursor() as cur:
                    await cur.execute(
                        f"SELECT shard, customer_id FROM {SCHEMA}.order_details__shards"
                    )
                    misplaced = [r for r in await cur.fetchall() if shard_of(r[1], SHARDS) != r[0]]
                report["problems"] = problems
                report["misplaced"] = len(misplaced)
        finally:
            stop.set()
            for p in procs.values():
                if p.poll() is None:
                    p.kill()
                    await asyncio.to_thread(p.wait)
            await asyncio.to_thread(
                subprocess.run, cluster_cmd(dsn, prefix, state, "--reset"), capture_output=True
            )

    run(go())
    print(f"scale-out: {report}; load committed={stats.committed}")
    assert report["problems"] == [], "\n".join(report["problems"]) + f"\nlog: {log}"
    assert report["misplaced"] == 0
    assert report["victim_shards"], "the victim owned no shards"
