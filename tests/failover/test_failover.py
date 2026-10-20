"""A failover under load (Postgres 17, docker/failover): the runner reaches
the promoted standby through its host list, finds its slot there (a
failover slot, synced by the standby), resumes from its checkpoint without
a bootstrap, and its views equal their SQL on the new primary."""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.devtools.loadgen import load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.worker import SINK_DDL, views
from deltaflow.devtools.workload import Workload
from deltaflow.runtime import DurableRunner
from deltaflow.sinks.postgres import ensure_watermark_table
from tests.integration.test_durable_runner import until
from tests.worker_harness import emit_marker, sink_differences, wait_caught_up

PRIMARY = int(os.environ.get("DELTAFLOW_FAILOVER_PRIMARY_PORT", "55441"))
STANDBY = int(os.environ.get("DELTAFLOW_FAILOVER_STANDBY_PORT", "55442"))
AUTH = "user=deltaflow password=deltaflow dbname=deltaflow_test"
COMPOSE = Path(__file__).resolve().parents[2] / "docker" / "failover" / "compose.yml"
SLOT = "failover_worker"

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]


def dsn(port: int) -> str:
    return f"host=localhost port={port} {AUTH}"


#: both servers, the primary wanted: what an application would configure
EITHER = f"host=localhost,localhost port={PRIMARY},{STANDBY} {AUTH} target_session_attrs=read-write"
#: the test's own connections, with a connect timeout as docs/failover.md
#: advises: Docker Desktop accepts connections to a dead container's port
#: and then says nothing, and libpq would wait on it for minutes
CLIENT = f"{EITHER} connect_timeout=5"


async def load(target: str, seed: int, txns: int) -> None:
    async with await psycopg.AsyncConnection.connect(target, autocommit=True) as conn:
        await run_load(conn, Workload(seed, await load_state(conn)), txns=txns)


async def synced(standby: Sql) -> bool:
    async with standby.cursor() as cur:
        await cur.execute(
            "SELECT synced, failover FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)
        )
        row = await cur.fetchone()
    return row is not None and bool(row[0]) and bool(row[1])


def test_a_failover_under_load_resumes_exactly_on_the_promoted_standby(tmp_path: Path) -> None:
    async def go() -> dict[str, int]:
        await seed_database(dsn(PRIMARY), seed=3, customers=200, orders=1_000)
        async with await psycopg.AsyncConnection.connect(dsn(PRIMARY), autocommit=True) as sql:
            await sql.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
            await sql.execute(SINK_DDL)
            await ensure_watermark_table(sql)
        runner = DurableRunner(
            views(), EITHER, state_dir=tmp_path, slot=SLOT, publication="failover_pub",
            flush_interval=0.05, checkpoint_interval=0.3, status_interval=0.5,
        )  # fmt: skip
        stop = asyncio.Event()
        task = asyncio.create_task(runner.serve(stop, first_delay=0.2, max_delay=2.0))
        try:
            await until(lambda: runner.started_by == "bootstrapped", timeout=120)
            await load(CLIENT, seed=1, txns=200)
            async with await psycopg.AsyncConnection.connect(dsn(PRIMARY), autocommit=True) as p:
                await wait_caught_up(p, await emit_marker(p))
            async with await psycopg.AsyncConnection.connect(dsn(STANDBY), autocommit=True) as s:
                async with asyncio.timeout(120):
                    while not await synced(s):  # noqa: ASYNC110 - polling another server
                        await asyncio.sleep(0.5)
            await load(CLIENT, seed=2, txns=100)

            # The primary dies; the standby is promoted.
            restarts = runner.counters["restarts"]
            killed = time.monotonic()
            await asyncio.to_thread(
                subprocess.run,
                ["docker", "compose", "-f", str(COMPOSE), "kill", "primary"],
                check=True,
                capture_output=True,
            )
            async with await psycopg.AsyncConnection.connect(dsn(STANDBY), autocommit=True) as s:
                await s.execute("SELECT pg_promote(wait => true)")
            await until(lambda: runner.counters["restarts"] > restarts, timeout=60)
            await until(lambda: runner.counters["resumes"] >= 1, timeout=120)
            resumed_after = time.monotonic() - killed

            await load(CLIENT, seed=3, txns=150)
            async with await psycopg.AsyncConnection.connect(dsn(STANDBY), autocommit=True) as s:
                await wait_caught_up(s, await emit_marker(s), timeout=120)
                assert await sink_differences(s) == []
            assert runner.system is not None
            assert runner.system.timeline == 2  # running on the promoted standby
        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=60)
            await runner.close()
        return {**runner.counters, "resumed_after_s": round(resumed_after)}

    counters = run(go())
    print(f"failover: {counters}")
    assert counters["bootstraps"] == 1, counters  # the first start only: then resumed
