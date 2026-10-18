"""M5 acceptance: kill the worker hard at randomized points during a sustained
write load, restart it, and the sinks must converge to exactly the
full-recompute result, with no duplicate keys.

The worker (`deltaflow.devtools.worker`) is a real subprocess. A load runs
against the source tables the whole time. Two tests:

* `test_kill_9_at_random_moments`: the worker is killed with SIGKILL
  (TerminateProcess on Windows) at random times, ten times, and restarted
  each time. Kills land anywhere: during the snapshot bootstrap, between a
  sink commit and a checkpoint, in the middle of a checkpoint.
* `test_crash_at_each_critical_point`: some windows are microseconds wide,
  so random timing would almost never hit them. A crash point
  (`deltaflow.faults`) kills the worker exactly there:
  - after the slot is created, before the sink holds the snapshot;
  - with the state changed but the frontier not yet advanced;
  - before and after the sink transaction commits;
  - with a checkpoint's files written but not marked complete;
  - right after a checkpoint, before the slot is acknowledged.

Afterwards a final worker catches up past a marker transaction, and every
sink table is compared with its plain-SQL definition (worker.VIEW_SQL).
"""

from __future__ import annotations

import asyncio
import os
import random
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.devtools.loadgen import LoadStats, load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.worker import SINK_DDL
from deltaflow.devtools.workload import Workload
from deltaflow.faults import EXIT_CODE
from deltaflow.replication.slot import drop_slot, slot_exists
from deltaflow.replication.wire import ReplicationConnection
from deltaflow.sinks.postgres import ensure_watermark_table
from tests.worker_harness import emit_marker, sink_differences, wait_caught_up

KILLS = 10


@dataclass
class Harness:
    dsn: str
    state_dir: Path
    log: Path
    slot: str = field(default_factory=lambda: f"chaos_{uuid.uuid4().hex[:10]}")
    starts: int = 0

    def spawn(self, crash_at: str | None = None) -> subprocess.Popen[bytes]:
        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        env.pop("DELTAFLOW_CRASH_AT", None)
        if crash_at:
            env["DELTAFLOW_CRASH_AT"] = crash_at
        self.starts += 1
        log = open(self.log, "ab")  # noqa: SIM115 - handed to the child, closed below
        log.write(f"--- start {self.starts} crash_at={crash_at}\n".encode())
        log.flush()
        try:
            return subprocess.Popen(
                [
                    sys.executable, "-m", "deltaflow.devtools.worker",
                    "--dsn", self.dsn, "--state-dir", str(self.state_dir), "--slot", self.slot,
                    "--publication", "deltaflow_chaos", "--memory-budget", "16",
                    "--flush-interval", "0.05", "--checkpoint-interval", "0.3", "--http-port", "-1",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                env=env,
            )  # fmt: skip
        finally:
            log.close()

    def errors(self) -> str:
        """The worker log's error lines (the last few)."""
        lines = self.log.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join([ln for ln in lines if "Error" in ln][-5:]) or "(none)"

    def outcomes(self) -> dict[str, int]:
        text = self.log.read_text(encoding="utf-8", errors="replace")
        return {
            "starts": self.starts,
            "bootstrapped": text.count(" bootstrapped;"),
            "resumed": text.count(" resumed;"),
            "crash points hit": text.count("crash point "),
        }


async def prepare(dsn: str, seed: int) -> None:
    await seed_database(dsn, seed=seed, customers=300, orders=1_500)
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        await sql.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
        await sql.execute(SINK_DDL)
        await ensure_watermark_table(sql)
        await sql.execute("DELETE FROM _deltaflow.watermarks")


async def cleanup(dsn: str, slot: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        exists = await slot_exists(sql, slot)
    if exists:
        repl = await ReplicationConnection.connect(dsn)
        try:
            await drop_slot(repl, slot)
        finally:
            await repl.close()


async def sustained_load(dsn: str, seed: int, stop: asyncio.Event, stats: LoadStats) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        workload = Workload(seed, await load_state(conn))
        while not stop.is_set():
            await run_load(conn, workload, txns=10, stats=stats)
            await asyncio.sleep(0.01)


async def converge(
    h: Harness, stop_load: asyncio.Event, load: asyncio.Task[None]
) -> dict[str, Any]:
    """Start a final worker, stop the load, wait for the sinks to catch up,
    and compare them with the recompute."""
    final = h.spawn()
    try:
        await asyncio.sleep(2.0)  # some load while the final worker runs, too
        stop_load.set()
        await load
        async with await psycopg.AsyncConnection.connect(h.dsn, autocommit=True) as sql:
            marker = await emit_marker(sql)
            try:
                caught_up = await wait_caught_up(sql, marker, timeout=180)
            except AssertionError as exc:
                raise AssertionError(f"{exc}\nworker errors:\n{h.errors()}") from None
            problems = await sink_differences(sql)
    finally:
        final.kill()
        await asyncio.to_thread(final.wait)
    return {"caught_up_s": round(caught_up, 2), "problems": problems}


def test_kill_9_at_random_moments(dsn: str, chaos_seed: int, tmp_path: Path) -> None:
    rng = random.Random(chaos_seed)
    h = Harness(dsn, tmp_path / "state", tmp_path / "worker.log")
    stats = LoadStats()

    async def go() -> dict[str, Any]:
        await prepare(dsn, chaos_seed)
        stop = asyncio.Event()
        load = asyncio.create_task(sustained_load(dsn, chaos_seed, stop, stats))
        try:
            for _ in range(KILLS):
                worker = h.spawn()
                await asyncio.sleep(rng.uniform(0.5, 5.0))
                worker.kill()  # SIGKILL; TerminateProcess on Windows
                await asyncio.to_thread(worker.wait)
            return await converge(h, stop, load)
        finally:
            stop.set()
            await cleanup(dsn, h.slot)

    result = run(go())
    print(f"load: committed={stats.committed} statements={sum(stats.statements.values())}")
    print(f"worker: {h.outcomes()}; final catch-up {result['caught_up_s']}s")
    assert result["problems"] == [], "\n".join(result["problems"]) + f"\nlog: {h.log}"
    assert h.outcomes()["resumed"] >= 3, "too few kills landed after a checkpoint to test resume"


CRASH_POINTS = [
    ("bootstrap:after_slot", 1),
    ("bootstrap:after_sink", 1),
    ("apply:after_state", None),
    ("flush:before_commit", None),
    ("flush:after_commit", None),
    ("checkpoint:before_complete", None),
    ("checkpoint:after", None),
]


@pytest.mark.parametrize(("point", "hit"), CRASH_POINTS, ids=[p for p, _ in CRASH_POINTS])
def test_crash_at_each_critical_point(
    dsn: str, chaos_seed: int, tmp_path: Path, point: str, hit: int | None
) -> None:
    rng = random.Random(f"{chaos_seed}:{point}")
    h = Harness(dsn, tmp_path / "state", tmp_path / "worker.log")
    stats = LoadStats()
    nth = hit if hit is not None else rng.randint(2, 6)

    async def go() -> dict[str, Any]:
        await prepare(dsn, chaos_seed)
        stop = asyncio.Event()
        load = asyncio.create_task(sustained_load(dsn, chaos_seed, stop, stats))
        try:
            if not point.startswith("bootstrap"):
                # Let a first worker bootstrap and checkpoint, then kill it, so
                # the crashing worker is a resumed one.
                first = h.spawn()
                await asyncio.sleep(4.0)
                first.kill()
                await asyncio.to_thread(first.wait)
            crashing = h.spawn(crash_at=f"{point}:{nth}")
            code = await asyncio.wait_for(asyncio.to_thread(crashing.wait), timeout=120)
            assert code == EXIT_CODE, f"worker exited {code}, not at the crash point; see {h.log}"
            return await converge(h, stop, load)
        finally:
            stop.set()
            await cleanup(dsn, h.slot)

    result = run(go())
    print(f"{point} hit {nth}: worker {h.outcomes()}; load committed={stats.committed}")
    assert result["problems"] == [], "\n".join(result["problems"]) + f"\nlog: {h.log}"
