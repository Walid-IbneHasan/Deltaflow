"""M3 acceptance, part 2: resident memory stays under the configured budget
while the state is several times larger than it.

The parent seeds a dataset whose operator state is several times the budget.
Customers carry large incompressible bios, which end up twice in the state:
once in the customers arrangement, once in the join's right-hand index. The
parent then starts the engine in a child process
(`memory_child.py`), runs a randomized load while the child follows the
stream, and finally sends an end marker. The child compares its view with
SQL and reports its memory.

What is asserted, and why:

* `peak_rss - baseline_rss <= budget + UNBUDGETED`. The baseline is the
  child's resident set after imports and one throwaway connection of each
  kind (which initialises the TLS libraries once), before the engine
  exists. The peak is
  the OS's high-water mark for the whole run, snapshot included. The budget
  bounds RocksDB's cache, memtables and index/filter blocks. UNBUDGETED
  covers what the budget does not:
  - the Python heap in flight (measured with tracemalloc: at most 9.7 MiB);
  - RocksDB allocations outside its cache;
  - allocator slack;
  - the in-memory sink's result.
  It is 1.5x the largest excess measured at this budget. docs/join.md §6
  has the measurements: the sweep over budgets and the run with doubled
  state.
* `sst_bytes >= 4 * budget`: the state really is much larger than memory,
  so the bound is not met just because everything fits.
* the view equals its SQL: bounded memory did not cost correctness.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.devtools.loadgen import LoadStats, load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import Workload, WorkloadConfig
from deltaflow.observability.memory import describe
from tests.differential.lockstep import MARKER

MB = 1 << 20
BUDGET = 32 * MB
UNBUDGETED = 32 * MB
CUSTOMERS = 10_000
ORDERS = 100_000
LOAD_TXNS = 3_000
#: Most customers get an out-of-line bio of 8-24 kB: ~100 MB of bios.
HEAVY = WorkloadConfig(toast_probability=0.6, null_probability=0.1)


def test_resident_memory_stays_under_budget_with_state_far_larger(
    dsn: str, workload_seed: int
) -> None:
    report, load = run(_scenario(dsn, workload_seed))
    growth = report["peak_rss"] - report["baseline_rss"]
    print(
        f"budget {describe(BUDGET)} + unbudgeted allowance {describe(UNBUDGETED)}; "
        f"state on disk {describe(report['sst_bytes'])}; "
        f"rss baseline {describe(report['baseline_rss'])}, "
        f"after snapshot {describe(report['after_snapshot_rss'])}, "
        f"peak {describe(report['peak_rss'])} (growth {describe(growth)})"
    )
    print(
        f"snapshot {report['snapshot_seconds']}s in {report['snapshot_chunks']} chunks; "
        f"source rows {report['source_rows']}; {report['transactions']} transactions "
        f"({load.committed} committed by the load); view rows {report['view_rows']}"
    )
    assert report["matches_sql"], report["difference"]
    assert report["sst_bytes"] >= 4 * BUDGET, "state is not much larger than the budget"
    assert report["transactions"] > load.committed  # all of them, plus the marker
    assert growth <= BUDGET + UNBUDGETED, (
        f"resident memory grew by {describe(growth)}, over budget "
        f"{describe(BUDGET)} + unbudgeted allowance {describe(UNBUDGETED)}"
    )


async def _scenario(dsn: str, seed: int) -> tuple[dict[str, Any], LoadStats]:
    await seed_database(dsn, seed=seed, customers=CUSTOMERS, orders=ORDERS, config=HEAVY)
    # Popen + threads rather than asyncio subprocesses: the selector event
    # loop psycopg needs on Windows cannot spawn subprocesses.
    child = subprocess.Popen(  # noqa: ASYNC220 - Popen does not block; see above
        [sys.executable, "-m", "tests.differential.memory_child", dsn, str(BUDGET)],
        stdout=subprocess.PIPE,
    )
    assert child.stdout is not None
    stats = LoadStats()
    try:
        async with asyncio.timeout(1800):
            ready = await asyncio.to_thread(child.stdout.readline)
            assert ready.strip() == b"ready", ready
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                workload = Workload(seed, await load_state(conn), HEAVY)
                await run_load(conn, workload, txns=LOAD_TXNS, stats=stats)
                await conn.execute("SELECT pg_logical_emit_message(true, %s, 'end')", (MARKER,))
            out, _ = await asyncio.to_thread(child.communicate)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
    assert child.returncode == 0, f"child exited with {child.returncode}"
    report: dict[str, Any] = json.loads(out.decode().strip().splitlines()[-1])
    return report, stats
