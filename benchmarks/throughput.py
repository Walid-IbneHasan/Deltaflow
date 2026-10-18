"""Benchmark: sustained changes per second and end-to-end freshness.

    uv run python -m benchmarks.throughput [--dsn DSN] [--backlog 6000] [--duration 60]

It re-seeds the target database, so do not point it at data you care about.
It drives the real worker (`deltaflow.devtools.worker`: four views,
Postgres sinks, exactly-once, checkpoints every 10 s) as a separate process,
so the load generator does not share the worker's interpreter.

**Capacity.** The worker is stopped while a backlog of transactions commits.
It is then started, and the clock runs from the moment its HTTP endpoint
answers (resume done) until every sink's watermark passes a marker
committed after the backlog. Changes per second = row changes in the
backlog / that time. With the work queued in advance, nothing waits on the
load generator, so this is the engine's own ceiling, checkpoints and sink
writes included.

**Freshness.** The worker runs while load arrives at fixed fractions of that
capacity. The worker's histogram `deltaflow_end_to_end_lag_seconds` records,
per transaction, the time from its commit on the source to the commit of
the sink transaction that made it visible. Percentiles are read off the
bucket bounds, so "p99 0.3 s" means at most 0.3 s. Freshness spans two
clocks (the Postgres server's commit time, the worker's flush time), so the
offset between them is measured and reported too.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import platform
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.config import pg_dsn
from deltaflow.devtools.loadgen import LoadStats, load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.worker import SINK_DDL
from deltaflow.devtools.workload import Workload
from deltaflow.replication.lsn import Lsn
from tests.worker_harness import emit_marker

PORT = 9188
STATE = Path(".deltaflow/bench")
SLOT = "deltaflow_bench"
RESULTS = Path("benchmarks/results")


def worker_cmd(dsn: str) -> list[str]:
    return [
        sys.executable, "-m", "deltaflow.devtools.worker", "--dsn", dsn,
        "--state-dir", str(STATE), "--slot", SLOT, "--publication", SLOT,
        "--http-port", str(PORT), "--memory-budget", "128",
    ]  # fmt: skip


def start_worker(dsn: str, log: Path) -> subprocess.Popen[bytes]:
    with open(log, "ab") as out:
        return subprocess.Popen(worker_cmd(dsn), stdout=out, stderr=subprocess.STDOUT)


async def http(path: str) -> str | None:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", PORT), 1)
        writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
        await writer.drain()
        data = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        return data.partition(b"\r\n\r\n")[2].decode()
    except (OSError, TimeoutError):
        return None


async def stats() -> dict[str, Any] | None:
    body = await http("/api/stats")
    return json.loads(body) if body else None


async def until_up(timeout: float = 120) -> float:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if await http("/healthz"):
            return time.monotonic()
        await asyncio.sleep(0.05)
    raise TimeoutError("worker did not come up")


async def until_past(
    marker: Lsn, timeout: float = 600, sample: list[dict[str, Any]] | None = None
) -> float:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        s = await stats()
        if s and sample is not None:
            sample.append(s)
        if s and s["watermark"] and Lsn.parse(s["watermark"]) > marker:
            return time.monotonic()
        await asyncio.sleep(0.05)
    raise TimeoutError(f"sinks did not pass {marker}")


async def until_checkpointed(marker: Lsn, timeout: float = 120) -> None:
    """Wait for a checkpoint past `marker`, so that the next start of the
    worker resumes from there instead of replaying this phase."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        s = await stats()
        if s and s["checkpoint"] and Lsn.parse(s["checkpoint"]) > marker:
            return
        await asyncio.sleep(0.2)
    raise TimeoutError(f"no checkpoint past {marker}")


def histogram(metrics: str, name: str) -> list[tuple[float, int]]:
    out = []
    for le, n in re.findall(rf'^{name}_bucket{{le="([^"]+)"}} (\d+)$', metrics, re.M):
        out.append((float("inf") if le == "+Inf" else float(le), int(n)))
    return out


def quantile(buckets: list[tuple[float, int]], q: float) -> float:
    total = buckets[-1][1]
    for bound, cumulative in buckets:
        if cumulative >= q * total:
            return bound
    return float("inf")


def kill(worker: subprocess.Popen[bytes]) -> None:
    worker.kill()
    worker.wait()


def machine() -> dict[str, Any]:
    info: dict[str, Any] = {
        "os": platform.platform(),
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
    }
    if sys.platform == "win32":
        name = subprocess.run(
            ["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"],
            capture_output=True, text=True, check=False,
        ).stdout.strip()  # fmt: skip
        mem = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory"],
            capture_output=True, text=True, check=False,
        ).stdout.strip()  # fmt: skip
        info["cpu"] = name
        info["memory_gib"] = round(int(mem) / 2**30, 1) if mem.isdigit() else None
    elif Path("/proc/cpuinfo").exists():
        cpuinfo = Path("/proc/cpuinfo").read_text()
        found = re.search(r"model name\s*:\s*(.+)", cpuinfo)
        info["cpu"] = found.group(1) if found else platform.processor()
        kb = re.search(r"MemTotal:\s+(\d+)", Path("/proc/meminfo").read_text())
        info["memory_gib"] = round(int(kb.group(1)) / 2**20, 1) if kb else None
    return info


async def clock_offset(sql: psycopg.AsyncConnection[tuple[Any, ...]]) -> float:
    """Postgres clock minus local clock, seconds (± half a round trip)."""
    best: tuple[float, float] | None = None
    for _ in range(10):
        t0 = time.time()
        row = await (await sql.execute("SELECT extract(epoch FROM clock_timestamp())")).fetchone()
        t1 = time.time()
        assert row is not None
        sample = (t1 - t0, float(row[0]) - (t0 + t1) / 2)
        best = sample if best is None or sample[0] < best[0] else best
    assert best is not None
    return best[1]


async def main(args: argparse.Namespace) -> dict[str, Any]:  # noqa: PLR0915 - one script, top to bottom
    dsn = args.dsn
    log = RESULTS / "worker.log"
    result: dict[str, Any] = {
        "when": datetime.now(UTC).isoformat(timespec="seconds"),
        "machine": machine(),
    }
    await seed_database(dsn, seed=1, customers=args.customers, orders=args.orders)
    await asyncio.to_thread(
        subprocess.run, [*worker_cmd(dsn), "--reset"], check=True, capture_output=True
    )
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        result["postgres"] = (await (await sql.execute("SELECT version()")).fetchone() or ["?"])[0]
        await sql.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
        await sql.execute(SINK_DDL)
        result["clock_offset_ms"] = round(1000 * await clock_offset(sql), 2)

        # Bootstrap, then stop the worker so a backlog can build up.
        worker = start_worker(dsn, log)
        await until_up()
        kill(worker)

        # --- capacity -------------------------------------------------------
        backlog = LoadStats()
        workload = Workload(2, await load_state(sql))
        await run_load(sql, workload, txns=args.backlog, stats=backlog)
        marker = await emit_marker(sql)
        changes = sum(backlog.statements.values())
        print(
            f"backlog: {backlog.committed} committed transactions, {changes} row changes",
            flush=True,
        )
        worker = start_worker(dsn, log)
        up = await until_up()
        done = await until_past(marker)
        await until_checkpointed(marker)
        kill(worker)
        seconds = done - up
        capacity_txn = (backlog.committed + 1) / seconds
        result["capacity"] = {
            "transactions": backlog.committed,
            "row_changes": changes,
            "seconds": round(seconds, 2),
            "changes_per_second": round(changes / seconds),
            "transactions_per_second": round(capacity_txn, 1),
        }
        print(f"capacity: {result['capacity']}", flush=True)

        # --- freshness at fixed load -----------------------------------------
        result["freshness"] = []
        for fraction in args.fractions:
            rate = fraction * capacity_txn
            log_mark = len(log.read_text(encoding="utf-8", errors="replace").splitlines())
            worker = start_worker(dsn, log)
            await until_up()
            samples: list[dict[str, Any]] = []
            load = LoadStats()
            sampling = asyncio.create_task(_sample(samples))
            t0 = time.monotonic()
            await run_load(sql, workload, txns=int(rate * args.duration), rate=rate, stats=load)
            elapsed = time.monotonic() - t0
            sampling.cancel()
            settled = await emit_marker(sql)
            await until_past(settled)
            metrics = await http("/metrics") or ""
            await until_checkpointed(settled)
            kill(worker)
            durations = [
                json.loads(line)["seconds"]
                for line in log.read_text(encoding="utf-8", errors="replace").splitlines()[
                    log_mark:
                ]
                if '"msg": "checkpoint"' in line
            ]
            lag = histogram(metrics, "deltaflow_end_to_end_lag_seconds")
            ck = histogram(metrics, "deltaflow_checkpoint_seconds")
            ck_sum = re.search(r"^deltaflow_checkpoint_seconds_sum (\S+)$", metrics, re.M)
            row = {
                "target_fraction_of_capacity": fraction,
                "achieved_transactions_per_second": round(load.committed / elapsed, 1),
                "achieved_changes_per_second": round(sum(load.statements.values()) / elapsed),
                "seconds": round(elapsed, 1),
                "transactions_observed": lag[-1][1] if lag else 0,
                "p50_s": quantile(lag, 0.5),
                "p90_s": quantile(lag, 0.9),
                "p99_s": quantile(lag, 0.99),
                "max_queued": max((s["queued"] for s in samples), default=0),
                "max_apply_lag_bytes": max((s["apply_lag_bytes"] for s in samples), default=0),
                "peak_rss_mib": round(max((s["rss_bytes"] for s in samples), default=0) / 2**20, 1),
                "checkpoints": ck[-1][1] if ck else 0,
                "mean_checkpoint_s": round(float(ck_sum.group(1)) / ck[-1][1], 3)
                if ck and ck[-1][1] and ck_sum
                else None,
                "max_checkpoint_s": max(durations, default=None),
            }
            row["timeline"] = [
                (
                    s["time"][11:19],
                    s["frontier"],
                    s["queued"],
                    s["slot_lag_bytes"],
                    s["counters"].get("transactions", 0),
                )
                for s in samples[::4]
            ]
            result["freshness"].append(row)
            print(f"freshness: {row}", flush=True)
    return result


async def _sample(into: list[dict[str, Any]]) -> None:
    while True:
        s = await stats()
        if s:
            into.append(s)
        await asyncio.sleep(0.5)


def report(r: dict[str, Any]) -> str:
    m, c = r["machine"], r["capacity"]
    lines = [
        f"Measured {r['when']} on {m.get('cpu')} ({m['cpus']} logical CPUs, "
        f"{m.get('memory_gib')} GiB), {m['os']}, Python {m['python']}; "
        f"{r['postgres'].split(',')[0]} in Docker. Postgres-vs-host clock offset "
        f"{r['clock_offset_ms']} ms.",
        "",
        "| Capacity (backlog catch-up) | |",
        "|---|---|",
        f"| row changes | {c['row_changes']} in {c['transactions']} transactions |",
        f"| time | {c['seconds']} s |",
        f"| **sustained changes/second** | **{c['changes_per_second']}** "
        f"({c['transactions_per_second']} transactions/s) |",
        "",
        "| Load (share of capacity) | achieved changes/s | p50 | p90 | **p99 freshness** "
        "| max queued | peak RSS | checkpoints (mean, max) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for f in r["freshness"]:
        lines.append(
            f"| {int(f['target_fraction_of_capacity'] * 100)}% "
            f"| {f['achieved_changes_per_second']} "
            f"| ≤ {f['p50_s']} s | ≤ {f['p90_s']} s | **≤ {f['p99_s']} s** | {f['max_queued']} "
            f"| {f['peak_rss_mib']} MiB "
            f"| {f['checkpoints']} ({f['mean_checkpoint_s']} s, {f['max_checkpoint_s']} s) |"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=pg_dsn())
    parser.add_argument("--customers", type=int, default=1_000)
    parser.add_argument("--orders", type=int, default=10_000)
    parser.add_argument("--backlog", type=int, default=6_000, help="transactions")
    parser.add_argument("--duration", type=float, default=60, help="seconds per load level")
    parser.add_argument("--fractions", type=float, nargs="+", default=[0.25, 0.5, 0.75])
    a = parser.parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "worker.log").write_bytes(b"")
    outcome = run(main(a))
    (RESULTS / "latest.json").write_text(json.dumps(outcome, indent=2, default=str))
    text = report(outcome)
    (RESULTS / "latest.md").write_text(text, encoding="utf-8")
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(errors="backslashreplace")
    print(text)
