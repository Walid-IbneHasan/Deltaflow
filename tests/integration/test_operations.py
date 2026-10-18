"""Operations (M6): the monitoring endpoints report what happened, and a slow
sink applies backpressure instead of buffering or losing the connection."""

from __future__ import annotations

import asyncio
import json
import re
import sys
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.devtools.loadgen import LoadStats, load_state, run_load
from deltaflow.devtools.worker import views
from deltaflow.devtools.workload import Workload
from deltaflow.observability.http import MonitorServer, json_handler
from deltaflow.observability.memory import rss
from deltaflow.runtime import DurableRunner
from tests.integration.test_durable_runner import drop, fresh
from tests.worker_harness import emit_marker, sink_differences, wait_caught_up


async def get(port: int, path: str) -> tuple[int, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    response = await reader.read()
    writer.close()
    head, _, body = response.partition(b"\r\n\r\n")
    return int(head.split()[1]), body


def metric(text: str, name: str, **labels: str) -> float:
    selector = ",".join(f'{k}="{v}"' for k, v in labels.items())
    pattern = re.escape(name + (f"{{{selector}}}" if selector else "")) + r" (\S+)"
    found = re.search(r"^" + pattern + "$", text, re.M)
    assert found, f"{name} {labels} not exposed"
    return float(found.group(1))


def runner(dsn: str, tmp_path: Path, slot: str) -> DurableRunner:
    return DurableRunner(
        views(), dsn, state_dir=tmp_path, slot=slot, publication="it_ops",
        flush_interval=0.05, checkpoint_interval=1.0, status_interval=0.5,
    )  # fmt: skip


def test_metrics_and_stats_report_the_run(dsn: str, tmp_path: Path) -> None:
    slot = f"it_ops_{uuid.uuid4().hex[:8]}"

    async def go() -> tuple[str, dict[str, Any], int]:
        await fresh(dsn)
        r = runner(dsn, tmp_path, slot)
        await r.start()
        monitor = MonitorServer(
            {
                "/metrics": lambda: ("text/plain", r.collect().expose().encode()),
                "/api/stats": json_handler(r.stats),
            },
            port=0,
        )
        await monitor.start()
        stop = asyncio.Event()
        task = asyncio.create_task(r.run(stop))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
                await run_load(sql, Workload(1, await load_state(sql)), txns=100)
                await wait_caught_up(sql, await emit_marker(sql))
            await asyncio.sleep(2.5)  # one slot-lag poll and a checkpoint
            _, text = await get(monitor.port, "/metrics")
            _, stats = await get(monitor.port, "/api/stats")
            missing, _ = await get(monitor.port, "/nope")
            return text.decode(), json.loads(stats), missing
        finally:
            stop.set()
            await task
            await monitor.close()
            await r.close()
            await drop(dsn, slot)

    text, stats, missing = run(go())
    assert missing == 404
    assert metric(text, "deltaflow_transactions_total") >= 90  # ~95% of the load commits
    assert metric(text, "deltaflow_end_to_end_lag_seconds_count") >= 90
    assert (
        metric(text, "deltaflow_operator_deltas_total", operator="order_details/0", direction="in")
        > 0
    )
    assert (
        metric(
            text,
            "deltaflow_operator_deltas_total",
            operator="source:public.orders",
            direction="out",
        )
        > 0
    )
    assert metric(text, "deltaflow_runner_events_total", event="checkpoints") >= 1
    assert metric(text, "deltaflow_state_bytes", kind="sst_bytes") > 0
    assert metric(text, "deltaflow_slot_lag_bytes") >= 0
    assert 'deltaflow_end_to_end_lag_seconds_bucket{le="+Inf"}' in text
    assert len(stats["graph"]["nodes"]) == 15
    assert stats["freshness_seconds"]["p99"] is not None
    assert stats["watermark"] is not None


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "Docker Desktop's Windows port forwarding stops relaying client->server data while the "
        "server->client direction is backed up, so heartbeats never reach the walsender. "
        "The same stalled consumer, run from a Linux container on the compose network, kept "
        "its connection (docs/milestones/M6.md). CI runs this test on Linux."
    ),
)
def test_a_slow_sink_applies_backpressure_and_survives_wal_sender_timeout(
    dsn: str, tmp_path: Path
) -> None:
    """The sink table is locked for 8 s, four times wal_sender_timeout (lowered
    to 2 s for this test), while load continues. The flush blocks. The engine
    stops consuming, the stream's bounded queue fills, and its reader stops
    reading the socket, so the walsender blocks on TCP. Nothing piles up in
    the process. The heartbeat task keeps sending status updates, so the
    server does not drop the connection. Afterwards everything catches up
    exactly."""
    slot = f"it_ops_{uuid.uuid4().hex[:8]}"

    async def go() -> dict[str, Any]:
        await fresh(dsn)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
            await admin.execute("ALTER SYSTEM SET wal_sender_timeout = '2s'")
            await admin.execute("SELECT pg_reload_conf()")
            await asyncio.sleep(0.5)
            setting = await (await admin.execute("SHOW wal_sender_timeout")).fetchone()
            # Without this the test is vacuous: a command-line -c setting would
            # silently win over ALTER SYSTEM (it did, the first time).
            assert setting == ("2s",), f"wal_sender_timeout is {setting}, not lowered"
        r = runner(dsn, tmp_path, slot)
        observed: dict[str, Any] = {"max_queued": 0}
        stop = asyncio.Event()
        try:
            await r.start()
            task = asyncio.create_task(r.run(stop))
            async with (
                await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql,
                await psycopg.AsyncConnection.connect(dsn) as locker,
            ):
                await locker.execute("LOCK TABLE analytics.order_details IN ACCESS EXCLUSIVE MODE")
                rss_before = rss().current
                stats = LoadStats()
                load = asyncio.create_task(
                    run_load(sql, Workload(2, await load_state(sql)), txns=600, stats=stats)
                )
                for _ in range(80):  # 8 s under the lock
                    await asyncio.sleep(0.1)
                    observed["max_queued"] = max(observed["max_queued"], r.stats()["queued"])
                    assert not task.done(), "the runner died while the sink was blocked"
                observed["rss_growth"] = rss().current - rss_before
                observed["watermark_during"] = r.flushed_lsn
                await locker.rollback()  # release the lock
                await load
                observed["committed"] = stats.committed
                observed["caught_up_s"] = await wait_caught_up(sql, await emit_marker(sql), 120)
                observed["problems"] = await sink_differences(sql)
            stop.set()
            await task
            observed["max_pending"] = r.stats()["max_queued"]
            return observed
        finally:
            stop.set()
            await r.close(final_checkpoint=False)
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
                await admin.execute("ALTER SYSTEM RESET wal_sender_timeout")
                await admin.execute("SELECT pg_reload_conf()")
            await drop(dsn, slot)

    observed = run(go())
    print(f"backpressure: {observed}")
    assert observed["problems"] == []
    assert observed["max_queued"] == observed["max_pending"], "the queue never filled"
    assert observed["rss_growth"] < 64 << 20
