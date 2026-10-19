"""The deltaflow command end to end, as a user runs it: their own views,
named on the command line, against the compose server."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import psycopg

from deltaflow.aio import run
from deltaflow.devtools.loadgen import load_state, run_load
from deltaflow.devtools.seed import seed_database
from deltaflow.devtools.workload import Workload
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks.postgres import ensure_watermark_table
from tests.integration import cli_views
from tests.worker_harness import emit_marker

ROOT = Path(__file__).resolve().parents[2]


def deltaflow(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "deltaflow", *args],
        capture_output=True, text=True, timeout=120, cwd=ROOT, check=False, **kwargs,
    )  # fmt: skip


def spawn(log: Path, *args: str) -> subprocess.Popen[bytes]:
    with log.open("ab") as out:
        return subprocess.Popen(
            [sys.executable, "-m", "deltaflow", *args],
            stdout=out, stderr=subprocess.STDOUT, cwd=ROOT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )  # fmt: skip


def wait_for(log: Path, pattern: str, proc: subprocess.Popen[bytes], timeout: float = 90) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = log.read_text(encoding="utf-8", errors="replace") if log.exists() else ""
        if m := re.search(pattern, text):
            return m.group(1)
        if proc.poll() is not None:
            raise AssertionError(f"deltaflow exited with {proc.returncode}:\n{text}")
        time.sleep(0.1)
    raise AssertionError(f"no {pattern!r} in {timeout}s:\n{log.read_text(encoding='utf-8')}")


def get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=10) as response:
        return str(response.read().decode())


def prepare(dsn: str) -> None:
    run(seed_database(dsn, seed=7, customers=30, orders=150))
    with psycopg.connect(dsn, autocommit=True) as sql:
        sql.execute(cli_views.DDL)
        sql.execute("DELETE FROM _deltaflow.watermarks WHERE view LIKE 'orders_%%'")
    run(_ensure_watermarks(dsn))


async def _ensure_watermarks(dsn: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        await ensure_watermark_table(sql)


async def load_then_marker(dsn: str) -> Lsn:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        await run_load(sql, Workload(8, await load_state(sql)), txns=60)
        return await emit_marker(sql)


def watermark(dsn: str, view: str) -> Lsn | None:
    with psycopg.connect(dsn) as sql:
        row = sql.execute("SELECT lsn::text FROM _deltaflow.watermarks WHERE view = %s", (view,))
        found = row.fetchone()
    return None if found is None else Lsn.parse(str(found[0]))


def wait_inactive(dsn: str, slot_pattern: str, timeout: float = 30) -> None:
    """A killed worker's walsender notices the closed socket shortly after."""
    deadline = time.monotonic() + timeout
    with psycopg.connect(dsn) as sql:
        while sql.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name LIKE %s AND active",
            (slot_pattern,),
        ).fetchone():
            assert time.monotonic() < deadline, f"{slot_pattern} still active"
            time.sleep(0.1)


def rows(dsn: str, query: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as sql:
        return sorted(sql.execute(query).fetchall())


def test_run_status_and_reset_a_users_own_views(dsn: str, tmp_path: Path) -> None:
    prepare(dsn)
    slot = f"it_cli_{uuid.uuid4().hex[:8]}"
    state = tmp_path / "state"
    log = tmp_path / "worker.log"
    proc = spawn(
        log, "run", "tests.integration.cli_views:views", "--dsn", dsn, "--slot", slot,
        "--publication", "it_cli", "--state-dir", str(state), "--http-port", "0",
        "--flush-interval", "0.05", "--checkpoint-interval", "0.3", "--log-format", "text",
    )  # fmt: skip
    try:
        url = wait_for(log, r"metrics on (http://\S+)/metrics", proc)
        wait_for(log, r"worker pid=\d+ (bootstrapped);", proc)
        marker = run(load_then_marker(dsn))
        deadline = time.monotonic() + 60
        while (w := watermark(dsn, "orders_by_status")) is None or w <= marker:
            assert time.monotonic() < deadline, "the sink never caught up"
            time.sleep(0.1)
        assert rows(dsn, "SELECT * FROM cli_it.orders_by_status") == rows(dsn, cli_views.SQL)
        metrics = get(f"{url}/metrics")
        applied = re.search(r"^deltaflow_transactions_total (\d+)", metrics, re.MULTILINE)
        assert applied is not None
        assert int(applied.group(1)) > 0  # rollbacks and unpublished tables never arrive
        assert json.loads(get(f"{url}/api/stats"))["slot"] == slot

        status = deltaflow("status", "--dsn", dsn, "--json")
        assert status.returncode == 0, status.stderr
        report = json.loads(status.stdout)
        (ours,) = [s for s in report["slots"] if s["slot"] == slot]
        assert ours["active"] is True
        assert ours["lag_bytes"] is not None
        assert any(w["view"] == "orders_by_status" for w in report["watermarks"])
        text = deltaflow("status", "--dsn", dsn)
        assert slot in text.stdout
    finally:
        proc.terminate()
        proc.wait(timeout=30)
    wait_inactive(dsn, slot)

    reset = deltaflow("reset", "--dsn", dsn, "--slot", slot, "--state-dir", str(state))
    assert reset.returncode == 0, reset.stderr
    with psycopg.connect(dsn) as sql:
        left = sql.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot,))
        assert left.fetchone() is None
    assert not (state / "checkpoints").exists() or not any((state / "checkpoints").iterdir())


def test_reset_refuses_a_slot_in_use(dsn: str, tmp_path: Path) -> None:
    """Dropping a slot a worker streams from would kill it mid-flight and
    lose its position: refused, with the holder's pid."""
    prepare(dsn)
    slot = f"it_cli_{uuid.uuid4().hex[:8]}"
    log = tmp_path / "worker.log"
    proc = spawn(
        log, "run", "tests.integration.cli_views:views", "--dsn", dsn, "--slot", slot,
        "--publication", "it_cli", "--state-dir", str(tmp_path / "state"), "--http-port", "-1",
    )  # fmt: skip
    try:
        wait_for(log, r"worker pid=\d+ (bootstrapped);", proc)
        reset = deltaflow("reset", "--dsn", dsn, "--slot", slot)
        assert reset.returncode != 0
        assert "is in use" in reset.stderr
    finally:
        proc.terminate()
        proc.wait(timeout=30)
        wait_inactive(dsn, slot)
        deltaflow("reset", "--dsn", dsn, "--slot", slot)


def test_a_cluster_worker_serves_every_shards_metrics(dsn: str, tmp_path: Path) -> None:
    prepare(dsn)
    prefix = f"it_clic_{uuid.uuid4().hex[:6]}"
    with psycopg.connect(dsn, autocommit=True) as sql:
        sql.execute("DELETE FROM _deltaflow.shard_leases")
        sql.execute("DELETE FROM _deltaflow.workers")
    log = tmp_path / "cluster.log"
    proc = spawn(
        log, "cluster", "tests.integration.cli_views:sharded", "--dsn", dsn,
        "--worker-id", "solo", "--slot-prefix", prefix, "--publication", "it_cli_cluster",
        "--state-root", str(tmp_path / "cluster"), "--http-port", "0", "--ttl", "3",
        "--checkpoint-interval", "0.3", "--log-format", "text",
    )  # fmt: skip
    try:
        url = wait_for(log, r"metrics on (http://\S+)/metrics", proc)
        deadline = time.monotonic() + 90
        while True:
            metrics = get(f"{url}/metrics")
            if all(f'deltaflow_lsn{{position="watermark",shard="{n}"}}' in metrics for n in "01"):
                break
            assert time.monotonic() < deadline, metrics[-2000:]
            time.sleep(0.2)
        assert re.search(r"^deltaflow_cluster_owned_shards 2$", metrics, re.MULTILINE)
        assert metrics.count("# TYPE deltaflow_transactions_total counter") == 1
        stats = json.loads(get(f"{url}/api/stats"))
        assert sorted(stats["owned"]) == ["0", "1"]
        # the merge view the command created over the partial table
        merged = rows(dsn, "SELECT * FROM cli_it.orders_per_customer")
        expected = rows(
            dsn, "SELECT customer_id, count(*) AS orders FROM orders GROUP BY customer_id"
        )
        assert merged == expected
    finally:
        proc.terminate()
        proc.wait(timeout=30)
        wait_inactive(dsn, f"{prefix}%")
        with psycopg.connect(dsn, autocommit=True) as sql:
            sql.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
                " WHERE slot_name LIKE %s AND NOT active",
                (f"{prefix}%",),
            )


def test_a_views_spec_that_does_not_resolve_is_reported() -> None:
    result = deltaflow("run", "no_such_module_xyz:views", "--slot", "x")
    assert result.returncode == 2
    assert "cannot import no_such_module_xyz" in result.stderr
