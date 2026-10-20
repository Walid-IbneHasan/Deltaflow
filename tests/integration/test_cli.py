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


def spawn(log: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.Popen[bytes]:
    with log.open("ab") as out:
        return subprocess.Popen(
            [sys.executable, "-m", "deltaflow", *args],
            stdout=out, stderr=subprocess.STDOUT, cwd=ROOT,
            env={**os.environ, "PYTHONUNBUFFERED": "1", **(env or {})},
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


# --- views written in SQL ------------------------------------------------------------

SQL_VIEWS = {
    "cli_sql.revenue_by_country": (
        "SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue, avg(o.amount) AS mean"
        " FROM orders o JOIN customers c ON c.id = o.customer_id"
        " WHERE o.status <> 'cancelled' GROUP BY c.country"
    ),
    "cli_sql.spend": (
        "SELECT c.id AS customer, count(o.id) AS orders, COALESCE(sum(o.amount), 0) AS spent"
        " FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.id"
    ),
    "cli_sql.paid_orders": (
        "SELECT o.id, o.customer_id, o.amount FROM orders o WHERE o.status = 'paid'"
    ),
}


def sql_file(path: Path, views: dict[str, str], *, materialized: str = "") -> Path:
    path.write_text(
        "\n".join(f"CREATE {materialized}VIEW {name} AS\n{q};\n" for name, q in views.items()),
        encoding="utf-8",
    )
    return path


def catalog(dsn: str, table: str) -> tuple[list[tuple[str, str]], list[str]]:
    with psycopg.connect(dsn) as sql:
        columns = sql.execute(
            "SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute"
            " WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            (table,),
        ).fetchall()
        key = sql.execute(
            "SELECT a.attname::text FROM pg_index i JOIN pg_attribute a"
            " ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)"
            " WHERE i.indrelid = to_regclass(%s) AND i.indisprimary ORDER BY a.attnum",
            (table,),
        ).fetchall()
    return [(str(n), str(t)) for n, t in columns], [str(k) for (k,) in key]


def test_run_a_sql_file_creating_its_sink_tables(dsn: str, tmp_path: Path) -> None:
    prepare(dsn)
    with psycopg.connect(dsn, autocommit=True) as sql:
        sql.execute("DROP SCHEMA IF EXISTS cli_sql CASCADE")
        sql.execute(
            "DELETE FROM _deltaflow.watermarks WHERE view = ANY (%s)",
            (["revenue_by_country", "spend", "paid_orders"],),
        )
    slot = f"it_cli_{uuid.uuid4().hex[:8]}"
    views = sql_file(tmp_path / "views.sql", SQL_VIEWS)
    log = tmp_path / "worker.log"
    proc = spawn(
        log, "run", str(views), "--dsn", dsn, "--slot", slot, "--publication", "it_cli_sql",
        "--state-dir", str(tmp_path / "state"), "--http-port", "-1",
        "--flush-interval", "0.05", "--checkpoint-interval", "0.3", "--log-format", "text",
    )  # fmt: skip
    try:
        wait_for(log, r"(3) SQL views checked by Postgres", proc)
        wait_for(log, r"worker pid=\d+ (bootstrapped);", proc)
        # sink tables made from Postgres's own types, keyed by the views' keys
        assert catalog(dsn, "cli_sql.revenue_by_country") == (
            [
                ("country", "text"),
                ("orders", "bigint"),
                ("revenue", "numeric"),
                ("mean", "numeric"),
            ],
            ["country"],
        )
        assert catalog(dsn, "cli_sql.spend")[1] == ["customer"]
        assert catalog(dsn, "cli_sql.paid_orders")[1] == ["id"]
        marker = run(load_then_marker(dsn))
        deadline = time.monotonic() + 60
        for view in ("revenue_by_country", "spend", "paid_orders"):
            while (w := watermark(dsn, view)) is None or w <= marker:
                assert time.monotonic() < deadline, f"{view} never caught up"
                time.sleep(0.1)
        for table, query in SQL_VIEWS.items():
            assert rows(dsn, f"SELECT * FROM {table}") == rows(dsn, query), table
    finally:
        proc.terminate()
        proc.wait(timeout=30)
    wait_inactive(dsn, slot)
    deltaflow("reset", "--dsn", dsn, "--slot", slot)


def test_a_sql_file_is_checked_before_anything_starts(dsn: str, tmp_path: Path) -> None:
    """Postgres's own message for a mistake, and no slot created for it."""
    slot = f"it_cli_{uuid.uuid4().hex[:8]}"
    bad = sql_file(tmp_path / "bad.sql", {"cli_sql.bad": "SELECT o.id, o.nope FROM orders o"})
    result = deltaflow("run", str(bad), "--dsn", dsn, "--slot", slot)
    assert result.returncode == 2
    assert "column o.nope does not exist" in result.stderr
    keyless = sql_file(
        tmp_path / "keyless.sql",
        {
            "cli_sql.pairs": (
                "SELECT c.id, o.id AS order_id"
                " FROM customers c LEFT JOIN orders o ON o.customer_id = c.id"
            )
        },
    )
    result = deltaflow("run", str(keyless), "--dsn", dsn, "--slot", slot)
    assert result.returncode == 2
    assert "the right side of a left join can be NULL" in result.stderr
    with psycopg.connect(dsn) as sql:
        assert (
            sql.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot,)
            ).fetchone()
            is None
        )


def test_a_cluster_runs_a_sql_file(dsn: str, tmp_path: Path) -> None:
    prepare(dsn)
    with psycopg.connect(dsn, autocommit=True) as sql:
        sql.execute("DROP SCHEMA IF EXISTS cli_sqlc CASCADE")
        # a previous run's watermarks would make the wait below end at once
        sql.execute("DELETE FROM _deltaflow.watermarks WHERE view LIKE 'per\\_customer@%%'")
        sql.execute("DELETE FROM _deltaflow.shard_leases")
        sql.execute("DELETE FROM _deltaflow.workers")
    query = (
        "SELECT o.customer_id, count(*) AS n, sum(o.amount) AS spent"
        " FROM orders o GROUP BY o.customer_id"
    )
    views = sql_file(
        tmp_path / "cluster.sql", {"cli_sqlc.per_customer": query}, materialized="MATERIALIZED "
    )
    prefix = f"it_clis_{uuid.uuid4().hex[:6]}"
    log = tmp_path / "cluster.log"
    proc = spawn(
        log, "cluster", str(views), "--dsn", dsn, "--worker-id", "solo", "--slot-prefix", prefix,
        "--publication", "it_cli_sql_cluster", "--shards", "2",
        "--shard-by", "public.orders=customer_id", "--state-root", str(tmp_path / "cluster"),
        "--http-port", "-1", "--ttl", "3", "--checkpoint-interval", "0.3", "--log-format", "text",
    )  # fmt: skip
    try:
        wait_for(log, r"(1) SQL views checked by Postgres", proc)
        # the partial table, led by the shard column, and the merge view over it
        columns, key = catalog(dsn, "cli_sqlc.per_customer__shards")
        assert columns[0] == ("shard", "integer")
        assert key == ["shard", "customer_id"]
        deadline = time.monotonic() + 90
        while not all(watermark(dsn, f"per_customer@{n}") for n in (0, 1)):
            assert time.monotonic() < deadline, "the shards never flushed"
            time.sleep(0.2)
        assert rows(dsn, "SELECT * FROM cli_sqlc.per_customer") == rows(dsn, query)
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


def test_editing_a_sql_file_rebuilds_only_what_changed_even_through_a_crash(
    dsn: str, tmp_path: Path
) -> None:
    """Change one view of a running SQL file and add another: the restart
    builds just those two from the checkpoint's source rows, with no
    bootstrap. A crash in the middle of saving the rebuilt state (between
    the two renames that swap the checkpoint) loses nothing: the next start
    resumes."""
    prepare(dsn)
    with psycopg.connect(dsn, autocommit=True) as sql:
        sql.execute("DROP SCHEMA IF EXISTS cli_sqlv CASCADE")
        sql.execute(
            "DELETE FROM _deltaflow.watermarks WHERE view = ANY (%s)",
            (["by_status", "big", "per_customer"],),
        )
    v1 = {
        "cli_sqlv.by_status": "SELECT status, count(*) AS n FROM orders GROUP BY status",
        "cli_sqlv.big": "SELECT o.id, o.amount FROM orders o WHERE o.amount > 1000",
    }
    v2 = {
        "cli_sqlv.by_status": (
            "SELECT status, count(*) AS n FROM orders WHERE amount > 10 GROUP BY status"
        ),
        "cli_sqlv.big": v1["cli_sqlv.big"],
        "cli_sqlv.per_customer": (
            "SELECT customer_id, count(*) AS n FROM orders GROUP BY customer_id"
        ),
    }
    slot = f"it_cli_{uuid.uuid4().hex[:8]}"
    common = (
        "--dsn", dsn, "--slot", slot, "--publication", "it_cli_sqlv",
        "--state-dir", str(tmp_path / "state"), "--http-port", "-1",
        "--flush-interval", "0.05", "--checkpoint-interval", "0.3", "--log-format", "text",
    )  # fmt: skip
    path = tmp_path / "views.sql"

    def run_until_caught_up(log: Path, views: dict[str, str]) -> str:
        proc = spawn(log, "run", str(path), *common)
        try:
            how = wait_for(log, r"worker pid=\d+ (\w+);", proc)
            marker = run(load_then_marker(dsn))
            deadline = time.monotonic() + 60
            for table in views:
                name = table.split(".")[1]
                while (w := watermark(dsn, name)) is None or w <= marker:
                    assert time.monotonic() < deadline, f"{name} never caught up"
                    time.sleep(0.1)
            for table, query in views.items():
                assert rows(dsn, f"SELECT * FROM {table}") == rows(dsn, query), table
            return how
        finally:
            proc.terminate()
            proc.wait(timeout=30)
            wait_inactive(dsn, slot)

    sql_file(path, v1)
    assert run_until_caught_up(tmp_path / "1.log", v1) == "bootstrapped"
    sql_file(path, v2)
    crashed = spawn(tmp_path / "2.log", "run", str(path), *common,
                    env={"DELTAFLOW_CRASH_AT": "checkpoint:swap:1"})  # fmt: skip
    assert crashed.wait(timeout=90) == 86, (tmp_path / "2.log").read_text(encoding="utf-8")
    text = (tmp_path / "2.log").read_text(encoding="utf-8")
    assert "built views ['by_status', 'per_customer']" in text
    wait_inactive(dsn, slot)
    assert run_until_caught_up(tmp_path / "3.log", v2) == "resumed"
    deltaflow("reset", "--dsn", dsn, "--slot", slot, "--state-dir", str(tmp_path / "state"))
