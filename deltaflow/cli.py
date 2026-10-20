"""The deltaflow command: run your own views.

    deltaflow run     app.views:registry --slot NAME                one durable worker
    deltaflow run     views.sql --slot NAME                          views written in SQL
    deltaflow cluster app.views:make --worker-id ID --slot-prefix P one worker of a cluster
    deltaflow status  [--json]                 slots, lag, watermarks, workers, shard leases
    deltaflow reset   --slot NAME | --slot-prefix P              drop slots and their state

`app.views:registry` names a `Deltaflow` registry, or a function that
returns one, importable from the current directory. A cluster needs the
function: every shard runs its own copy of the views, with its own sinks.

A `.sql` file holds `CREATE [MATERIALIZED] VIEW schema.name AS SELECT ...;`
statements (docs/sql.md). Before streaming, every view is checked by
Postgres itself and typed from the catalog, and missing sink tables are
created (`--no-create-sinks` to refuse instead); runtime/sqlviews.py. For
a cluster, give `--shards` and `--shard-by table=column` for each table.

Connection settings come from --dsn, else $DELTAFLOW_DSN, else libpq's
environment variables (PGHOST, PGUSER, PGPASSWORD, PGSSLMODE, ...), which
both kinds of connection honour (replication/conninfo.py).

The sink tables must exist, with each view's key as their primary key
(sinks/postgres.py). A cluster needs, per view, the partial table
`<table>__shards` with a leading `shard` column; `cluster` creates the merge
view `<table>` over it (docs/scale-out.md).

`run` restarts the pipeline after transient failures (`--no-restart` to
exit instead), resuming from its last checkpoint: exactly-once either way.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import io
import json
import os
import shutil
import signal
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg

from deltaflow import __version__, aio
from deltaflow.observability import logs
from deltaflow.observability.http import Handler, MonitorServer, Unavailable, json_handler
from deltaflow.planner import Deltaflow, PlanError
from deltaflow.runtime import sqlviews
from deltaflow.runtime.cluster import ClusterWorker, prepare_cluster
from deltaflow.runtime.durable import DurableRunner
from deltaflow.state.durable import StateDirectory


class CliError(Exception):
    """A mistake on the command line or in what it names: reported, exit 2."""


# --- finding the views ----------------------------------------------------


def load_views(spec: str, *, factory: bool = False) -> Callable[[], Deltaflow]:
    """`module:attribute` as a function returning the registry. With
    `factory`, the attribute must itself be that function."""
    module_name, sep, attribute = spec.partition(":")
    if not sep or not module_name or not attribute:
        raise CliError(f"{spec!r}: expected module:attribute, e.g. app.views:registry")
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)  # as `python -m` and uvicorn do
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise CliError(f"cannot import {module_name}: {exc}") from exc
    try:
        target = getattr(module, attribute)
    except AttributeError:
        raise CliError(f"module {module_name} has no attribute {attribute!r}") from None
    if isinstance(target, Deltaflow):
        if factory:
            raise CliError(
                f"{spec} is a registry; a cluster needs a function that returns a fresh one "
                "(every shard runs its own copy of the views)"
            )
        return lambda: target
    if not callable(target):
        raise CliError(
            f"{spec} is a {type(target).__name__}, "
            "not a Deltaflow registry or a function returning one"
        )

    def make() -> Deltaflow:
        made = target()
        if not isinstance(made, Deltaflow):
            raise CliError(f"{spec}() returned a {type(made).__name__}, not a Deltaflow registry")
        return made

    return make


# --- arguments ------------------------------------------------------------


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="deltaflow", description=__doc__.splitlines()[0])
    p.add_argument("--version", action="version", version=f"deltaflow {__version__}")
    commands = p.add_subparsers(dest="command", required=True, metavar="command")

    def common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--dsn",
            default=os.environ.get("DELTAFLOW_DSN", ""),
            help="connection string (default: $DELTAFLOW_DSN, else libpq's PG* variables)",
        )
        sub.add_argument("--log-format", choices=("json", "text"), default="json")

    def worker(sub: argparse.ArgumentParser, *, budget: int, checkpoint: float) -> None:
        sub.add_argument("--memory-budget", type=int, default=budget, help="MiB of state cache")
        sub.add_argument("--flush-interval", type=float, default=0.1, help="seconds")
        sub.add_argument("--checkpoint-interval", type=float, default=checkpoint, help="seconds")
        sub.add_argument("--status-interval", type=float, default=2.0, help="seconds")
        sub.add_argument("--http-host", default="127.0.0.1")
        sub.add_argument("--http-port", type=int, default=9187, help="-1 disables; 0 picks one")
        sub.add_argument(
            "--snapshot",
            choices=("chunked", "single"),
            default="chunked",
            help="how a bootstrap reads the tables: in key-ordered pages, each with its own "
            "snapshot (default), or in one transaction, holding back VACUUM meanwhile",
        )
        sub.add_argument(
            "--snapshot-chunk-rows", type=int, default=20_000, help="rows per page (chunked)"
        )
        sub.add_argument(
            "--max-transaction-changes",
            type=int,
            default=10_000,
            help="a larger transaction is applied in parts of this many changes",
        )
        sub.add_argument(
            "--sink-spill-after",
            type=int,
            default=100_000,
            help="buffered rows per sink beyond which they move to the state store",
        )

    run = commands.add_parser("run", help="run views in one durable worker")
    run.add_argument("views", help="module:attribute naming the registry, or a .sql file")
    run.add_argument("--slot", required=True, help="replication slot")
    run.add_argument("--publication", help="default: the slot's name")
    run.add_argument("--state-dir", help="default: .deltaflow/<slot>")
    run.add_argument(
        "--no-create-sinks",
        dest="create_sinks",
        action="store_false",
        help="for a .sql file: require the sink tables to exist",
    )
    run.add_argument(
        "--no-restart",
        dest="restart",
        action="store_false",
        help="exit on a lost connection instead of restarting",
    )
    worker(run, budget=64, checkpoint=10.0)
    common(run)

    cluster = commands.add_parser("cluster", help="run one worker of a sharded cluster")
    cluster.add_argument(
        "views", help="module:function returning the sharded registry, or a .sql file"
    )
    cluster.add_argument("--shards", type=int, help="for a .sql file: the number of shards")
    cluster.add_argument(
        "--shard-by",
        action="append",
        default=[],
        metavar="TABLE=COLUMN",
        help="for a .sql file: a source table's shard column (once per table)",
    )
    cluster.add_argument(
        "--no-create-sinks",
        dest="create_sinks",
        action="store_false",
        help="for a .sql file: require the partial tables to exist",
    )
    cluster.add_argument("--worker-id", required=True)
    cluster.add_argument("--slot-prefix", required=True, help="shard n streams <prefix>_s<n>")
    cluster.add_argument("--publication", help="default: the slot prefix")
    cluster.add_argument("--state-root", help="default: .deltaflow/<slot-prefix>")
    cluster.add_argument("--heartbeat", type=float, default=0.5, help="seconds")
    # It must exceed the slowest checkpoint: rocksdict holds the GIL during
    # one, and that stalls the heartbeat thread too (docs/operations.md).
    cluster.add_argument("--ttl", type=float, default=10.0, help="seconds without heartbeat")
    worker(cluster, budget=32, checkpoint=5.0)
    common(cluster)

    status = commands.add_parser("status", help="slots, lag, watermarks, workers, leases")
    status.add_argument("--json", action="store_true")
    common(status)

    reset = commands.add_parser("reset", help="drop slots and their state (refused if in use)")
    which = reset.add_mutually_exclusive_group(required=True)
    which.add_argument("--slot")
    which.add_argument("--slot-prefix", help="every shard slot <prefix>_s<n> of a cluster")
    reset.add_argument("--state-dir", help="default: .deltaflow/<slot or prefix>")
    common(reset)
    return p


# --- commands -------------------------------------------------------------


def _stop_on_signals() -> asyncio.Event:
    stop = asyncio.Event()
    if sys.platform != "win32":  # Windows: Ctrl-C raises KeyboardInterrupt
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
    return stop


async def _monitor(args: argparse.Namespace, routes: dict[str, Handler]) -> MonitorServer | None:
    if args.http_port < 0:
        return None
    routes = {**routes, "/healthz": lambda: ("text/plain", b"ok\n")}
    server = MonitorServer(routes, host=args.http_host, port=args.http_port)
    await server.start()
    print(f"metrics on http://{args.http_host}:{server.port}/metrics", flush=True)
    return server


def runner_routes(runner: DurableRunner) -> dict[str, Handler]:
    def ready() -> tuple[str, bytes]:
        """/readyz: the views are live (not starting, loading, or restarting)."""
        engine = runner.engine
        if engine is None or not engine.live or runner.flushed_lsn is None:
            raise Unavailable("not live yet: starting, loading or restarting")
        return "text/plain", f"live; sinks written through {runner.flushed_lsn}\n".encode()

    return {
        "/metrics": lambda: ("text/plain; version=0.0.4", runner.collect().expose().encode()),
        "/api/stats": json_handler(runner.stats),
        "/readyz": ready,
    }


async def serve_runner(
    runner: DurableRunner, args: argparse.Namespace, *, restart: bool = True
) -> None:
    """Run `runner` until a signal, with the monitoring server, printing a
    line at every start. Shared by `deltaflow run` and the demo worker."""
    stop = _stop_on_signals()

    def started() -> None:
        print(
            f"worker pid={os.getpid()} {runner.started_by}; checkpoint {runner.checkpointed_lsn}, "
            f"sinks written through {runner.flushed_lsn}",
            flush=True,
        )

    monitor = await _monitor(args, runner_routes(runner))
    try:
        if restart:
            await runner.serve(stop, on_start=started)
        else:
            await runner.start()
            started()
            await runner.run(stop)
    finally:
        if monitor is not None:
            await monitor.close()
        await runner.close()
        print(f"stopped: {dict(runner.counters)}", flush=True)


def _is_sql_file(spec: str) -> bool:
    return spec.endswith(".sql")


async def _sql_registry(
    args: argparse.Namespace, *, shards: int | None = None, shard_by: dict[str, str] | None = None
) -> Callable[[], Deltaflow]:
    """The views of a .sql file, prepared against the database (checked,
    typed, sinks created); a function building a fresh registry of them."""
    try:
        views = sqlviews.read_sql_file(args.views)
    except OSError as exc:
        raise CliError(f"cannot read {args.views}: {exc}") from None
    catalog = await sqlviews.prepare(
        args.dsn, views, create=args.create_sinks, shards=shards, shard_by=shard_by
    )
    print(f"{len(views)} SQL views checked by Postgres: {', '.join(v.table for v in views)}")
    return lambda: sqlviews.build(views, catalog, shards=shards, shard_by=shard_by)


async def _run(args: argparse.Namespace, make: Callable[[], Deltaflow] | None) -> None:
    if make is None:
        make = await _sql_registry(args)
    runner = DurableRunner(
        make(),
        args.dsn,
        state_dir=Path(args.state_dir or f".deltaflow/{args.slot}"),
        slot=args.slot,
        publication=args.publication or args.slot,
        memory_budget=args.memory_budget << 20,
        flush_interval=args.flush_interval,
        checkpoint_interval=args.checkpoint_interval,
        status_interval=args.status_interval,
        snapshot=args.snapshot,
        snapshot_chunk_rows=args.snapshot_chunk_rows,
        max_transaction_changes=args.max_transaction_changes,
        sink_spill_after=args.sink_spill_after,
    )
    await serve_runner(runner, args, restart=args.restart)


async def _cluster(args: argparse.Namespace, make: Callable[[], Deltaflow] | None) -> None:
    if make is None:
        if not args.shards or not args.shard_by:
            raise CliError("a .sql file in a cluster needs --shards and --shard-by table=column")
        shard_by = dict(_shard_column(item) for item in args.shard_by)
        make = await _sql_registry(args, shards=args.shards, shard_by=shard_by)
    elif args.shards or args.shard_by:
        raise CliError("--shards and --shard-by are for .sql files; a registry defines its own")
    publication = args.publication or args.slot_prefix
    registry = make()
    await prepare_cluster(args.dsn, registry, publication)
    worker = ClusterWorker(
        make,
        args.dsn,
        worker_id=args.worker_id,
        state_root=Path(args.state_root or f".deltaflow/{args.slot_prefix}"),
        slot_prefix=args.slot_prefix,
        publication=publication,
        heartbeat=args.heartbeat,
        ttl=args.ttl,
        runner_options={
            "flush_interval": args.flush_interval,
            "checkpoint_interval": args.checkpoint_interval,
            "memory_budget": args.memory_budget << 20,
            "status_interval": args.status_interval,
            "snapshot": args.snapshot,
            "snapshot_chunk_rows": args.snapshot_chunk_rows,
            "max_transaction_changes": args.max_transaction_changes,
            "sink_spill_after": args.sink_spill_after,
        },
    )
    stop = _stop_on_signals()
    monitor = await _monitor(
        args,
        {
            "/metrics": lambda: ("text/plain; version=0.0.4", worker.collect().encode()),
            "/api/stats": json_handler(worker.stats_json),
        },
    )
    print(f"cluster worker {args.worker_id} pid={os.getpid()} shards={registry.shards}", flush=True)
    try:
        await worker.run(stop)
    finally:
        if monitor is not None:
            await monitor.close()


def _shard_column(item: str) -> tuple[str, str]:
    table, eq, column = item.partition("=")
    if not eq or not table or not column:
        raise CliError(
            f"--shard-by {item!r}: expected TABLE=COLUMN, e.g. public.orders=customer_id"
        )
    return table if "." in table else f"public.{table}", column


async def status_report(dsn: str) -> dict[str, Any]:
    """Every logical slot with its retained WAL, and Deltaflow's own
    bookkeeping where it exists: watermarks, cluster workers, shard leases."""
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:

        async def query(q: str) -> list[tuple[Any, ...]]:
            async with conn.cursor() as cur:
                await cur.execute(q)
                return list(await cur.fetchall())

        async def exists(table: str) -> bool:
            return bool((await query(f"SELECT to_regclass('{table}') IS NOT NULL"))[0][0])

        slots = await query(
            "SELECT slot_name::text, active, active_pid, confirmed_flush_lsn::text,"
            " pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint,"
            " wal_status FROM pg_replication_slots WHERE slot_type = 'logical'"
            " ORDER BY slot_name"
        )
        report: dict[str, Any] = {
            "slots": [_slot(row) for row in slots],
            "watermarks": [],
            "workers": [],
            "leases": [],
        }
        if await exists("_deltaflow.watermarks"):
            report["watermarks"] = [
                {"view": v, "lsn": lsn, "age_seconds": round(float(age), 1)}
                for v, lsn, age in await query(
                    "SELECT view, lsn::text, extract(epoch FROM now() - updated_at)"
                    " FROM _deltaflow.watermarks ORDER BY view"
                )
            ]
        if await exists("_deltaflow.workers"):
            report["workers"] = [
                {"worker": w, "heartbeat_age_seconds": round(float(age), 1)}
                for w, age in await query(
                    "SELECT worker, extract(epoch FROM now() - heartbeat_at)"
                    " FROM _deltaflow.workers ORDER BY worker"
                )
            ]
        if await exists("_deltaflow.shard_leases"):
            report["leases"] = [
                {"shard": int(s), "owner": o, "epoch": int(e)}
                for s, o, e in await query(
                    "SELECT shard, owner, epoch FROM _deltaflow.shard_leases ORDER BY shard"
                )
            ]
    return report


def _slot(row: tuple[Any, ...]) -> dict[str, Any]:
    name, active, pid, lsn, lag, wal_status = row
    return {
        "slot": name,
        "active": active,
        "pid": pid,
        "confirmed_flush_lsn": lsn,
        "lag_bytes": None if lag is None else int(lag),
        "wal_status": wal_status,
    }


def _print_status(report: dict[str, Any]) -> None:
    def table(title: str, rows: list[dict[str, Any]], columns: list[str]) -> None:
        print(f"{title} ({len(rows)})")
        if not rows:
            return
        cells = [[("-" if r[c] is None else str(r[c])) for c in columns] for r in rows]
        widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(columns)]
        print("  " + "  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)))
        for row in cells:
            print("  " + "  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)))

    table("slots", report["slots"],
          ["slot", "active", "pid", "confirmed_flush_lsn", "lag_bytes", "wal_status"])  # fmt: skip
    table("watermarks", report["watermarks"], ["view", "lsn", "age_seconds"])
    if report["workers"] or report["leases"]:
        table("workers", report["workers"], ["worker", "heartbeat_age_seconds"])
        table("leases", report["leases"], ["shard", "owner", "epoch"])


async def _drop_slots(args: argparse.Namespace) -> list[str]:
    async with await psycopg.AsyncConnection.connect(args.dsn, autocommit=True) as conn:
        async with conn.cursor() as cur:
            if args.slot:
                await cur.execute(
                    "SELECT slot_name::text, active_pid FROM pg_replication_slots"
                    " WHERE slot_name = %s",
                    (args.slot,),
                )
            else:
                await cur.execute(
                    "SELECT slot_name::text, active_pid FROM pg_replication_slots"
                    " WHERE slot_name ~ %s ORDER BY slot_name",
                    (f"^{args.slot_prefix}_s[0-9]+$",),
                )
            slots = list(await cur.fetchall())
        # Dropping a slot a worker streams from would kill it mid-flight and
        # lose its position: refuse, before dropping any of them.
        if busy := [(name, pid) for name, pid in slots if pid is not None]:
            names = ", ".join(f"{name} (pid {pid})" for name, pid in busy)
            raise CliError(f"slot {names} is in use: stop its worker first")
        for name, _ in slots:
            await conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
    return [str(name) for name, _ in slots]


def _reset_state(args: argparse.Namespace, dropped: list[str]) -> None:
    state = Path(args.state_dir or f".deltaflow/{args.slot or args.slot_prefix}")
    if args.slot:
        if state.exists():
            StateDirectory(state).reset()
    else:
        for shard_dir in sorted(state.glob("shard-*")):
            shutil.rmtree(shard_dir)
    print(f"dropped slots: {', '.join(dropped) or 'none found'}; state reset in {state}")


async def _status(args: argparse.Namespace) -> None:
    report = await status_report(args.dsn)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        _print_status(report)


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    logs.configure(args.log_format)
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(errors="backslashreplace")
    try:
        if args.command == "run":
            command = _run(args, None if _is_sql_file(args.views) else load_views(args.views))
        elif args.command == "cluster":
            sql_file = _is_sql_file(args.views)
            command = _cluster(args, None if sql_file else load_views(args.views, factory=True))
        elif args.command == "status":
            command = _status(args)
        else:
            with contextlib.suppress(KeyboardInterrupt):
                _reset_state(args, aio.run(_drop_slots(args)))
            return
        with contextlib.suppress(KeyboardInterrupt):
            aio.run(command)
    except (CliError, PlanError) as exc:  # a mistake in the command or the views
        print(f"deltaflow: error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
