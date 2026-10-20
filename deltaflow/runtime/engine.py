"""The engine: committed transactions in, maintained views out.

For each transaction:

1. every change to a source table goes through that table's arrangement,
   which turns it into whole-row deltas (row, commit_lsn, ±1);
2. the deltas are consolidated per source and pushed through the operator
   graph, and each view's output is handed to its sink;
3. every source's frontier advances to the transaction's `end_lsn`. Every
   later transaction commits at or after it, so the transaction's commit
   time is now complete, and sinks publish it.

**Per-view state.** A checkpoint records each view's fingerprint and the
keyspaces its operators own. Resumed under a changed registry, the engine
keeps the views whose fingerprint is unchanged, drops the removed ones'
state, and *rebuilds* the new and changed ones from the source rows the
checkpoint holds (the arrangements, decision 0003), exactly as of its
position: no snapshot, and the other views are not touched. A view that
reads a table the checkpoint has no copy of cannot be rebuilt that way
(`cannot_resume` says why); the runner then bootstraps everything.

Results become visible only in step 3, a whole transaction at a time. The
snapshot is installed at time 0, and `start(consistent_point)` completes
it: everything committed before the slot's consistent point is in the
snapshot, and everything after it arrives as a transaction.

**Loading in chunks (decision 0009).** Built with `chunked=True`, the
engine is loaded with `begin_load`: the stream's transactions and chunks
read at their own consistent points go into the arrangements only (the
views have no state yet), until `ready()`. Then `go_live` builds every
view from the arrangements, as a snapshot is installed, at the engine's
frontier.
"""

from __future__ import annotations

import base64
import hashlib
import json
import pickle
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from deltaflow.core import (
    Delta,
    Frontier,
    FrontierRegressionError,
    FrontierViolationError,
    Row,
    consolidate,
)
from deltaflow.faults import crash_point
from deltaflow.operators import Source
from deltaflow.planner import Deltaflow, FilterStep, GroupStep, JoinStep, MapStep, PlanError, View
from deltaflow.planner.compile import Plan, compile_views
from deltaflow.replication.changes import (
    Change,
    RowDelete,
    RowInsert,
    RowUpdate,
    TableTruncate,
    Transaction,
    TransactionPart,
)
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import UNCHANGED_TOAST
from deltaflow.sharding import shard_of
from deltaflow.state import TableArrangement, TableSchema
from deltaflow.state.store import StateStore

#: Timestamp of the initial snapshot: before every streamed transaction.
SNAPSHOT_TIME = Lsn(0)
#: The format of the state and its checkpoint metadata. A version that
#: cannot read another's checkpoints bootstraps instead (`cannot_resume`).
STATE_VERSION = 1


class Engine:
    def __init__(
        self,
        registry: Deltaflow,
        store: StateStore,
        *,
        namespace: str = "",
        snapshot_chunk_rows: int = 5_000,
        snapshot_chunk_bytes: int = 2 << 20,
        resume: Mapping[str, Any] | None = None,
        shard: int | None = None,
        chunked: bool = False,
    ) -> None:
        """All operator state (arrangements, join indices) goes into `store`.
        `namespace` prefixes every keyspace, so several engines can share one
        store (tests do). Snapshots are pushed through the graph in chunks
        of at most `snapshot_chunk_rows` rows or about `snapshot_chunk_bytes`
        of row data, so a large table is never in memory as a whole.

        `resume` is the `metadata()` saved with a state checkpoint, when
        `store` is that checkpoint restored: the engine continues from it
        instead of expecting snapshots.

        With `shard`, the registry must be sharded and the engine keeps only
        that shard's rows: its views' results are this shard's partials
        (planner/merge.py combines them).

        With `chunked`, the engine is loaded with `begin_load` instead of
        snapshots (decision 0009)."""
        if shard is not None and (registry.shards is None or not 0 <= shard < registry.shards):
            raise PlanError(f"shard {shard} given for a registry with shards={registry.shards}")
        self.shard = shard
        self._owns: dict[str, Callable[[Mapping[str, Any]], bool]] = {}
        if shard is not None and registry.shards is not None:
            n = registry.shards
            for table, column in registry.shard_by.items():
                self._owns[table] = _owner_check(column, n, shard)
        registry.freeze()
        self._registry = registry
        self._store = store
        self._namespace = namespace
        self._chunk_limits = (snapshot_chunk_rows, snapshot_chunk_bytes)
        #: every source table's schema, once declared (or loaded, or resumed)
        self._schemas: dict[str, TableSchema] = {}
        self._plan = compile_views(registry.views, store, namespace, self._columns_of)
        #: per view, the keyspaces its operators own (sources are shared)
        self._view_keyspaces: dict[str, list[str]] = {v.name: [] for v in registry.views}
        for op in self._plan.operators:
            for view in self._plan.nodes[id(op)].views:
                if not isinstance(op, Source):
                    self._view_keyspaces[view].extend(op.state_keyspaces)
        #: views built from the checkpoint's source rows on resume, and views dropped
        self.rebuilt: tuple[str, ...] = ()
        self.dropped: tuple[str, ...] = ()
        #: per operator: [deltas in, deltas out, seconds], by id(operator)
        self.operator_stats: dict[int, list[float]] = {
            id(op): [0, 0, 0.0] for op in self._plan.operators
        }
        self._arrangements: dict[str, TableArrangement] = {}
        self._started = False
        self.counters: Counter[str] = Counter()
        self._fingerprint = fingerprint(registry)
        #: while loading in chunks: the frontier (the plan's sources stay
        #: at 0 until go_live), each table's paging, the newest chunk's CP
        self._loading = False
        self._load_lsn = SNAPSHOT_TIME
        self._load: dict[str, TableLoad] = {}
        self._max_cp = SNAPSHOT_TIME
        #: (xid, commit LSN) of a transaction applied in part (apply_part)
        self._partial: tuple[int, Lsn] | None = None
        if resume is not None:
            self._resume(resume)
        elif not chunked:
            # Operators with output before any input (a global aggregate's
            # row). Loaded in chunks, the views are built at go_live instead.
            self._step({}, initial=True)

    def metadata(self) -> dict[str, Any]:
        """What a checkpoint must record to resume this engine (JSON-able)."""
        if not self._started or self.frontier.lower is None:
            raise RuntimeError("metadata is only meaningful for a started, open engine")
        if self._partial is not None:
            raise RuntimeError(
                f"transaction {self._partial[0]} is applied in part; the state is not at a "
                "transaction boundary"
            )
        loading = {"loading": self._load_meta()} if self._loading else {}
        return {
            **loading,
            "version": STATE_VERSION,
            "lsn": int(self.frontier.lower),
            "fingerprint": self._fingerprint,
            "shard": self.shard,
            "sharding": _sharding(self._registry),
            "views": {
                v.name: {
                    "fingerprint": view_fingerprint(v, self._registry),
                    "keyspaces": self._view_keyspaces[v.name],
                }
                for v in self._registry.views
            },
            "arrangements": {
                table: {
                    "columns": list(a.schema.columns),
                    "key": list(a.schema.key),
                    "types": list(a.schema.types),
                    "typmods": list(a.schema.typmods),
                    "partitioned": a.schema.partitioned,
                    "count": len(a),
                }
                for table, a in self._arrangements.items()
            },
        }

    @staticmethod
    def cannot_resume(registry: Deltaflow, meta: Mapping[str, Any]) -> str | None:
        """Why a checkpoint taken under `meta` cannot be resumed with
        `registry` (None if it can, rebuilding changed views if need be)."""
        if (version := meta.get("version")) != STATE_VERSION:
            return (
                f"the checkpoint's state format is version {version}; this one is {STATE_VERSION}"
            )
        if meta.get("fingerprint") == fingerprint(registry):
            return None
        if "loading" in meta:
            return "the views changed while their tables were being loaded"
        if "views" not in meta:
            return "the views changed since the checkpoint, which predates per-view state"
        if meta.get("sharding") != _sharding(registry):
            return "the sharding changed since the checkpoint"
        return Engine._unbuildable(registry, meta)

    @staticmethod
    def _unbuildable(registry: Deltaflow, meta: Mapping[str, Any]) -> str | None:
        """A changed or new view that reads a table the checkpoint has no
        copy of, so that it cannot be built from the checkpoint."""
        arranged = set(meta["arrangements"])
        for view in Engine.views_to_rebuild(registry, meta):
            for table in view.sources:
                if table not in arranged:
                    return (
                        f"view {view.name} reads {table}, and the checkpoint has no copy of "
                        f"{table} to build it from"
                    )
        return None

    @staticmethod
    def views_to_rebuild(registry: Deltaflow, meta: Mapping[str, Any]) -> list[View]:
        """The new views, and those whose fingerprint changed."""
        old = meta.get("views") or {}
        return [
            v for v in registry.views
            if old.get(v.name, {}).get("fingerprint") != view_fingerprint(v, registry)
        ]  # fmt: skip

    def _resume(self, meta: Mapping[str, Any]) -> None:
        if meta.get("shard") != self.shard:
            raise PlanError(f"checkpoint is of shard {meta.get('shard')}, not {self.shard}")
        if (reason := Engine.cannot_resume(self._registry, meta)) is not None:
            raise PlanError(f"the state checkpoint cannot be resumed: {reason}")
        if "loading" in meta:
            self._resume_load(meta)
            return
        rebuild = [] if "views" not in meta else Engine.views_to_rebuild(self._registry, meta)
        old: Mapping[str, Any] = meta.get("views") or {}
        dropped = tuple(name for name in old if name not in self._view_keyspaces)
        used = set(self._plan.sources)
        for table, info in meta["arrangements"].items():
            keyspace = self._store.keyspace(f"{self._namespace}arr:{table}")
            if table not in used:
                # No view reads it any more, and its rows would go stale (the
                # publication stops covering it): a later view must not
                # mistake them for current.
                keyspace.clear()
                continue
            schema = _schema(table, info)
            self._schemas[table] = schema
            self._arrangements[table] = TableArrangement(
                schema, keyspace, count=int(info["count"]), owns=self._owns.get(table)
            )
        for name in (*dropped, *(v.name for v in rebuild)):
            for keyspace_name in (
                *old.get(name, {}).get("keyspaces", ()),
                *self._view_keyspaces.get(name, ()),
            ):
                self._store.keyspace(keyspace_name).clear()
        self._started = True
        lsn = Lsn(int(meta["lsn"]))
        if rebuild:
            self._rebuild(rebuild, lsn)
        self.advance(Frontier.at(lsn))
        self.rebuilt = tuple(v.name for v in rebuild)
        self.dropped = dropped

    def _rebuild(self, views: list[View], lsn: Lsn) -> None:
        """Build `views` from the arrangements, as of `lsn`: their own
        operators, fed every source row at time 0 (as a snapshot is), in
        chunks. Their state lands in the keyspaces the main plan's
        operators of the same views use."""
        plan = compile_views(views, self._store, self._namespace, self._columns_of)
        self._run_step(plan, {}, initial=True)
        limit = self._chunk_limits[0]
        for table in plan.sources:
            chunk: list[Delta[Row]] = []
            for row in self._arrangements[table].rows():
                chunk.append(Delta(row, SNAPSHOT_TIME, 1))
                if len(chunk) >= limit:
                    self._run_step(plan, {table: chunk})
                    chunk = []
            if chunk:
                self._run_step(plan, {table: chunk})
        for source in plan.sources.values():
            source.advance(Frontier.at(lsn))
        for view, node in plan.outputs:
            view.sink.advance(node.frontier)
        self.counters["views_rebuilt"] += len(views)

    # --- loading in chunks (decision 0009) ---------------------------------

    def begin_load(self, schemas: Sequence[TableSchema], at: Lsn) -> None:
        """Start loading every source in chunks, the stream running from
        `at` (the slot's consistent point)."""
        if self._started:
            raise RuntimeError("begin_load on a started engine")
        if missing := set(self._plan.sources) - {s.table for s in schemas}:
            raise PlanError(f"no schema given for {sorted(missing)}")
        for schema in schemas:
            self.declare(schema)
        for schema in schemas:
            for kind in ("arr", "mark", "reread"):
                self._store.keyspace(f"{self._namespace}{kind}:{schema.table}").clear()
            self._arrangements[schema.table] = self._loading_arrangement(schema, count=0)
            self._load[schema.table] = TableLoad()
        self._loading = self._started = True
        self._load_lsn = at

    @property
    def loading(self) -> bool:
        """Being loaded in chunks (begin_load, not yet go_live)."""
        return self._loading

    @property
    def live(self) -> bool:
        """Started and loaded: its views are maintained."""
        return self._started and not self._loading

    def next_page(self) -> tuple[TableSchema, tuple[Any, ...] | None] | None:
        """The next page to read: a table, and the last key read before it
        (None: from the start). None once every table has been read."""
        for table, progress in self._load.items():
            if not progress.done:
                return self._schemas[table], progress.after
        return None

    def load_chunk(
        self, table: str, rows: Sequence[Row], cp: Lsn, *, after: tuple[Any, ...] | None
    ) -> None:
        """Merge one page, read at consistent point `cp`. `after` is the
        page's last key when the page was full (more may follow); None when
        it was the table's last page."""
        self._loading_check()
        progress = self._load[table]
        self._arrangements[table].load_chunk(rows, cp)
        progress.chunks += 1
        progress.rows += len(rows)
        progress.after, progress.done = after, after is None
        self._max_cp = max(self._max_cp, cp)
        self.counters["snapshot_chunks"] += 1
        self.counters["snapshot_rows"] += len(rows)

    def rereads(self) -> dict[str, list[tuple[Any, ...]]]:
        """Per table, the keys to read again (decision 0009)."""
        self._loading_check()
        return {t: keys for t, a in self._arrangements.items() if (keys := a.rereads())}

    def reread(
        self, table: str, keys: Sequence[tuple[Any, ...]], rows: Sequence[Row], cp: Lsn
    ) -> None:
        self._loading_check()
        self._arrangements[table].reread(keys, rows, cp)
        self._max_cp = max(self._max_cp, cp)
        self.counters["rereads"] += len(keys)

    def ready(self) -> bool:
        """Loaded: every page read, no re-read pending, and the stream past
        the newest chunk's consistent point, so every change older than any
        chunk has been applied."""
        return (
            self._loading
            and self._partial is None
            and all(p.done for p in self._load.values())
            and not any(a.rereads() for a in self._arrangements.values())
            and self._load_lsn >= self._max_cp
        )

    def go_live(self) -> None:
        """Build every view from the arrangements, at the frontier."""
        if not self.ready():
            raise RuntimeError("go_live before the load is complete")
        for arrangement in self._arrangements.values():
            arrangement.finish_loading()
        self._loading = False
        self._step({}, initial=True)
        limit = self._chunk_limits[0]
        for table, arrangement in self._arrangements.items():
            chunk: list[Delta[Row]] = []
            for row in arrangement.rows():
                chunk.append(Delta(row, SNAPSHOT_TIME, 1))
                if len(chunk) >= limit:
                    self._step({table: chunk})
                    chunk = []
            if chunk:
                self._step({table: chunk})
        self.advance(Frontier.at(self._load_lsn))
        self._load = {}

    def load_progress(self) -> dict[str, dict[str, Any]]:
        """Per table: rows and chunks read, and whether it is done."""
        return {
            t: {"rows": p.rows, "chunks": p.chunks, "done": p.done} for t, p in self._load.items()
        }

    def _loading_arrangement(self, schema: TableSchema, count: int) -> TableArrangement:
        ks = self._store.keyspace
        table = schema.table
        return TableArrangement(
            schema,
            ks(f"{self._namespace}arr:{table}"),
            count=count,
            owns=self._owns.get(table),
            marks=ks(f"{self._namespace}mark:{table}"),
            rereads=ks(f"{self._namespace}reread:{table}"),
        )

    def _loading_check(self) -> None:
        if not self._loading:
            raise RuntimeError("the engine is not loading")

    def _load_meta(self) -> dict[str, Any]:
        return {
            "max_cp": int(self._max_cp),
            "tables": {
                t: {
                    "after": None if p.after is None else _pack(p.after),
                    "done": p.done,
                    "chunks": p.chunks,
                    "rows": p.rows,
                }
                for t, p in self._load.items()
            },
        }

    def _resume_load(self, meta: Mapping[str, Any]) -> None:
        loading = meta["loading"]
        for table, info in meta["arrangements"].items():
            schema = _schema(table, info)
            self._schemas[table] = schema
            self._arrangements[table] = self._loading_arrangement(schema, count=int(info["count"]))
        for table, info in loading["tables"].items():
            after = None if info["after"] is None else _unpack(info["after"])
            self._load[table] = TableLoad(after, info["done"], info["chunks"], info["rows"])
        self._max_cp = Lsn(int(loading["max_cp"]))
        self._load_lsn = Lsn(int(meta["lsn"]))
        self._loading = self._started = True

    # --- running ----------------------------------------------------------------

    @property
    def sources(self) -> tuple[str, ...]:
        return tuple(self._plan.sources)

    @property
    def frontier(self) -> Frontier:
        if self._loading:
            return Frontier.at(self._load_lsn)
        return Frontier.meet(*(s.frontier for s in self._plan.sources.values()))

    def graph(self) -> dict[str, Any]:
        """The operator graph and per-operator counters, JSON-able."""
        nodes = []
        for op in self._plan.operators:
            info = self._plan.nodes[id(op)]
            d_in, d_out, seconds = self.operator_stats[id(op)]
            nodes.append(
                {
                    "id": info.id,
                    "kind": info.kind,
                    "label": info.label,
                    "inputs": list(info.inputs),
                    "views": list(info.views),
                    "deltas_in": d_in,
                    "deltas_out": d_out,
                    "seconds": seconds,
                }
            )
        sinks = [
            {
                "id": f"{view.name}/sink",
                "kind": "sink",
                "label": getattr(view.sink, "table", type(view.sink).__name__),
                "inputs": [self._plan.nodes[id(node)].id],
                "views": [view.name],
            }
            for view, node in self._plan.outputs
        ]
        return {"nodes": [*nodes, *sinks]}

    @property
    def arrangements(self) -> dict[str, TableArrangement]:
        return dict(self._arrangements)

    def declare(self, schema: TableSchema) -> None:
        """Make a source table's schema known before its rows arrive. A left
        join pads rows with its right table's columns, possibly while
        another table's snapshot is being loaded; load_snapshot therefore
        declares every table first."""
        if schema.table not in self._plan.sources:
            raise PlanError(f"no view reads {schema.table}")
        self._schemas[schema.table] = schema

    def _columns_of(self, table: str) -> tuple[str, ...]:
        schema = self._schemas.get(table)
        return () if schema is None else schema.columns

    def install(self, schema: TableSchema, rows: Iterable[Row]) -> None:
        """Load one source table's snapshot."""
        loader = self.begin_install(schema)
        for row in rows:
            loader.add(row)
        loader.finish()

    def begin_install(self, schema: TableSchema) -> SnapshotLoader:
        """Load one source table's snapshot row by row (for async readers)."""
        if schema.table not in self._plan.sources:
            raise PlanError(f"no view reads {schema.table}")
        if self._started:
            raise RuntimeError("snapshots must be installed before start()")
        if schema.table in self._arrangements:
            raise RuntimeError(f"snapshot of {schema.table} already installed")
        self._schemas[schema.table] = schema
        arrangement = TableArrangement(
            schema,
            self._store.keyspace(f"{self._namespace}arr:{schema.table}"),
            owns=self._owns.get(schema.table),
        )
        self._arrangements[schema.table] = arrangement
        return SnapshotLoader(self, arrangement, *self._chunk_limits)

    def start(self, consistent_point: Lsn) -> None:
        """All snapshots are installed; they are complete as of `consistent_point`."""
        for table in self._plan.sources:
            if table not in self._arrangements:
                raise RuntimeError(f"snapshot of {table} not installed")
        self._started = True
        self.advance(Frontier.at(consistent_point))

    @property
    def partial(self) -> bool:
        """A transaction is applied in part: not at a transaction boundary,
        so neither checkpoint nor go live."""
        return self._partial is not None

    def apply_part(self, part: TransactionPart) -> None:
        """Changes of a large transaction whose end has not arrived yet. They
        are applied at its commit time; nothing at that time is complete
        until `apply` of the transaction's final part advances the frontier."""
        self._admit(part.xid, part.commit_lsn)
        if self._partial is not None and self._partial != (part.xid, part.commit_lsn):
            raise RuntimeError(
                f"part of transaction {part.xid} while {self._partial[0]} is unfinished"
            )
        self._partial = (part.xid, part.commit_lsn)
        if self._loading:
            self._load_changes(part.changes, part.commit_lsn)
        else:
            self._step(self._batches(part.changes, part.commit_lsn))
        self.counters["transaction_parts"] += 1

    def apply(self, txn: Transaction) -> None:
        self._admit(txn.xid, txn.commit_lsn)
        if txn.continued != (self._partial == (txn.xid, txn.commit_lsn)):
            raise RuntimeError(
                f"transaction {txn.xid}: its parts and its end do not match "
                f"(continued={txn.continued}, applied in part: {self._partial})"
            )
        self._partial = None
        if self._loading:
            self._apply_loading(txn)
            return
        self._step(self._batches(txn.changes, txn.commit_lsn))
        crash_point("apply:after_state")  # state changed, frontier not yet advanced
        self.counters["transactions"] += 1
        self.advance(Frontier.at(txn.end_lsn))

    def _admit(self, xid: int, commit_lsn: Lsn) -> None:
        if not self._started:
            raise RuntimeError("engine not started: install snapshots, then start()")
        if not self.frontier.admits(commit_lsn):
            raise FrontierViolationError(
                f"transaction {xid} commits at {commit_lsn}, behind the frontier "
                f"{self.frontier}: it was already applied"
            )

    def _batches(self, changes: Iterable[Change], commit_lsn: Lsn) -> dict[str, list[Delta[Row]]]:
        """Each source table's deltas from `changes`, at `commit_lsn`."""
        batches: dict[str, list[Delta[Row]]] = {}
        for change in changes:
            match change:
                case RowInsert() | RowUpdate() | RowDelete():
                    arrangement = self._arrangements.get(change.relation.qualified_name)
                    if arrangement is None:
                        continue
                    out = batches.setdefault(arrangement.schema.table, [])
                    out.extend(Delta(r, commit_lsn, d) for r, d in arrangement.apply(change))
                case TableTruncate():
                    for rel in change.relations:
                        arrangement = self._arrangements.get(rel.qualified_name)
                        if arrangement is not None:
                            out = batches.setdefault(arrangement.schema.table, [])
                            out.extend(Delta(r, commit_lsn, d) for r, d in arrangement.truncate())
                case _:
                    pass  # logical messages carry no row data
        return batches

    def _apply_loading(self, txn: Transaction) -> None:
        """Into the arrangements only: the views are built at go_live."""
        self._load_changes(txn.changes, txn.commit_lsn)
        crash_point("apply:after_state")
        self.counters["transactions"] += 1
        self.advance(Frontier.at(txn.end_lsn))

    def _load_changes(self, changes: Iterable[Change], commit_lsn: Lsn) -> None:
        for change in changes:
            match change:
                case RowInsert() | RowUpdate() | RowDelete():
                    arrangement = self._arrangements.get(change.relation.qualified_name)
                    if arrangement is not None:
                        arrangement.apply_loading(change, commit_lsn)
                case TableTruncate():
                    for rel in change.relations:
                        if (arrangement := self._arrangements.get(rel.qualified_name)) is not None:
                            arrangement.truncate_loading(commit_lsn)
                case _:
                    pass

    def advance(self, to: Frontier) -> None:
        if self._loading:
            if to.lower is None or to.lower < self._load_lsn:
                raise FrontierRegressionError(
                    f"frontier would move back from {self._load_lsn} to {to}"
                )
            self._load_lsn = to.lower
            return
        for source in self._plan.sources.values():
            source.advance(to)
        for view, node in self._plan.outputs:
            view.sink.advance(node.frontier)

    def _install_chunk(self, arrangement: TableArrangement, chunk: list[Row]) -> None:
        self.counters["snapshot_chunks"] += 1
        self.counters["snapshot_rows"] += len(chunk)
        mine = [row for row in chunk if arrangement.load(row)]
        self._step({arrangement.schema.table: [Delta(row, SNAPSHOT_TIME, 1) for row in mine]})

    def _step(self, batches: dict[str, list[Delta[Row]]], *, initial: bool = False) -> None:
        self._run_step(self._plan, batches, initial=initial)

    def _run_step(
        self, plan: Plan, batches: dict[str, list[Delta[Row]]], *, initial: bool = False
    ) -> None:
        """Push one batch per source through `plan`. With `initial`, each
        operator first contributes its `initial()` deltas (time 0)."""
        outputs: dict[int, list[Delta[Row]]] = {}
        unrecorded = [0, 0, 0.0]
        for op in plan.operators:
            started = time.perf_counter()
            received = 0
            produced: list[Delta[Row]] = op.initial() if initial else []
            if isinstance(op, Source):
                if batch := batches.get(op.table):
                    received = len(batch)
                    produced = op.process(0, consolidate(batch))
                    self.counters["source_deltas"] += len(produced)
            else:
                for port, inp in enumerate(op.inputs):
                    if upstream := outputs.get(id(inp)):
                        received += len(upstream)
                        produced.extend(op.process(port, upstream))
            outputs[id(op)] = produced
            stats = self.operator_stats.get(id(op), unrecorded)
            stats[0] += received
            stats[1] += len(produced)
            stats[2] += time.perf_counter() - started
        for view, node in plan.outputs:
            if out := outputs.get(id(node)):
                view.sink.accept(consolidate(out))


@dataclass(slots=True)
class TableLoad:
    """A table being loaded in chunks: the last key read (the paging
    position), whether its last page has been read, and counts."""

    after: tuple[Any, ...] | None = None
    done: bool = False
    chunks: int = 0
    rows: int = 0


def _schema(table: str, info: Mapping[str, Any]) -> TableSchema:
    """A source table's schema from checkpoint metadata (older ones have no types)."""
    return TableSchema(
        table,
        tuple(info["columns"]),
        tuple(info["key"]),
        tuple(int(t) for t in info.get("types", ())),
        bool(info.get("partitioned", False)),
        tuple(int(m) for m in info.get("typmods", ())),
    )


def _pack(key: tuple[Any, ...]) -> str:
    """A key in checkpoint metadata (JSON), exactly: Decimal scale, tzinfo."""
    return base64.b64encode(pickle.dumps(key, protocol=5)).decode()


def _unpack(text: str) -> tuple[Any, ...]:
    key = pickle.loads(base64.b64decode(text))
    assert isinstance(key, tuple)
    return key


class SnapshotLoader:
    """Buffers snapshot rows and pushes them through the engine in chunks."""

    def __init__(
        self, engine: Engine, arrangement: TableArrangement, max_rows: int, max_bytes: int
    ) -> None:
        self._engine = engine
        self._arrangement = arrangement
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._chunk: list[Row] = []
        self._bytes = 0

    def add(self, row: Row) -> None:
        self._chunk.append(row)
        self._bytes += _approx_size(row)
        if len(self._chunk) >= self._max_rows or self._bytes >= self._max_bytes:
            self._flush()

    def finish(self) -> None:
        self._flush()

    def _flush(self) -> None:
        if self._chunk:
            self._engine._install_chunk(self._arrangement, self._chunk)
        self._chunk = []
        self._bytes = 0


def _approx_size(row: Row) -> int:
    """Rough bytes held by a row: its strings and byte strings dominate."""
    return 64 + sum(len(v) if isinstance(v, str | bytes) else 16 for v in (row[c] for c in row))


def fingerprint(registry: Deltaflow) -> str:
    """A digest of the views' structure: names, sources, the kind and static
    parameters of every step, and sinks. Function *bodies* are not part of
    it (only their qualified names), so changing what a lambda computes
    needs a reset; a SQL view's qualified names carry its SQL."""
    views = [_view_description(v) for v in registry.views]
    return hashlib.sha256(
        json.dumps([views, _sharding(registry)], sort_keys=True, default=list).encode()
    ).hexdigest()


def view_fingerprint(view: View, registry: Deltaflow) -> str:
    """One view's fingerprint, under the registry's sharding."""
    return hashlib.sha256(
        json.dumps(
            [_view_description(view), _sharding(registry)], sort_keys=True, default=list
        ).encode()
    ).hexdigest()


def _sharding(registry: Deltaflow) -> list[Any]:
    return [registry.shards, sorted(registry.shard_by.items())]


def _view_description(v: View) -> list[Any]:
    def step(s: object) -> object:
        match s:
            case FilterStep():
                return ["filter", getattr(s.predicate, "__qualname__", repr(s.predicate))]
            case MapStep():
                return ["map", getattr(s.fn, "__qualname__", repr(s.fn))]
            case JoinStep():
                # `how` only when not inner, so existing checkpoints keep
                # their fingerprint
                kind = [] if s.how == "inner" else [s.how]
                return [
                    "join",
                    s.table,
                    s.left_on,
                    s.right_on,
                    s.left_prefix,
                    s.right_prefix,
                    *kind,
                ]
            case GroupStep():
                aggs = {k: repr(v) for k, v in s.aggregates.items()}
                return ["group", s.keys, s.output_keys, aggs]
            case _:
                return [type(s).__name__]

    return [
        v.name,
        v.source,
        [step(s) for s in v.steps],
        type(v.sink).__name__,
        getattr(v.sink, "table", None),
    ]


def _owner_check(column: str, shards: int, shard: int) -> Callable[[Mapping[str, Any]], bool]:
    def owns(row: Mapping[str, Any]) -> bool:
        value = row[column]
        if value is UNCHANGED_TOAST:
            raise TypeError(f"shard column {column!r} sent as unchanged-TOAST placeholder")
        return shard_of(value, shards) == shard

    return owns
