"""The engine: committed transactions in, maintained views out.

For each transaction:

1. every change to a source table goes through that table's arrangement,
   which turns it into whole-row deltas (row, commit_lsn, ±1);
2. the deltas are consolidated per source and pushed through the operator
   graph, and each view's output is handed to its sink;
3. every source's frontier advances to the transaction's `end_lsn`. Every
   later transaction commits at or after it, so the transaction's commit
   time is now complete, and sinks publish it.

Results become visible only in step 3, a whole transaction at a time. The
snapshot is installed at time 0, and `start(consistent_point)` completes
it: everything committed before the slot's consistent point is in the
snapshot, and everything after it arrives as a transaction.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from deltaflow.core import Delta, Frontier, FrontierViolationError, Row, consolidate
from deltaflow.faults import crash_point
from deltaflow.operators import Source
from deltaflow.planner import Deltaflow, FilterStep, GroupStep, JoinStep, MapStep, PlanError
from deltaflow.planner.compile import compile_views
from deltaflow.replication.changes import (
    RowDelete,
    RowInsert,
    RowUpdate,
    TableTruncate,
    Transaction,
)
from deltaflow.replication.lsn import Lsn
from deltaflow.state import TableArrangement, TableSchema
from deltaflow.state.store import StateStore

#: Timestamp of the initial snapshot: before every streamed transaction.
SNAPSHOT_TIME = Lsn(0)


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
    ) -> None:
        """All operator state (arrangements, join indices) goes into `store`.
        `namespace` prefixes every keyspace, so several engines can share one
        store (tests do). Snapshots are pushed through the graph in chunks
        of at most `snapshot_chunk_rows` rows or about `snapshot_chunk_bytes`
        of row data, so a large table is never in memory as a whole.

        `resume` is the `metadata()` saved with a state checkpoint, when
        `store` is that checkpoint restored: the engine continues from it
        instead of expecting snapshots."""
        registry.freeze()
        self._store = store
        self._namespace = namespace
        self._chunk_limits = (snapshot_chunk_rows, snapshot_chunk_bytes)
        self._plan = compile_views(registry.views, store, namespace)
        #: per operator: [deltas in, deltas out, seconds], by id(operator)
        self.operator_stats: dict[int, list[float]] = {
            id(op): [0, 0, 0.0] for op in self._plan.operators
        }
        self._arrangements: dict[str, TableArrangement] = {}
        self._started = False
        self.counters: Counter[str] = Counter()
        self._fingerprint = fingerprint(registry)
        if resume is not None:
            self._resume(resume)
        else:
            # Operators with output before any input (a global aggregate's row).
            self._step({}, initial=True)

    def metadata(self) -> dict[str, Any]:
        """What a checkpoint must record to resume this engine (JSON-able)."""
        if not self._started or self.frontier.lower is None:
            raise RuntimeError("metadata is only meaningful for a started, open engine")
        return {
            "version": 1,
            "lsn": int(self.frontier.lower),
            "fingerprint": self._fingerprint,
            "arrangements": {
                table: {
                    "columns": list(a.schema.columns),
                    "key": list(a.schema.key),
                    "count": len(a),
                }
                for table, a in self._arrangements.items()
            },
        }

    def _resume(self, meta: Mapping[str, Any]) -> None:
        if meta.get("fingerprint") != self._fingerprint:
            raise PlanError(
                "the views changed since the state checkpoint was taken; the state cannot "
                "be reused (reset it to rebuild from a fresh snapshot)"
            )
        for table, info in meta["arrangements"].items():
            schema = TableSchema(table, tuple(info["columns"]), tuple(info["key"]))
            self._arrangements[table] = TableArrangement(
                schema,
                self._store.keyspace(f"{self._namespace}arr:{table}"),
                count=int(info["count"]),
            )
        self._started = True
        self.advance(Frontier.at(Lsn(int(meta["lsn"]))))

    @property
    def sources(self) -> tuple[str, ...]:
        return tuple(self._plan.sources)

    @property
    def frontier(self) -> Frontier:
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
        arrangement = TableArrangement(
            schema, self._store.keyspace(f"{self._namespace}arr:{schema.table}")
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

    def apply(self, txn: Transaction) -> None:
        if not self._started:
            raise RuntimeError("engine not started: install snapshots, then start()")
        if not self.frontier.admits(txn.commit_lsn):
            raise FrontierViolationError(
                f"transaction {txn.xid} commits at {txn.commit_lsn}, behind the frontier "
                f"{self.frontier}: it was already applied"
            )
        batches: dict[str, list[Delta[Row]]] = {}
        for change in txn.changes:
            match change:
                case RowInsert() | RowUpdate() | RowDelete():
                    arrangement = self._arrangements.get(change.relation.qualified_name)
                    if arrangement is None:
                        continue
                    out = batches.setdefault(arrangement.schema.table, [])
                    out.extend(Delta(r, txn.commit_lsn, d) for r, d in arrangement.apply(change))
                case TableTruncate():
                    for rel in change.relations:
                        arrangement = self._arrangements.get(rel.qualified_name)
                        if arrangement is not None:
                            out = batches.setdefault(arrangement.schema.table, [])
                            out.extend(
                                Delta(r, txn.commit_lsn, d) for r, d in arrangement.truncate()
                            )
                case _:
                    pass  # logical messages carry no row data
        self._step(batches)
        crash_point("apply:after_state")  # state changed, frontier not yet advanced
        self.counters["transactions"] += 1
        self.advance(Frontier.at(txn.end_lsn))

    def advance(self, to: Frontier) -> None:
        for source in self._plan.sources.values():
            source.advance(to)
        for view, node in self._plan.outputs:
            view.sink.advance(node.frontier)

    def _install_chunk(self, arrangement: TableArrangement, chunk: list[Row]) -> None:
        self.counters["snapshot_chunks"] += 1
        for row in chunk:
            arrangement.load(row)
        self._step({arrangement.schema.table: [Delta(row, SNAPSHOT_TIME, 1) for row in chunk]})

    def _step(self, batches: dict[str, list[Delta[Row]]], *, initial: bool = False) -> None:
        """Push one batch per source through the graph. With `initial`, each
        operator first contributes its `initial()` deltas (time 0)."""
        outputs: dict[int, list[Delta[Row]]] = {}
        for op in self._plan.operators:
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
            stats = self.operator_stats[id(op)]
            stats[0] += received
            stats[1] += len(produced)
            stats[2] += time.perf_counter() - started
        for view, node in self._plan.outputs:
            if out := outputs.get(id(node)):
                view.sink.accept(consolidate(out))


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
    parameters of every step, and sinks. A checkpoint is only reused under
    the same fingerprint. Function *bodies* are not part of it (only their
    qualified names), so changing what a lambda computes needs a reset."""

    def step(s: object) -> object:
        match s:
            case FilterStep():
                return ["filter", getattr(s.predicate, "__qualname__", repr(s.predicate))]
            case MapStep():
                return ["map", getattr(s.fn, "__qualname__", repr(s.fn))]
            case JoinStep():
                return ["join", s.table, s.left_on, s.right_on, s.left_prefix, s.right_prefix]
            case GroupStep():
                return ["group", s.keys, {k: repr(v) for k, v in s.aggregates.items()}]
            case _:
                return [type(s).__name__]

    views = [
        [
            v.name,
            v.source,
            [step(s) for s in v.steps],
            type(v.sink).__name__,
            getattr(v.sink, "table", None),
        ]
        for v in registry.views
    ]
    return hashlib.sha256(json.dumps(views, sort_keys=True, default=list).encode()).hexdigest()
