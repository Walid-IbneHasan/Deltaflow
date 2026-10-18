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

from collections import Counter
from collections.abc import Iterable

from deltaflow.core import Delta, Frontier, FrontierViolationError, Row, consolidate
from deltaflow.operators import Source
from deltaflow.planner import Deltaflow, PlanError
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
    ) -> None:
        """All operator state (arrangements, join indices) goes into `store`.
        `namespace` prefixes every keyspace, so several engines can share one
        store (tests do). Snapshots are pushed through the graph in chunks
        of at most `snapshot_chunk_rows` rows or about `snapshot_chunk_bytes`
        of row data, so a large table is never in memory as a whole."""
        registry.freeze()
        self._store = store
        self._namespace = namespace
        self._chunk_limits = (snapshot_chunk_rows, snapshot_chunk_bytes)
        self._plan = compile_views(registry.views, store, namespace)
        self._arrangements: dict[str, TableArrangement] = {}
        self._started = False
        self.counters: Counter[str] = Counter()

    @property
    def sources(self) -> tuple[str, ...]:
        return tuple(self._plan.sources)

    @property
    def frontier(self) -> Frontier:
        return Frontier.meet(*(s.frontier for s in self._plan.sources.values()))

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

    def _step(self, batches: dict[str, list[Delta[Row]]]) -> None:
        outputs: dict[int, list[Delta[Row]]] = {}
        for op in self._plan.operators:
            produced: list[Delta[Row]] = []
            if isinstance(op, Source):
                if batch := batches.get(op.table):
                    produced = op.process(0, consolidate(batch))
                    self.counters["source_deltas"] += len(produced)
            else:
                for port, inp in enumerate(op.inputs):
                    if upstream := outputs.get(id(inp)):
                        produced.extend(op.process(port, upstream))
            outputs[id(op)] = produced
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
