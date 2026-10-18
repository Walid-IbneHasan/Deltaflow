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

#: Timestamp of the initial snapshot: before every streamed transaction.
SNAPSHOT_TIME = Lsn(0)


class Engine:
    def __init__(self, registry: Deltaflow) -> None:
        registry.freeze()
        self._plan = compile_views(registry.views)
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
        if schema.table not in self._plan.sources:
            raise PlanError(f"no view reads {schema.table}")
        if self._started:
            raise RuntimeError("snapshots must be installed before start()")
        arrangement = TableArrangement(schema)
        deltas = []
        for row in rows:
            arrangement.load(row)
            deltas.append(Delta(row, SNAPSHOT_TIME, 1))
        self._arrangements[schema.table] = arrangement
        self._step({schema.table: deltas})

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
