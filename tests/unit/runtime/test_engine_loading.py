"""An engine loaded in chunks (decision 0009) goes live with exactly the
views a snapshot-installed engine shows after the same transactions, and
from then on produces the same results; also when the load is
checkpointed halfway and restored."""

from __future__ import annotations

import random
from decimal import Decimal
from pathlib import Path

import pytest

from deltaflow.core import Delta, Frontier, Row, accumulate
from deltaflow.replication.changes import RowDelete, RowInsert, RowUpdate, Transaction
from deltaflow.replication.lsn import Lsn
from deltaflow.runtime import Engine
from deltaflow.state import TableSchema
from deltaflow.state.durable import StateDirectory
from tests.unit.runtime.test_engine_resume import (
    C_SCHEMA,
    CP,
    MB,
    O_SCHEMA,
    Recorder,
    install,
    registry,
    workload,
)

type Tables = dict[str, dict[int, Row]]


def states(txns: list[Transaction]) -> list[Tables]:
    """The tables just before each transaction commits, and after the last."""
    tables: Tables = {
        O_SCHEMA.table: {i: Row({"id": i, "cid": i % 3, "amt": Decimal(i)}) for i in range(10)},
        C_SCHEMA.table: {i: Row({"id": i, "country": "ABC"[i]}) for i in range(3)},
    }
    out = []
    for txn in txns:
        out.append({t: dict(rows) for t, rows in tables.items()})
        for c in txn.changes:
            assert isinstance(c, RowInsert | RowUpdate | RowDelete)
            rows = tables[c.relation.qualified_name]
            if isinstance(c, RowDelete):
                del rows[c.old["id"]]
            else:
                rows[c.new["id"]] = Row(c.new)
    out.append({t: dict(rows) for t, rows in tables.items()})
    return out


def cp_before(txns: list[Transaction], m: int) -> Lsn:
    """A consistent point just before transaction m commits (or after all)."""
    return txns[m].commit_lsn if m < len(txns) else Lsn(CP + 0x100 * (len(txns) + 1))


def load(
    engine: Engine,
    txns: list[Transaction],
    before: list[Tables],
    rng: random.Random,
    *,
    applied: int,
    page: int,
    stop_after_chunks: int | None = None,
) -> int:
    """Interleave pages and transactions until the engine is ready (or has
    merged `stop_after_chunks` pages). Returns the transactions applied."""
    chunks = 0
    while not engine.ready():
        if stop_after_chunks is not None and chunks == stop_after_chunks:
            return applied
        m = min(applied + rng.randint(0, 3), len(txns))
        if (nxt := engine.next_page()) is not None:
            schema, after = nxt
            keys = sorted(k for k in before[m][schema.table] if after is None or k > after[0])
            rows = [before[m][schema.table][k] for k in keys[:page]]
            last = (rows[-1]["id"],) if len(rows) == page else None
            engine.load_chunk(schema.table, rows, cp_before(txns, m), after=last)
            chunks += 1
        for txn in txns[applied : applied + rng.randint(0, 2)]:
            engine.apply(txn)
            applied += 1
        if applied == len(txns) and engine.next_page() is None:
            # Everything delivered: the stream reports progress past the end.
            engine.advance(Frontier.at(cp_before(txns, len(txns))))
    return applied


def reference(store_dir: Path, txns: list[Transaction]) -> tuple[list[Delta[Row]], list[int]]:
    """A snapshot-installed engine's output, and its length after each txn."""
    out: list[Delta[Row]] = []
    sd = StateDirectory(store_dir)
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry(out), store)
    install(engine)
    marks = [len(out)]
    for txn in txns:
        engine.apply(txn)
        marks.append(len(out))
    store.close()
    return out, marks


@pytest.mark.parametrize("seed", range(8))
def test_a_chunked_load_goes_live_with_the_snapshot_engines_views(
    tmp_path: Path, seed: int
) -> None:
    txns = workload(seed=seed, n=40)
    before = states(txns)
    expected, marks = reference(tmp_path / "ref", txns)
    rng = random.Random(seed)

    out: list[Delta[Row]] = []
    sd = StateDirectory(tmp_path / "chunked")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry(out), store, chunked=True)
    engine.begin_load([O_SCHEMA, C_SCHEMA], CP)
    applied = load(engine, txns, before, rng, applied=0, page=rng.randint(1, 4))
    assert out == []  # the views have no state, and no output, until live
    engine.go_live()
    assert not engine.loading
    assert accumulate(out) == accumulate(expected[: marks[applied]])
    for txn in txns[applied:]:
        engine.apply(txn)
    assert accumulate(out) == accumulate(expected)
    store.close()


@pytest.mark.parametrize("seed", range(4))
def test_a_load_checkpointed_halfway_resumes_where_it_stopped(tmp_path: Path, seed: int) -> None:
    txns = workload(seed=100 + seed, n=40)
    before = states(txns)
    expected, _marks = reference(tmp_path / "ref", txns)
    rng = random.Random(seed)

    sd = StateDirectory(tmp_path / "chunked")
    store = sd.open_live(memory_budget=8 * MB)
    first = Engine(registry([]), store, chunked=True)
    first.begin_load([O_SCHEMA, C_SCHEMA], CP)
    applied = load(first, txns, before, rng, applied=0, page=2, stop_after_chunks=3)
    assert first.loading
    sd.checkpoint(store, Lsn(first.frontier.lower or 0), first.metadata())
    store.close()  # "crash"

    ck = sd.latest()
    assert ck is not None
    assert "loading" in ck.meta
    out: list[Delta[Row]] = []
    restored = sd.open_live(memory_budget=8 * MB, restore=ck)
    resumed = Engine(registry(out), restored, resume=ck.meta)
    assert resumed.loading
    assert resumed.frontier == Frontier.at(ck.lsn)
    assert resumed.counters["snapshot_chunks"] == 0  # nothing read again yet
    applied = load(resumed, txns, before, rng, applied=applied, page=2)
    resumed.go_live()
    for txn in txns[applied:]:
        resumed.apply(txn)
    assert accumulate(out) == accumulate(expected)
    restored.close()


def test_a_load_cannot_resume_under_changed_views(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path / "s")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry([]), store, chunked=True)
    engine.begin_load([O_SCHEMA, C_SCHEMA], CP)
    meta = engine.metadata()
    store.close()
    changed = registry([])
    changed.view("extra").source("public.orders").sink(Recorder([]))
    assert Engine.cannot_resume(changed, meta) == (
        "the views changed while their tables were being loaded"
    )


def test_a_load_must_name_every_source(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path / "s")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry([]), store, chunked=True)
    with pytest.raises(Exception, match="no schema given"):
        engine.begin_load([O_SCHEMA], CP)
    store.close()


def test_schemas_are_declared_before_any_row(tmp_path: Path) -> None:
    """A left join pads with its right table's columns: known from the start."""
    sd = StateDirectory(tmp_path / "s")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry([]), store, chunked=True)
    engine.begin_load([O_SCHEMA, C_SCHEMA], CP)
    assert engine._columns_of("public.customers") == C_SCHEMA.columns
    assert isinstance(engine.arrangements["public.orders"].schema, TableSchema)
    store.close()


def test_a_checkpoint_of_another_state_format_is_not_resumed(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path / "s")
    store = sd.open_live(memory_budget=8 * MB)
    engine = Engine(registry([]), store)
    install(engine)
    meta = {**engine.metadata(), "version": 2}
    store.close()
    assert Engine.cannot_resume(registry([]), meta) == (
        "the checkpoint's state format is version 2; this one is 1"
    )
