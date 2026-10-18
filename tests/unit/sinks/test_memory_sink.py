from __future__ import annotations

import pytest

from deltaflow.core import Delta, Frontier, FrontierViolationError, Multiset, Row
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks import MemorySink, NegativeMultiplicityError

A, B = Row({"id": 1}), Row({"id": 2})


def at(n: int) -> Frontier:
    return Frontier.at(Lsn(n))


def test_deltas_become_visible_only_once_their_time_is_complete() -> None:
    changes: list[tuple[Frontier, list[Delta[Row]]]] = []
    sink = MemorySink(on_change=lambda f, batch: changes.append((f, batch)))
    sink.accept([Delta(A, Lsn(1), 1), Delta(B, Lsn(5), 1)])
    assert sink.result == Multiset()

    sink.advance(at(2))  # time 1 complete, time 5 not yet
    assert sink.frontier == at(2)
    assert sink.result == Multiset.of([A])
    sink.advance(at(6))
    assert sink.result == Multiset.of([A, B])
    assert changes == [(at(2), [Delta(A, Lsn(1), 1)]), (at(6), [Delta(B, Lsn(5), 1)])]


def test_batches_are_consolidated_and_no_op_advances_are_silent() -> None:
    changes: list[tuple[Frontier, list[Delta[Row]]]] = []
    sink = MemorySink(on_change=lambda f, batch: changes.append((f, batch)))
    sink.accept(
        [Delta(A, Lsn(1), 1), Delta(A, Lsn(1), 1), Delta(B, Lsn(1), 1), Delta(B, Lsn(1), -1)]
    )
    sink.advance(at(2))
    sink.advance(at(3))
    assert sink.result == Multiset.from_pairs([(A, 2)])
    assert sorted(map(dict, sink.rows()), key=str) == [{"id": 1}, {"id": 1}]
    assert changes == [(at(2), [Delta(A, Lsn(1), 2)])]


def test_retracting_a_row_that_is_not_there_raises() -> None:
    sink = MemorySink()
    sink.accept([Delta(A, Lsn(1), -1)])
    with pytest.raises(NegativeMultiplicityError, match=r"Row\(\{'id': 1\}\) has multiplicity -1"):
        sink.advance(at(2))


def test_a_delta_behind_the_frontier_raises() -> None:
    sink = MemorySink()
    sink.advance(at(10))
    with pytest.raises(FrontierViolationError):
        sink.accept([Delta(A, Lsn(9), 1)])
