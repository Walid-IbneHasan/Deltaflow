"""The Postgres sink's buffering, without a database: complete transactions
become net writes per key; replays at or below the watermark are dropped;
a view that is not unique on the table's key is refused."""

from __future__ import annotations

import pytest

from deltaflow.core import Delta, Frontier, FrontierViolationError, Row
from deltaflow.replication.lsn import Lsn
from deltaflow.sinks import PostgresSink, SinkError

COLUMNS = ("country", "n")


def bound(watermark: Lsn | None = None, key: tuple[str, ...] = ("country",)) -> PostgresSink:
    sink = PostgresSink("analytics.per_country")
    sink.bind(view="per_country", columns=COLUMNS, key=key, watermark=watermark)
    return sink


def nl(n: int) -> Row:
    return Row({"country": "NL", "n": n})


def at(n: int) -> Frontier:
    return Frontier.at(Lsn(n))


def test_complete_transactions_become_net_writes_per_key() -> None:
    sink = bound()
    sink.accept([Delta(nl(1), Lsn(1), 1)])
    sink.advance(at(2))
    sink.accept([Delta(nl(1), Lsn(3), -1), Delta(nl(2), Lsn(3), 1)])
    sink.accept([Delta(Row({"country": "DE", "n": 5}), Lsn(5), 1)])  # not complete yet
    sink.advance(at(4))
    writes = sink.take()
    assert writes.inserts == [nl(2)]  # 1 then 2: only the last state is written
    assert writes.deletes == []
    sink.advance(at(6))
    sink.accept([Delta(nl(2), Lsn(7), -1)])
    sink.advance(at(8))
    writes = sink.take()
    assert writes.inserts == [Row({"country": "DE", "n": 5})]
    assert writes.deletes == [("NL",)]  # existed in the table: the earlier flush wrote it


def test_writes_are_classified_against_what_the_table_has() -> None:
    sink = bound()
    # Inserted and deleted within one flush: the table never sees it.
    sink.accept([Delta(nl(1), Lsn(1), 1)])
    sink.accept([Delta(nl(1), Lsn(2), -1)])
    sink.advance(at(3))
    writes = sink.take()
    assert (writes.inserts, writes.updates, writes.deletes) == ([], [], [])
    # A key first seen being retracted existed before this flush.
    sink.accept([Delta(nl(4), Lsn(3), -1), Delta(nl(5), Lsn(3), 1)])
    sink.advance(at(4))
    writes = sink.take()
    assert (writes.inserts, writes.updates, writes.deletes) == ([], [nl(5)], [])
    sink.accept([Delta(nl(5), Lsn(4), -1)])
    sink.accept([Delta(nl(6), Lsn(5), 1)])
    sink.advance(at(6))
    writes = sink.take()  # deleted then re-added in one flush: an update
    assert (writes.inserts, writes.updates, writes.deletes) == ([], [nl(6)], [])


def test_replays_at_or_below_the_watermark_are_dropped() -> None:
    sink = bound(watermark=Lsn(10))
    sink.accept([Delta(nl(9), Lsn(8), 1)])
    sink.advance(at(10))  # the transaction ended at 10: already in the table
    assert sink.take().upserts == []
    sink.accept([Delta(nl(9), Lsn(10), -1), Delta(nl(11), Lsn(10), 1)])
    sink.advance(at(12))  # first transaction past the watermark
    assert sink.take().upserts == [nl(11)]


def test_a_view_that_is_not_unique_on_the_key_is_refused() -> None:
    sink = bound()
    sink.accept([Delta(nl(1), Lsn(1), 1), Delta(nl(2), Lsn(1), 1)])
    with pytest.raises(SinkError, match=r"has two rows with key \('NL',\)"):
        sink.advance(at(2))
    later = bound()
    later.accept([Delta(nl(1), Lsn(1), 1)])
    later.accept([Delta(nl(2), Lsn(2), 1)])  # a later transaction adds a second NL row
    with pytest.raises(SinkError, match="would be a second row"):
        later.advance(at(3))
    dup = bound()
    dup.accept([Delta(nl(1), Lsn(1), 2)])
    with pytest.raises(SinkError, match="multiplicity 2"):
        dup.advance(at(2))


def test_rows_must_have_exactly_the_tables_columns() -> None:
    sink = bound()
    sink.accept([Delta(Row({"country": "NL", "count": 1}), Lsn(1), 1)])
    with pytest.raises(SinkError, match=r"missing \['n'\].*extra \['count'\]"):
        sink.advance(at(2))


def test_a_keyless_table_holds_at_most_one_row() -> None:
    sink = bound(key=())
    sink.accept([Delta(nl(1), Lsn(1), 1)])
    sink.advance(at(2))
    sink.accept([Delta(nl(1), Lsn(3), 1)])  # in the same flush: caught here
    with pytest.raises(SinkError, match="second row"):
        sink.advance(at(4))
    # Across flushes the sink has forgotten what it wrote, so the row is
    # classified as an insert, and the table refuses it at flush (it already
    # has a row): tests/integration/test_postgres_sink.py.
    later = bound(key=())
    later.accept([Delta(nl(1), Lsn(1), 1)])
    later.advance(at(2))
    later.take()
    later.accept([Delta(nl(2), Lsn(3), 1)])
    later.advance(at(4))
    assert later.take().inserts == [nl(2)]


def test_unbound_or_behind_the_frontier() -> None:
    with pytest.raises(SinkError, match="not bound"):
        PostgresSink("analytics.x").accept([])
    with pytest.raises(ValueError, match="schema-qualified"):
        PostgresSink("x")
    sink = bound()
    sink.advance(at(5))
    with pytest.raises(FrontierViolationError):
        sink.accept([Delta(nl(1), Lsn(4), 1)])


def test_binding_again_starts_a_fresh_session() -> None:
    """A runner that restarts in-process (after a dropped connection)
    re-binds its sinks and replays from its checkpoint. Deltas and net
    writes the failed session never flushed must not survive into the new
    one, or the replay would apply them twice; nor may its frontier, or the
    replay would be rejected as behind it."""
    sink = bound()
    sink.accept([Delta(nl(1), Lsn(1), 1)])
    sink.advance(at(2))  # a net write, never taken (the flush failed)
    sink.accept([Delta(nl(2), Lsn(3), 1)])  # pending, incomplete
    sink.bind(view="per_country", columns=COLUMNS, key=("country",), watermark=None)
    assert sink.frontier == at(0)
    sink.accept([Delta(nl(1), Lsn(1), 1)])  # the replay
    sink.advance(at(2))
    writes = sink.take()
    assert writes.inserts == [nl(1)]  # once
