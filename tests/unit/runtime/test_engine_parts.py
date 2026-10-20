"""A large transaction applied in parts (TransactionPart, then its final
Transaction) produces exactly what it produces applied whole, and nothing
of it is complete before its end."""

from __future__ import annotations

import random
import uuid
from dataclasses import replace

import pytest

from deltaflow.core import Delta, Row, accumulate
from deltaflow.replication.changes import Transaction, TransactionPart
from deltaflow.runtime import Engine
from deltaflow.state.store import StateStore
from tests.unit.runtime.test_engine_resume import install, registry, workload


def split(txn: Transaction, rng: random.Random) -> tuple[list[TransactionPart], Transaction]:
    """The transaction as the assembler hands it over with a small limit."""
    changes = list(txn.changes)
    parts = []
    while len(changes) > 1 and rng.random() < 0.7:
        n = rng.randint(1, len(changes) - 1)
        parts.append(TransactionPart(txn.xid, txn.commit_lsn, tuple(changes[:n])))
        changes = changes[n:]
    return parts, replace(txn, changes=tuple(changes), continued=bool(parts))


@pytest.mark.parametrize("seed", range(6))
def test_parts_produce_what_the_whole_transaction_does(state_store: StateStore, seed: int) -> None:
    txns = workload(seed=seed, n=40)
    whole: list[Delta[Row]] = []
    a = Engine(registry(whole), state_store, namespace=uuid.uuid4().hex)
    install(a)
    for txn in txns:
        a.apply(txn)

    parted: list[Delta[Row]] = []
    b = Engine(registry(parted), state_store, namespace=uuid.uuid4().hex)
    install(b)
    rng = random.Random(seed)
    for txn in txns:
        parts, end = split(txn, rng)
        for part in parts:
            b.apply_part(part)
            assert b.partial
            assert b.frontier.admits(txn.commit_lsn)  # nothing of it complete yet
        b.apply(end)
        assert not b.partial
    assert accumulate(parted) == accumulate(whole)


def test_mid_transaction_the_state_cannot_be_checkpointed(state_store: StateStore) -> None:
    [txn] = workload(seed=1, n=1)
    engine = Engine(registry([]), state_store, namespace=uuid.uuid4().hex)
    install(engine)
    engine.apply_part(TransactionPart(txn.xid, txn.commit_lsn, txn.changes[:1]))
    with pytest.raises(RuntimeError, match="applied in part"):
        engine.metadata()


def test_an_end_without_its_parts_is_refused(state_store: StateStore) -> None:
    [txn] = workload(seed=2, n=1)
    engine = Engine(registry([]), state_store, namespace=uuid.uuid4().hex)
    install(engine)
    with pytest.raises(RuntimeError, match="parts and its end do not match"):
        engine.apply(replace(txn, continued=True))
