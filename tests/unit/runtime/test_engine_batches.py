"""Consecutive transactions applied as one step (Engine.apply_batch) leave
every view exactly as applying them one by one does, at every batch
boundary: the only states a sink can ever see."""

from __future__ import annotations

import random
import uuid

import pytest

from deltaflow.core import Delta, Row, accumulate
from deltaflow.runtime import Engine
from deltaflow.state.store import StateStore
from tests.unit.runtime.test_engine_resume import install, registry, workload


@pytest.mark.parametrize("seed", range(6))
def test_batches_leave_the_views_as_single_transactions_do(
    state_store: StateStore, seed: int
) -> None:
    txns = workload(seed=seed, n=60)
    single: list[Delta[Row]] = []
    a = Engine(registry(single), state_store, namespace=uuid.uuid4().hex)
    install(a)
    after: list[int] = []  # len(single) after each transaction
    for txn in txns:
        a.apply(txn)
        after.append(len(single))

    batched: list[Delta[Row]] = []
    b = Engine(registry(batched), state_store, namespace=uuid.uuid4().hex)
    install(b)
    rng = random.Random(seed)
    i = 0
    while i < len(txns):
        n = rng.randint(1, 12)
        b.apply_batch(txns[i : i + n])
        i = min(i + n, len(txns))
        assert accumulate(batched) == accumulate(single[: after[i - 1]]), i
        assert b.frontier == a.frontier or b.frontier.lower == txns[i - 1].end_lsn
    assert accumulate(batched) == accumulate(single)
    # Fewer deltas: changes to the same key cancel within a batch.
    assert len(batched) <= len(single)
