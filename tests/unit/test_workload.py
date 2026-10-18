"""The randomized workload is deterministic and keeps an exact model of the tables."""

from __future__ import annotations

import zlib
from collections import Counter

from deltaflow.devtools.workload import (
    TOAST_VALUE_MIN_LEN,
    OpKind,
    TableState,
    Transaction,
    Workload,
    initial_state,
)


def _run(
    seed: int, txns: int, state: TableState | None = None
) -> tuple[Workload, list[Transaction]]:
    workload = Workload(
        seed, state if state is not None else initial_state(seed=7, customers=50, orders=300)
    )
    return workload, [workload.next_transaction() for _ in range(txns)]


def test_initial_state_has_requested_sizes_and_valid_foreign_keys() -> None:
    state = initial_state(seed=1, customers=40, orders=250)
    assert len(state.customers) == 40
    assert len(state.orders) == 250
    assert all(o.customer_id in state.customers for o in state.orders.values())


def test_initial_state_is_deterministic() -> None:
    assert initial_state(seed=3, customers=20, orders=100) == initial_state(
        seed=3, customers=20, orders=100
    )


def test_same_seed_and_start_state_give_identical_transactions() -> None:
    _, first = _run(seed=11, txns=300)
    _, second = _run(seed=11, txns=300)
    assert first == second


def test_different_seeds_diverge() -> None:
    _, first = _run(seed=11, txns=50)
    _, second = _run(seed=12, txns=50)
    assert first != second


def test_rolled_back_transaction_leaves_model_unchanged() -> None:
    workload = Workload(5, initial_state(seed=2, customers=30, orders=100))
    for _ in range(500):
        before = workload.state.copy()
        txn = workload.next_transaction()
        if not txn.commit:
            assert workload.state == before
            return
    raise AssertionError("no rolled-back transaction in 500 attempts")


def test_committed_transaction_changes_model() -> None:
    workload = Workload(5, initial_state(seed=2, customers=30, orders=100))
    before = workload.state.copy()
    txn = workload.next_transaction()
    while not txn.commit:
        txn = workload.next_transaction()
    assert workload.state != before


def test_foreign_keys_hold_after_every_transaction() -> None:
    workload = Workload(21, initial_state(seed=4, customers=10, orders=60))
    for _ in range(2000):
        workload.next_transaction()
        state = workload.state
        assert all(o.customer_id in state.customers for o in state.orders.values())


def test_tables_can_drain_to_empty_and_recover() -> None:
    # With nothing to update or delete the generator must still make progress.
    workload, txns = _run(seed=9, txns=200, state=TableState(customers={}, orders={}))
    assert txns
    assert workload.state.customers


def test_ids_are_never_reused() -> None:
    workload = Workload(13, initial_state(seed=4, customers=10, orders=60))
    seen_customers = set(workload.state.customers)
    seen_orders = set(workload.state.orders)
    for _ in range(1000):
        before_c = set(workload.state.customers)
        before_o = set(workload.state.orders)
        workload.next_transaction()
        new_c = set(workload.state.customers) - before_c
        new_o = set(workload.state.orders) - before_o
        assert not new_c & seen_customers
        assert not new_o & seen_orders
        seen_customers |= new_c
        seen_orders |= new_o


def test_workload_covers_every_edge_case_the_replication_reader_must_handle() -> None:
    _, txns = _run(seed=17, txns=3000)
    kinds = Counter(s.kind for t in txns for s in t.statements)
    assert set(kinds) == set(OpKind), f"missing kinds: {set(OpKind) - set(kinds)}"
    assert any(not t.commit for t in txns), "no rollbacks"
    assert any(len(t.statements) > 1 for t in txns), "no multi-statement transactions"

    params = [p for t in txns for s in t.statements for p in s.params]
    assert None in params, "no NULLs written"
    assert any(isinstance(p, str) and len(p) >= TOAST_VALUE_MIN_LEN for p in params), (
        "no TOAST-sized values written"
    )

    # An UPDATE that does not set `bio` is what makes pgoutput send the
    # unchanged-TOAST placeholder for it.
    customer_updates = [
        s.sql for t in txns for s in t.statements if s.kind is OpKind.UPDATE_CUSTOMER
    ]
    assert any("bio" not in sql for sql in customer_updates)
    assert any("bio" in sql for sql in customer_updates)


def test_toast_values_are_incompressible() -> None:
    # pglz must not be able to shrink the value below the out-of-line
    # threshold, otherwise it is stored inline and TOAST is never exercised.
    _, txns = _run(seed=17, txns=2000)
    big = next(
        p
        for t in txns
        for s in t.statements
        for p in s.params
        if isinstance(p, str) and len(p) >= TOAST_VALUE_MIN_LEN
    )
    assert len(zlib.compress(big.encode(), 9)) > 0.6 * len(big)
