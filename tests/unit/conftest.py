from __future__ import annotations

from collections.abc import Iterator

import pytest

from deltaflow.state.store import StateStore


@pytest.fixture(scope="session")
def state_store(tmp_path_factory: pytest.TempPathFactory) -> Iterator[StateStore]:
    """One real RocksDB store for the unit suite. Session-scoped so property
    tests can use it (Hypothesis runs many examples per test). Tests isolate
    themselves by using uniquely named keyspaces and operators."""
    with StateStore(tmp_path_factory.mktemp("state"), memory_budget=16 << 20) as store:
        yield store
