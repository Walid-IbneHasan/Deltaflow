from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """These need a fresh Postgres 17 primary and standby, which they then
    break (one is promoted): `make failover` creates the pair, runs them and
    removes it. They are skipped otherwise, saying so."""
    if os.environ.get("DELTAFLOW_FAILOVER") == "1":
        return
    skip = pytest.mark.skip(reason="needs the failover pair: run `make failover`")
    for item in items:
        if "failover" in item.path.parts:
            item.add_marker(skip)
