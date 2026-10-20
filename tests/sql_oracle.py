"""The recompute side of differential comparisons: run SQL, get a multiset of
rows typed exactly as the engine types them."""

from __future__ import annotations

from typing import Any

import psycopg

from deltaflow.core import Multiset, Row
from deltaflow.core.values import to_array
from deltaflow.replication.changes import register_value_loaders


async def query_multiset(
    conn: psycopg.AsyncConnection[tuple[Any, ...]], query: str
) -> Multiset[Row]:
    register_value_loaders(conn.adapters)  # json, jsonb and arrays as the engine has them
    async with conn.cursor(binary=True) as cur:
        await cur.execute(query)
        assert cur.description is not None
        names = [c.name for c in cur.description]
        return Multiset.of(
            Row(zip(names, map(to_array, rec), strict=True)) for rec in await cur.fetchall()
        )


def first_difference(view: Multiset[Row], expected: Multiset[Row]) -> str:
    """A readable account of how two multisets differ."""
    diff = view - expected
    extra = [(r, n) for r, n in diff.items() if n > 0]
    missing = [(r, -n) for r, n in diff.items() if n < 0]
    return (
        f"view has {view.size()} rows, SQL has {expected.size()}; "
        f"{len(extra)} extra in view e.g. {extra[:2]!r:.400}; "
        f"{len(missing)} missing from view e.g. {missing[:2]!r:.400}"
    )
