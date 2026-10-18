"""Merging per-shard partial results into the view's result.

Each shard maintains its view over the rows it owns. How partial results
combine depends on the view's last step:

- no aggregation (filter/map/join): the partials are disjoint, since a row
  belongs to its shard key's shard; merged = their union.
- distinct: merged = the distinct rows of the union.
- group_by / agg: per group, Count and Sum are summed, Min is the min and
  Max the max of the partials.

All three are exact. Count and Sum add up. The minimum of the shards'
minima is the minimum, and the same for maxima. A NULL partial (a shard
whose part of a group had no non-NULL values) is skipped, as SQL skips NULL.

`merge_partials` does this in Python, for tests. `merge_view_sql` writes it
as the Postgres view that sits on top of a sharded sink's partial table.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from psycopg import sql

from deltaflow.core import Multiset, Row
from deltaflow.operators import Count, Max, Min, Sum
from deltaflow.planner.dsl import DistinctStep, GroupStep, View

SHARD_COLUMN = "shard"


def _last(view: View) -> object:
    return view.steps[-1] if view.steps else None


def merge_partials(view: View, partials: Iterable[Multiset[Row]]) -> Multiset[Row]:
    last = _last(view)
    merged: Multiset[Row] = Multiset()
    parts = list(partials)
    if isinstance(last, DistinctStep):
        for part in parts:
            for row, _ in part.items():
                if not merged.count(row):
                    merged.add(row)
        return merged
    if not isinstance(last, GroupStep):
        for part in parts:
            merged = merged + part
        return merged
    keys = last.output_keys
    groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for part in parts:
        for row, n in part.items():
            assert n == 1, f"a partial aggregate row has multiplicity {n}"
            key = tuple(row[k] for k in keys)
            acc = groups.get(key)
            if acc is None:
                groups[key] = dict(row)
                continue
            for name, agg in last.aggregates.items():
                acc[name] = _combine(agg, acc[name], row[name])
    for acc in groups.values():
        merged.add(Row(acc))
    return merged


def _combine(agg: object, a: Any, b: Any) -> Any:
    if isinstance(agg, Count):
        return a + b
    if a is None:
        return b
    if b is None:
        return a
    if isinstance(agg, Sum):
        return a + b
    if isinstance(agg, Min):
        return min(a, b)
    assert isinstance(agg, Max)
    return max(a, b)


def merge_view_sql(
    view: View, table: str, partial_table: str, columns: Sequence[str]
) -> sql.Composed:
    """CREATE OR REPLACE VIEW `table` over `partial_table` (which has
    `columns` plus the shard column)."""
    target = sql.Identifier(*table.split(".", 1))
    source = sql.Identifier(*partial_table.split(".", 1))
    last = _last(view)
    if isinstance(last, GroupStep):
        keys = list(last.output_keys)
        exprs: list[sql.Composable] = []
        for c in columns:
            col = sql.Identifier(c)
            agg = last.aggregates.get(c)
            if c in keys:
                exprs.append(col)
            elif isinstance(agg, Count):
                exprs.append(sql.SQL("sum({0})::bigint AS {0}").format(col))
            elif isinstance(agg, Sum):
                exprs.append(sql.SQL("sum({0}) AS {0}").format(col))
            elif isinstance(agg, Min):
                exprs.append(sql.SQL("min({0}) AS {0}").format(col))
            elif isinstance(agg, Max):
                exprs.append(sql.SQL("max({0}) AS {0}").format(col))
            else:
                raise ValueError(
                    f"column {c!r} of {partial_table} is neither a key nor an aggregate"
                )
        group = (
            sql.SQL(" GROUP BY {}").format(sql.SQL(", ").join(map(sql.Identifier, keys)))
            if keys
            else sql.SQL("")
        )
        body = sql.SQL("SELECT {} FROM {}{}").format(sql.SQL(", ").join(exprs), source, group)
    else:
        distinct = sql.SQL("DISTINCT ") if isinstance(last, DistinctStep) else sql.SQL("")
        body = sql.SQL("SELECT {}{} FROM {}").format(
            distinct, sql.SQL(", ").join(map(sql.Identifier, columns)), source
        )
    return sql.SQL("CREATE OR REPLACE VIEW {} AS {}").format(target, body)
