"""The view DSL.

    df = Deltaflow()
    df.view("big_orders").source("public.orders").filter(lambda r: r["amount"] > 100).sink(sink)

Registration checks everything that can be checked without a database:
shape, names and callables. Source tables are checked against the catalog
when the engine starts, before any change is consumed (runtime.snapshot).

The DSL only offers operators that exist and are maintained incrementally:
source, filter, map, select (M2), inner join (M3), and group_by/agg
and distinct (M4).

After a join, columns are qualified by table name (`orders.amount`,
`customers.country`). An unqualified name still works wherever it is
unambiguous (see `core.Row`).
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from deltaflow.core import Row
from deltaflow.operators import Aggregate, Count, MapFunction, Max, Min, Predicate, Sum
from deltaflow.sinks import Sink

_VIEW_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")
_TABLE_NAME = re.compile(r"[^.\s]+\.[^.\s]+")


class PlanError(ValueError):
    """A view cannot be registered as declared."""


@dataclass(frozen=True, slots=True)
class FilterStep:
    predicate: Predicate


@dataclass(frozen=True, slots=True)
class MapStep:
    fn: MapFunction


@dataclass(frozen=True, slots=True)
class JoinStep:
    table: str
    left_on: tuple[str, ...]
    right_on: tuple[str, ...]
    #: None when the left rows are already qualified by an earlier join
    left_prefix: str | None
    right_prefix: str


@dataclass(frozen=True, slots=True)
class GroupStep:
    #: empty for a global aggregate (exactly one output row, as in SQL)
    keys: tuple[str, ...]
    aggregates: dict[str, Aggregate]
    #: output column name of each key (defaults to the key itself)
    names: tuple[str, ...] = ()

    @property
    def output_keys(self) -> tuple[str, ...]:
        return self.names or self.keys


@dataclass(frozen=True, slots=True)
class DistinctStep:
    pass


type Step = FilterStep | MapStep | JoinStep | GroupStep | DistinctStep


@dataclass(frozen=True, slots=True)
class View:
    name: str
    source: str
    steps: tuple[Step, ...]
    sink: Sink

    @property
    def sources(self) -> tuple[str, ...]:
        joined = (s.table for s in self.steps if isinstance(s, JoinStep))
        return (self.source, *joined)


class Deltaflow:
    """Registry of views. Frozen once an engine is built from it.

    With `shards` and `shard_by`, every view is checked to be exactly
    computable shard by shard (M7, docs/scale-out.md). `shard_by` names, per
    source table, the column whose value decides a row's shard."""

    def __init__(
        self, *, shards: int | None = None, shard_by: Mapping[str, str] | None = None
    ) -> None:
        if (shards is None) != (shard_by is None):
            raise PlanError("give both shards and shard_by, or neither")
        if shards is not None and shards < 1:
            raise PlanError("shards must be at least 1")
        self.shards = shards
        self.shard_by: dict[str, str] = dict(shard_by or {})
        self._views: dict[str, View] = {}
        self._frozen = False

    @property
    def views(self) -> tuple[View, ...]:
        return tuple(self._views.values())

    @property
    def sources(self) -> tuple[str, ...]:
        """Distinct source tables, in order of first use."""
        return tuple(dict.fromkeys(t for v in self._views.values() for t in v.sources))

    def view(self, name: str) -> ViewBuilder:
        self._check_open()
        if not _VIEW_NAME.fullmatch(name):
            raise PlanError(f"invalid view name {name!r}: use [A-Za-z_][A-Za-z0-9_]*")
        if name in self._views:
            raise PlanError(f"view {name!r} is already registered")
        return ViewBuilder(self, name)

    def freeze(self) -> None:
        self._frozen = True

    def _check_open(self) -> None:
        if self._frozen:
            raise PlanError("this Deltaflow is frozen: an engine was built from it")

    def _register(self, view: View) -> View:
        self._check_open()
        if view.name in self._views:
            raise PlanError(f"view {view.name!r} is already registered")
        for other in self._views.values():
            if other.sink is view.sink:
                raise PlanError(f"that sink is already the sink of view {other.name!r}")
        self._views[view.name] = view
        return view


class ViewBuilder:
    def __init__(self, registry: Deltaflow, name: str) -> None:
        self._registry = registry
        self._name = name
        self._source: str | None = None
        self._steps: list[Step] = []
        self._prefixes: list[str] = []
        self._aggregated = False
        self._mapped = False
        self._done = False

    def source(self, table: str) -> ViewBuilder:
        self._check_usable()
        if self._source is not None:
            raise PlanError(f"view {self._name!r} already has a source ({self._source})")
        if not _TABLE_NAME.fullmatch(table):
            raise PlanError(
                f"source {table!r} must be schema-qualified, e.g. 'public.orders' "
                "(an unqualified name would depend on search_path)"
            )
        if self._registry.shards and table not in self._registry.shard_by:
            raise PlanError(
                f"{table} has no shard column in shard_by; a sharded view cannot read it"
            )
        self._source = table
        self._prefixes = [_relname(table)]
        return self

    def filter(self, predicate: Predicate) -> ViewBuilder:
        self._check_step(predicate, "filter")
        self._check_not_terminal("filter")
        self._steps.append(FilterStep(predicate))
        return self

    def map(self, fn: MapFunction) -> ViewBuilder:
        self._check_step(fn, "map")
        self._check_not_terminal("map")
        self._mapped = True
        self._steps.append(MapStep(fn))
        return self

    def select(self, *columns: str) -> ViewBuilder:
        if not columns:
            raise PlanError("select needs at least one column")
        return self.map(_Select(columns))

    def join(
        self,
        table: str,
        *,
        on: tuple[str, str] | Sequence[tuple[str, str]],
        how: str = "inner",
    ) -> ViewBuilder:
        """Inner equi-join with source table `table`: `on=("customer_id", "id")`
        or a list of such pairs for a composite key (left column, right column)."""
        self._check_usable()
        if self._source is None:
            raise PlanError(f"view {self._name!r}: call .source(table) before .join()")
        if how != "inner":
            raise PlanError(
                f"only inner joins are maintained incrementally so far; "
                f"how={how!r} is not supported"
            )
        if not _TABLE_NAME.fullmatch(table):
            raise PlanError(
                f"join table {table!r} must be schema-qualified, e.g. 'public.customers'"
            )
        if self._aggregated:
            raise PlanError(
                "a join after an aggregation (group_by, agg, distinct) is not supported"
            )
        pairs = _pairs(on)
        if self._registry.shards:
            self._check_co_partitioned(table, pairs)
        prefix = _relname(table)
        if prefix in self._prefixes:
            raise PlanError(
                f"join with {table}: a table with the same name ({prefix}) is already in this "
                "view; self-joins and same-named tables are not supported yet"
            )
        left_prefix = self._prefixes[0] if len(self._prefixes) == 1 else None
        self._steps.append(
            JoinStep(
                table,
                tuple(left for left, _ in pairs),
                tuple(right for _, right in pairs),
                left_prefix,
                prefix,
            )
        )
        self._prefixes.append(prefix)
        return self

    def group_by(self, *columns: str, **aliases: str) -> GroupedBuilder:
        """`.group_by("customers.country").agg(n=Count(), revenue=Sum("amount"))`.
        Output rows hold the group columns (named as given, or as aliased:
        `group_by(country="customers.country")`) and the aggregates."""
        self._check_usable()
        self._check_not_terminal("group_by")
        if self._source is None:
            raise PlanError(f"view {self._name!r}: call .source(table) before .group_by()")
        keys = (*columns, *aliases.values())
        names = (*columns, *aliases)
        if not keys or not all(isinstance(c, str) and c for c in keys):
            raise PlanError(
                "group_by needs at least one column name (use .agg() for a global aggregate)"
            )
        if len(set(keys)) != len(keys) or len(set(names)) != len(names):
            raise PlanError(f"group_by lists a column more than once: {keys!r}")
        return GroupedBuilder(self, tuple(keys), tuple(names))

    def agg(self, **aggregates: Aggregate) -> ViewBuilder:
        """A global aggregate over all rows: one output row, even over none."""
        if not aggregates:
            raise PlanError("agg needs at least one aggregate, e.g. agg(n=Count())")
        return self._aggregate((), aggregates)

    def distinct(self) -> ViewBuilder:
        self._check_usable()
        self._check_not_terminal("distinct")
        if self._source is None:
            raise PlanError(f"view {self._name!r}: call .source(table) before .distinct()")
        self._steps.append(DistinctStep())
        self._aggregated = True
        return self

    def _aggregate(
        self,
        keys: tuple[str, ...],
        aggregates: dict[str, Aggregate],
        names: tuple[str, ...] = (),
    ) -> ViewBuilder:
        self._check_usable()
        self._check_not_terminal("agg")
        if self._source is None:
            raise PlanError(f"view {self._name!r}: call .source(table) before .agg()")
        for name, agg in aggregates.items():
            if not isinstance(agg, Count | Sum | Min | Max):
                raise PlanError(
                    f"aggregate {name}={agg!r}: expected Count, Sum, Min or Max "
                    "(other aggregates are not maintained incrementally yet)"
                )
            if name in (names or keys):
                raise PlanError(f"aggregate name {name!r} collides with a group_by column")
        self._steps.append(GroupStep(keys, dict(aggregates), names if names != keys else ()))
        self._aggregated = True
        return self

    def sink(self, sink: Sink) -> View:
        self._check_usable()
        if self._source is None:
            raise PlanError(f"view {self._name!r}: call .source(table) first")
        view = View(self._name, self._source, tuple(self._steps), sink)
        self._done = True
        return self._registry._register(view)

    def _check_step(self, fn: object, what: str) -> None:
        self._check_usable()
        if self._source is None:
            raise PlanError(f"view {self._name!r}: call .source(table) before .{what}()")
        if not callable(fn):
            raise PlanError(f"{what} argument must be callable, got {type(fn).__name__}")
        if inspect.iscoroutinefunction(fn):
            raise PlanError(f"{what} function must be synchronous, not async")

    def _check_not_terminal(self, what: str) -> None:
        """In a sharded registry, shards compute partial aggregates that a
        merge view combines; that merge knows the aggregate columns only if
        nothing transforms them afterwards."""
        if self._registry.shards and self._aggregated:
            raise PlanError(
                f"in a sharded view an aggregation (group_by, agg, distinct) must be the last "
                f"step; .{what}() after it is not supported"
            )

    def _check_co_partitioned(self, table: str, pairs: list[tuple[str, str]]) -> None:
        """A join is computed shard by shard only if both sides are sharded by
        their join keys: then every matching pair lives on the same shard."""
        assert self._source is not None
        if table not in self._registry.shard_by:
            raise PlanError(
                f"{table} has no shard column in shard_by; a sharded view cannot read it"
            )
        if len(self._prefixes) > 1:
            raise PlanError("a sharded view may contain at most one join")
        if self._mapped:
            raise PlanError(
                "in a sharded view a map before a join could change the join key, so rows "
                "would no longer meet on the same shard; filter or select after the join"
            )
        left, right = self._registry.shard_by[self._source], self._registry.shard_by[table]
        if pairs != [(left, right)]:
            raise PlanError(
                f"join on {pairs} is not co-partitioned: sharded views can only join "
                f"{self._source}.{left} = {table}.{right}, the two tables' shard columns"
            )

    def _check_usable(self) -> None:
        if self._done:
            raise PlanError(f"view {self._name!r} is already registered; start a new .view()")


class GroupedBuilder:
    """Returned by group_by: say which aggregates to compute."""

    def __init__(self, builder: ViewBuilder, keys: tuple[str, ...], names: tuple[str, ...]) -> None:
        self._builder = builder
        self._keys = keys
        self._names = names

    def agg(self, **aggregates: Aggregate) -> ViewBuilder:
        """Zero aggregates is allowed: the distinct groups themselves."""
        return self._builder._aggregate(self._keys, aggregates, self._names)


def _relname(table: str) -> str:
    return table.split(".", 1)[1]


def _pairs(on: object) -> list[tuple[str, str]]:
    candidates = [on] if isinstance(on, tuple) else on
    if isinstance(candidates, list | tuple) and candidates:
        pairs = list(candidates)
        if all(
            isinstance(p, tuple) and len(p) == 2 and all(isinstance(c, str) and c for c in p)
            for p in pairs
        ):
            return pairs
    raise PlanError(
        f"join on={on!r}: expected pairs of column names, e.g. ('customer_id', 'id') "
        "or [('a', 'x'), ('b', 'y')]"
    )


class _Select:
    """Projection onto named columns (a map with a readable repr)."""

    def __init__(self, columns: tuple[str, ...]) -> None:
        self.columns = columns
        self.__qualname__ = f"select({', '.join(columns)})"

    def __call__(self, row: Row) -> Mapping[str, Any]:
        return {c: row[c] for c in self.columns}
