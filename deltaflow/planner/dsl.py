"""The view DSL.

    df = Deltaflow()
    df.view("big_orders").source("public.orders").filter(lambda r: r["amount"] > 100).sink(sink)

Registration checks everything that can be checked without a database:
shape, names and callables. Source tables are checked against the catalog
when the engine starts, before any change is consumed (runtime.snapshot).

The DSL only offers operators that exist and are maintained incrementally.
In M2 that is source, filter, map and select. There is no `join` or
`group_by` method that raises "not implemented"; they appear with the
milestones that implement them.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from deltaflow.core import Row
from deltaflow.operators import MapFunction, Predicate
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


type Step = FilterStep | MapStep


@dataclass(frozen=True, slots=True)
class View:
    name: str
    source: str
    steps: tuple[Step, ...]
    sink: Sink


class Deltaflow:
    """Registry of views. Frozen once an engine is built from it."""

    def __init__(self) -> None:
        self._views: dict[str, View] = {}
        self._frozen = False

    @property
    def views(self) -> tuple[View, ...]:
        return tuple(self._views.values())

    @property
    def sources(self) -> tuple[str, ...]:
        """Distinct source tables, in order of first use."""
        return tuple(dict.fromkeys(v.source for v in self._views.values()))

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
        self._source = table
        return self

    def filter(self, predicate: Predicate) -> ViewBuilder:
        self._check_step(predicate, "filter")
        self._steps.append(FilterStep(predicate))
        return self

    def map(self, fn: MapFunction) -> ViewBuilder:
        self._check_step(fn, "map")
        self._steps.append(MapStep(fn))
        return self

    def select(self, *columns: str) -> ViewBuilder:
        if not columns:
            raise PlanError("select needs at least one column")
        return self.map(_Select(columns))

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

    def _check_usable(self) -> None:
        if self._done:
            raise PlanError(f"view {self._name!r} is already registered; start a new .view()")


class _Select:
    """Projection onto named columns (a map with a readable repr)."""

    def __init__(self, columns: tuple[str, ...]) -> None:
        self.columns = columns
        self.__qualname__ = f"select({', '.join(columns)})"

    def __call__(self, row: Row) -> Mapping[str, Any]:
        return {c: row[c] for c in self.columns}
