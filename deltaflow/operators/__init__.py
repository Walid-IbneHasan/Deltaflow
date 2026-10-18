"""Dataflow operators: sources, map and filter (M2), inner equi-join (M3).
Aggregates (M4) do not exist yet; the planner has no way to ask for them."""

from deltaflow.operators.base import Operator, Source
from deltaflow.operators.join import Join, JoinStateError
from deltaflow.operators.linear import Filter, Map, MapFunction, Predicate

__all__ = [
    "Filter",
    "Join",
    "JoinStateError",
    "Map",
    "MapFunction",
    "Operator",
    "Predicate",
    "Source",
]
