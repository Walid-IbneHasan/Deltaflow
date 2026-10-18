"""Dataflow operators. M2: sources, map and filter. Joins (M3) and
aggregates (M4) do not exist yet; the planner has no way to ask for them."""

from deltaflow.operators.base import Operator, Source
from deltaflow.operators.linear import Filter, Map, MapFunction, Predicate

__all__ = ["Filter", "Map", "MapFunction", "Operator", "Predicate", "Source"]
