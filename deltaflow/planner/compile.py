"""Registered views -> operator graph."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from deltaflow.operators import Filter, Map, Operator, Source
from deltaflow.planner.dsl import FilterStep, MapStep, View


@dataclass(frozen=True, slots=True)
class Plan:
    #: one Source per table, shared by every view that reads it
    sources: dict[str, Source]
    #: every operator, each after all of its inputs
    operators: tuple[Operator, ...]
    #: each view's final operator, whose output goes to the view's sink
    outputs: tuple[tuple[View, Operator], ...]


def compile_views(views: Sequence[View]) -> Plan:
    sources: dict[str, Source] = {}
    operators: list[Operator] = []
    outputs: list[tuple[View, Operator]] = []
    for view in views:
        if view.source not in sources:
            sources[view.source] = Source(view.source)
            operators.append(sources[view.source])
        node: Operator = sources[view.source]
        for step in view.steps:
            match step:
                case FilterStep():
                    node = Filter(node, step.predicate)
                case MapStep():
                    node = Map(node, step.fn)
            operators.append(node)
        outputs.append((view, node))
    return Plan(sources, tuple(operators), tuple(outputs))
