"""Registered views -> operator graph."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from deltaflow.operators import Distinct, Filter, GroupBy, Join, Map, Operator, Source
from deltaflow.planner.dsl import DistinctStep, FilterStep, GroupStep, JoinStep, MapStep, View
from deltaflow.state.store import StateStore


@dataclass(frozen=True, slots=True)
class Plan:
    #: one Source per table, shared by every view that reads it
    sources: dict[str, Source]
    #: every operator, each after all of its inputs
    operators: tuple[Operator, ...]
    #: each view's final operator, whose output goes to the view's sink
    outputs: tuple[tuple[View, Operator], ...]


def compile_views(views: Sequence[View], store: StateStore, namespace: str = "") -> Plan:
    """Operators are listed after their inputs. A join's right-hand source is
    created where the join first needs it, which keeps that order."""
    sources: dict[str, Source] = {}
    operators: list[Operator] = []
    outputs: list[tuple[View, Operator]] = []

    def source(table: str) -> Source:
        if table not in sources:
            sources[table] = Source(table)
            operators.append(sources[table])
        return sources[table]

    for view in views:
        node: Operator = source(view.source)
        for i, step in enumerate(view.steps):
            match step:
                case FilterStep():
                    node = Filter(node, step.predicate)
                case MapStep():
                    node = Map(node, step.fn)
                case JoinStep():
                    node = Join(
                        node,
                        source(step.table),
                        left_on=step.left_on,
                        right_on=step.right_on,
                        left_prefix=step.left_prefix,
                        right_prefix=step.right_prefix,
                        store=store,
                        name=f"{namespace}{view.name}.{i}",
                    )
                case GroupStep():
                    node = GroupBy(
                        node,
                        keys=step.keys,
                        aggregates=step.aggregates,
                        store=store,
                        name=f"{namespace}{view.name}.{i}",
                    )
                case DistinctStep():
                    node = Distinct(node, store=store, name=f"{namespace}{view.name}.{i}")
            operators.append(node)
        outputs.append((view, node))
    return Plan(sources, tuple(operators), tuple(outputs))
