"""Registered views -> operator graph."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from deltaflow.operators import Distinct, Filter, GroupBy, Join, Map, Operator, Source
from deltaflow.planner.dsl import DistinctStep, FilterStep, GroupStep, JoinStep, MapStep, View
from deltaflow.state.store import StateStore


@dataclass(frozen=True, slots=True)
class Node:
    """Description of one operator, for monitoring (the console's graph)."""

    id: str
    kind: str
    label: str
    inputs: tuple[str, ...]
    views: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Plan:
    #: one Source per table, shared by every view that reads it
    sources: dict[str, Source]
    #: every operator, each after all of its inputs
    operators: tuple[Operator, ...]
    #: each view's final operator, whose output goes to the view's sink
    outputs: tuple[tuple[View, Operator], ...]
    #: id(operator) -> its description
    nodes: dict[int, Node]


def compile_views(views: Sequence[View], store: StateStore, namespace: str = "") -> Plan:
    """Operators are listed after their inputs. A join's right-hand source is
    created where the join first needs it, which keeps that order."""
    sources: dict[str, Source] = {}
    operators: list[Operator] = []
    outputs: list[tuple[View, Operator]] = []
    nodes: dict[int, Node] = {}

    def describe(op: Operator, node_id: str, kind: str, label: str, view: str) -> None:
        inputs = tuple(nodes[id(i)].id for i in op.inputs)
        nodes[id(op)] = Node(node_id, kind, label, inputs, (view,))

    def source(table: str, view: str) -> Source:
        if table not in sources:
            sources[table] = Source(table)
            operators.append(sources[table])
            describe(sources[table], f"source:{table}", "source", table, view)
        else:
            node = nodes[id(sources[table])]
            if view not in node.views:
                nodes[id(sources[table])] = Node(
                    node.id, node.kind, node.label, node.inputs, (*node.views, view)
                )
        return sources[table]

    for view in views:
        node: Operator = source(view.source, view.name)
        for i, step in enumerate(view.steps):
            kind, label = _describe(step)
            match step:
                case FilterStep():
                    node = Filter(node, step.predicate)
                case MapStep():
                    node = Map(node, step.fn)
                case JoinStep():
                    node = Join(
                        node,
                        source(step.table, view.name),
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
            describe(node, f"{view.name}/{i}", kind, label, view.name)
        outputs.append((view, node))
    return Plan(sources, tuple(operators), tuple(outputs), nodes)


def _describe(step: object) -> tuple[str, str]:
    def fn(f: object) -> str:
        return str(getattr(f, "__qualname__", repr(f)))

    match step:
        case FilterStep():
            return "filter", fn(step.predicate)
        case MapStep():
            return "map", fn(step.fn)
        case JoinStep():
            on = ", ".join(f"{a} = {b}" for a, b in zip(step.left_on, step.right_on, strict=True))
            return "join", f"{step.table} on {on}"
        case GroupStep():
            aggs = ", ".join(f"{k}={type(v).__name__}" for k, v in step.aggregates.items())
            return "group_by", f"{', '.join(step.keys) or '(all)'}: {aggs}"
        case _:
            return "distinct", "distinct"
