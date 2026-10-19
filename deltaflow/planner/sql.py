"""Views written in SQL, translated into the dataflow plan.

    df.sql('''
        CREATE VIEW analytics.revenue_by_country AS
        SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue
        FROM orders o JOIN customers c ON c.id = o.customer_id
        WHERE o.status <> 'cancelled'
        GROUP BY c.country
    ''')

The statement is parsed by Postgres's own parser (pglast), and translated
into the same steps a Python view is made of, so it runs on the same
engine, under the same exactly-once and sharding rules:

- `FROM t [alias] [[INNER | LEFT] JOIN u [alias] ON a.x = b.y [AND ...]]...`
  becomes source and joins. An inner join's other ON conditions move to
  WHERE, when every join is inner. Unqualified table names are public.
- `WHERE` becomes a filter after the joins.
- `GROUP BY` and the aggregates count(*), count(x), count(DISTINCT x),
  sum, min, max and avg become a group step. Expression keys and arguments
  are computed by a map before it; HAVING, and a SELECT list that is more
  than the keys and aggregates, by steps after it.
- `SELECT DISTINCT` becomes a distinct step.
- `CREATE [MATERIALIZED] VIEW s.v AS ...` names the view `v` and its sink
  the table `s.v`; a bare SELECT needs both given.

Expressions follow planner/sqlexpr.py: Postgres's semantics, or refused.
Refused here: set operations, CTEs, subqueries in FROM, ORDER BY and LIMIT
(a maintained table has no order), DISTINCT ON, window functions, GROUPING
SETS, RIGHT and FULL joins, USING and NATURAL, self-joins, and output
columns Postgres would name `?column?` (give them an alias).

min and max over text need `COLLATE "C"`, as text comparisons do: the
engine orders text by code point.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from pglast import ast, enums, parse_sql

from deltaflow.core import Row
from deltaflow.operators import Aggregate, Avg, Count, CountDistinct, Max, Min, Sum
from deltaflow.planner.dsl import Deltaflow, View, ViewBuilder
from deltaflow.planner.sqlexpr import (
    Compiled,
    EvalError,
    SqlError,
    compile_expr,
    deparse,
    output,
    truth,
)
from deltaflow.sinks import PostgresSink
from deltaflow.sinks.base import Sink

#: per source table, its columns' static kinds (int, numeric, text, ...)
type ColumnKinds = Mapping[str, Mapping[str, str]]


def sql_view(
    df: Deltaflow,
    sql: str,
    *,
    name: str | None = None,
    sink: Sink | None = None,
    column_kinds: ColumnKinds | None = None,
) -> View:
    """Register the view `sql` defines. `column_kinds`, from the catalog,
    makes the expressions' types exactly Postgres's (sqlexpr.py)."""
    target, select = _statement(sql)
    if target is not None:
        schema, relname = target
        name = name or relname
        sink = sink or PostgresSink(f"{schema}.{relname}")
    if name is None or sink is None:
        raise SqlError(
            "a bare SELECT needs name= and sink=; or write CREATE VIEW s.v AS SELECT ..."
        )
    return _Translator(df, name, select, column_kinds or {}).translate(sink)


def _statement(sql: str) -> tuple[tuple[str, str] | None, ast.SelectStmt]:
    statements = parse_sql(sql)
    if len(statements) != 1:
        raise SqlError(f"expected one statement, found {len(statements)}")
    stmt = statements[0].stmt
    if isinstance(stmt, ast.ViewStmt):
        if stmt.aliases:
            raise SqlError("CREATE VIEW v (columns) is not supported: alias the SELECT list")
        query, rel = stmt.query, stmt.view
    elif (
        isinstance(stmt, ast.CreateTableAsStmt) and stmt.objtype == enums.ObjectType.OBJECT_MATVIEW
    ):
        into = stmt.into
        assert into is not None
        if into.colNames:
            raise SqlError(
                "CREATE MATERIALIZED VIEW v (columns) is not supported: alias the SELECT list"
            )
        query, rel = stmt.query, into.rel
    elif isinstance(stmt, ast.SelectStmt):
        return None, stmt
    else:
        raise SqlError(
            f"expected SELECT or CREATE [MATERIALIZED] VIEW, found {type(stmt).__name__}"
        )
    if not isinstance(query, ast.SelectStmt):
        raise SqlError("the view must be a SELECT")
    assert rel is not None
    return (rel.schemaname or "public", str(rel.relname)), query


@dataclass
class _Table:
    schema: str
    relname: str
    #: the column kinds the catalog gave, if any
    kinds: Mapping[str, str] = field(default_factory=dict)

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.relname}"


class _Translator:
    def __init__(
        self, df: Deltaflow, name: str, select: ast.SelectStmt, kinds: ColumnKinds
    ) -> None:
        self.df = df
        self.name = name
        self.select = select
        self.column_kinds = kinds
        self.tables: dict[str, _Table] = {}  # alias (or relname) -> table
        self.joined = False

    # --- the statement ------------------------------------------------------------

    def translate(self, sink: Sink) -> View:
        s = self.select
        self._check_shape(s)
        builder = self.df.view(self.name)
        extra_where = self._from(builder, _items(s.fromClause))
        predicates = [w for w in [s.whereClause, *extra_where] if w is not None]
        for where in predicates:
            compiled = self._compile(where)
            builder.filter(_Predicate(compiled, "where"))
        targets = self._targets(_items(s.targetList))
        if self._aggregates_used(targets) or s.groupClause or s.havingClause is not None:
            self._aggregation(builder, targets)
        else:
            self._projection(builder, targets)
        if s.distinctClause:
            builder.distinct()
        return builder.sink(sink)

    def _check_shape(self, s: ast.SelectStmt) -> None:
        if s.op != enums.SetOperation.SETOP_NONE:
            raise SqlError("UNION, INTERSECT and EXCEPT are not supported in a maintained view")
        refused = {
            "a WITH clause (CTE)": s.withClause,
            "VALUES": s.valuesLists,
            "ORDER BY (a maintained table has no order)": s.sortClause,
            "LIMIT or OFFSET": s.limitCount or s.limitOffset,
            "FOR UPDATE / FOR SHARE": s.lockingClause,
            "a WINDOW clause": s.windowClause,
            "SELECT INTO": s.intoClause,
        }
        for what, present in refused.items():
            if present:
                raise SqlError(f"{what} is not supported in a maintained view")
        if s.distinctClause and any(d is not None for d in s.distinctClause):
            raise SqlError("DISTINCT ON is not supported (it picks a row per group by order)")
        if not s.fromClause:
            raise SqlError("a maintained view reads at least one table (FROM ...)")
        for item in _items(s.groupClause):
            if isinstance(item, ast.GroupingSet):
                raise SqlError("GROUPING SETS, ROLLUP and CUBE are not supported")

    # --- FROM and joins --------------------------------------------------------

    def _from(self, builder: ViewBuilder, items: tuple[Any, ...]) -> list[ast.Node]:
        """Source and joins; returns ON conditions that move to WHERE."""
        base, chain = _join_chain(items)
        builder.source(self._table(base).qualified)
        self.joined = bool(chain)
        all_inner = all(j == enums.JoinType.JOIN_INNER for _, j, _ in chain)
        moved: list[ast.Node] = []
        left_aliases = set(self.tables)
        for i, (rangevar, jointype, quals) in enumerate(chain):
            right_alias = self._alias(rangevar)
            right = self._table(rangevar)
            pairs: list[tuple[str, str]] = []
            for cond in _conjuncts(quals):
                pair = self._join_pair(cond, left_aliases, right_alias, first=i == 0)
                if pair is not None:
                    pairs.append(pair)
                elif all_inner:
                    moved.append(cond)  # same rows, whether filtered in ON or WHERE
                else:
                    raise SqlError(
                        f"ON condition {deparse(cond)} is not an equality of columns: with a "
                        "left join in the view, only equalities between the two sides are supported"
                    )
            if not pairs:
                raise SqlError(f"the join with {right.qualified} needs an ON equality of columns")
            how = "left" if jointype == enums.JoinType.JOIN_LEFT else "inner"
            builder.join(right.qualified, on=pairs, how=how)
            left_aliases.add(right_alias)
        return moved

    def _alias(self, rangevar: ast.RangeVar) -> str:
        if rangevar.alias is not None:
            if rangevar.alias.colnames:
                raise SqlError("column aliases on a table (t AS x(a, b)) are not supported")
            return str(rangevar.alias.aliasname)
        return str(rangevar.relname)

    def _table(self, rangevar: ast.RangeVar) -> _Table:
        alias = self._alias(rangevar)
        if alias in self.tables:
            raise SqlError(f"table name {alias!r} is used twice in FROM")
        schema, relname = rangevar.schemaname or "public", str(rangevar.relname)
        table = _Table(schema, relname, self.column_kinds.get(f"{schema}.{relname}", {}))
        self.tables[alias] = table
        return table

    def _join_pair(
        self, cond: ast.Node, left: set[str], right: str, *, first: bool
    ) -> tuple[str, str] | None:
        """(left key, right column) if `cond` is `left_col = right_col`."""
        if not (
            isinstance(cond, ast.A_Expr)
            and cond.kind == enums.A_Expr_Kind.AEXPR_OP
            and cond.name
            and cond.name[-1].sval == "="
            and isinstance(cond.lexpr, ast.ColumnRef)
            and isinstance(cond.rexpr, ast.ColumnRef)
        ):
            return None
        sides = []
        for ref in (cond.lexpr, cond.rexpr):
            fields = _fields(ref)
            if len(fields) < 2:
                raise SqlError(f"qualify the columns of join condition {deparse(cond)}")
            sides.append((fields[-2], fields[-1]))
        (qa, ca), (qb, cb) = sides
        for (q_left, c_left), (q_right, c_right) in (((qa, ca), (qb, cb)), ((qb, cb), (qa, ca))):
            if q_right == right and q_left in left:
                table = self.tables[q_left]
                # The first join's left rows are the source's own, unqualified;
                # after a join they carry the table's name.
                key = c_left if first else f"{table.relname}.{c_left}"
                return key, c_right
        return None

    # --- columns ------------------------------------------------------------------

    def _resolve(self, fields: tuple[str, ...]) -> str:
        if len(fields) == 1:
            return fields[0]  # unqualified: the row resolves an unambiguous name
        qualifier, column = fields[-2], fields[-1]
        table = self.tables.get(qualifier)
        if table is None:
            raise SqlError(f"missing FROM-clause entry for table {qualifier!r}")
        return f"{table.relname}.{column}" if self.joined else column

    def _kind(self, key: str) -> str | None:
        relname, dot, column = key.rpartition(".")
        if dot:
            for table in self.tables.values():
                if table.relname == relname:
                    return table.kinds.get(column)
            return None
        kinds = [t.kinds[key] for t in self.tables.values() if key in t.kinds]
        return kinds[0] if len(kinds) == 1 else None

    def _compile(self, node: ast.Node, substitute: Callable[[ast.Node], str | None] | None = None,
                 kinds: Callable[[str], str | None] | None = None) -> Compiled:  # fmt: skip
        return compile_expr(node, self._resolve, substitute, kinds or self._kind)

    # --- SELECT list ------------------------------------------------------------

    def _targets(self, targets: tuple[Any, ...]) -> list[tuple[str | None, ast.Node]]:
        out: list[tuple[str | None, ast.Node]] = []
        for target in targets:
            assert isinstance(target, ast.ResTarget)
            if target.indirection:
                raise SqlError("subscripted output columns are not supported")
            assert target.val is not None
            out.append((target.name, target.val))
        return out

    def _names(self, targets: list[tuple[str | None, ast.Node]]) -> list[str]:
        names = [name or _figure_colname(node) for name, node in targets]
        for name, (_, node) in zip(names, targets, strict=True):
            if name == "?column?":
                raise SqlError(f"give the output column {deparse(node)} a name (AS ...)")
        duplicates = {n for n in names if names.count(n) > 1}
        if duplicates:
            raise SqlError(f"output columns {sorted(duplicates)} appear twice: alias them")
        return names

    def _projection(self, builder: ViewBuilder, targets: list[tuple[str | None, ast.Node]]) -> None:
        if len(targets) == 1 and _is_star(targets[0][1]):
            if self.joined:
                raise SqlError("SELECT * over a join is not supported: list the columns")
            return  # the source's rows, as they are
        names = self._names(targets)
        items = [(n, self._compile(node)) for n, (_, node) in zip(names, targets, strict=True)]
        builder.map(_Mapping(items))

    # --- aggregation -----------------------------------------------------------------

    def _aggregates_used(self, targets: list[tuple[str | None, ast.Node]]) -> bool:
        nodes = [n for _, n in targets]
        if self.select.havingClause is not None:
            nodes.append(self.select.havingClause)
        return any(_aggregate_calls(n) for n in nodes)

    def _aggregation(
        self, builder: ViewBuilder, targets: list[tuple[str | None, ast.Node]]
    ) -> None:
        names = self._names(targets)
        grouping = _Grouping()
        self._group_keys(grouping, targets)
        self._group_aggregates(grouping, targets)
        # output names of keys and aggregates that are plain SELECT items
        plain: dict[str, str] = {}
        for out_name, (_, node) in zip(names, targets, strict=True):
            text = deparse(node)
            if (text in grouping.keys or text in grouping.aggs) and text not in plain:
                plain[text] = out_name
        key_names = {t: plain.get(t, f"__k{i}") for i, t in enumerate(grouping.keys)}
        agg_names = {t: plain.get(t, column) for t, (column, _, _) in grouping.aggs.items()}
        if grouping.needs_map:
            builder.map(_Mapping(grouping.inputs))
        aggregates = {agg_names[t]: agg for t, (_, agg, _) in grouping.aggs.items()}
        if grouping.keys:
            by = {key_names[t]: column for t, column in grouping.keys.items()}
            builder.group_by(**by).agg(**aggregates)
        else:
            builder.agg(**aggregates)
        self._after_grouping(
            builder, grouping, names, targets, key_names=key_names, agg_names=agg_names
        )

    def _group_keys(self, grouping: _Grouping, targets: list[tuple[str | None, ast.Node]]) -> None:
        for i, item in enumerate(_items(self.select.groupClause)):
            node = self._group_item(item, targets)
            text = deparse(node)
            if text not in grouping.keys:
                compiled = self._compile(node)
                grouping.keys[text] = self._input(grouping, node, compiled, f"__g{i}")
                grouping.key_kinds[text] = compiled.kind
                if isinstance(node, ast.ColumnRef):
                    grouping.key_columns[self._resolve(_fields(node))] = text

    def _group_aggregates(
        self, grouping: _Grouping, targets: list[tuple[str | None, ast.Node]]
    ) -> None:
        calls = [c for _, node in targets for c in _aggregate_calls(node)]
        if self.select.havingClause is not None:
            calls.extend(_aggregate_calls(self.select.havingClause))
        for i, call in enumerate(calls):
            text = deparse(call)
            if text not in grouping.aggs:
                agg, kind = self._aggregate(call, grouping, f"__a{i}")
                grouping.aggs[text] = (f"__agg{i}", agg, kind)

    def _after_grouping(
        self,
        builder: ViewBuilder,
        grouping: _Grouping,
        names: list[str],
        targets: list[tuple[str | None, ast.Node]],
        *,
        key_names: dict[str, str],
        agg_names: dict[str, str],
    ) -> None:
        """HAVING, then the SELECT list if it is more than the keys and
        aggregates: over the grouped row, where both are columns."""
        after = {**{t: key_names[t] for t in grouping.keys}, **agg_names}
        kinds = {key_names[t]: grouping.key_kinds[t] for t in grouping.keys} | {
            agg_names[t]: kind for t, (_, _, kind) in grouping.aggs.items()
        }

        def substitute(node: ast.Node) -> str | None:
            text = deparse(node)
            if text in after:
                return after[text]
            if isinstance(node, ast.ColumnRef):
                key_text = grouping.key_columns.get(self._resolve(_fields(node)))
                return key_names[key_text] if key_text is not None else None
            return None

        having = self.select.havingClause
        if having is not None:
            builder.filter(_Predicate(self._compile(having, substitute, kinds.get), "having"))
        pairs = list(zip(names, targets, strict=True))
        exact = all(after.get(deparse(node)) == out for out, (_, node) in pairs)
        if not (exact and set(after.values()) == set(names)):
            items = [(out, self._compile(node, substitute, kinds.get)) for out, (_, node) in pairs]
            builder.map(_Mapping(items))

    def _input(self, grouping: _Grouping, node: ast.Node, compiled: Compiled, label: str) -> str:
        """The input column of a group key or aggregate argument: the column
        itself, or one the map before grouping computes."""
        if isinstance(node, ast.ColumnRef):
            key = self._resolve(_fields(node))
            if key not in dict(grouping.inputs):
                grouping.inputs.append((key, compiled))
            return key
        grouping.needs_map = True
        grouping.inputs.append((label, compiled))
        return label

    def _group_item(self, item: ast.Node, targets: list[tuple[str | None, ast.Node]]) -> ast.Node:
        """A GROUP BY item; an integer is a position in the SELECT list."""
        if isinstance(item, ast.A_Const) and isinstance(item.val, ast.Integer):
            position = int(item.val.ival or 0)
            if not 1 <= position <= len(targets):
                raise SqlError(f"GROUP BY position {position} is not in the select list")
            return targets[position - 1][1]
        if isinstance(item, ast.ColumnRef) and len(_fields(item)) == 1:
            alias = _fields(item)[0]
            for name, node in targets:
                if name == alias and not (
                    isinstance(node, ast.ColumnRef) and _fields(node)[-1] == alias
                ):
                    raise SqlError(
                        f"GROUP BY {alias}: grouping by an output column's name is not supported "
                        "(Postgres would look for an input column first); repeat the expression"
                    )
        return item

    def _aggregate(
        self, call: ast.FuncCall, grouping: _Grouping, label: str
    ) -> tuple[Aggregate, str | None]:
        fname = str(_items(call.funcname)[-1].sval).lower()
        if call.agg_filter is not None or call.agg_order or call.agg_within_group:
            raise SqlError(
                f"{deparse(call)}: FILTER and ORDER BY in an aggregate are not supported"
            )
        if call.over is not None:
            raise SqlError(f"window function {deparse(call)} is not supported")
        if call.agg_star:
            if fname != "count":
                raise SqlError(f"{deparse(call)} is not an aggregate Deltaflow maintains")
            return Count(), "int"
        args = _items(call.args)
        if len(args) != 1:
            raise SqlError(f"{deparse(call)}: aggregates of one argument are supported")
        (arg,) = args
        for nested in _aggregate_calls(arg):
            raise SqlError(f"aggregate calls cannot be nested: {deparse(nested)}")
        if call.agg_distinct and fname != "count":
            raise SqlError(f"{fname}(DISTINCT ...) is not supported; count(DISTINCT ...) is")
        compiled = self._compile(arg)
        if fname in ("min", "max") and not isinstance(arg, ast.CollateClause):
            if compiled.kind in (None, "text", "unknown"):
                # text order is the collation's; an uncollated text value raises
                grouping.needs_map = True
                grouping.inputs.append((label, _guard_text(compiled)))
                column = label
            else:
                column = self._input(grouping, arg, compiled, label)
        else:
            column = self._input(grouping, arg, compiled, label)
        made = _AGGREGATE_MAKERS.get("count distinct" if call.agg_distinct else fname)
        if made is None:
            raise SqlError(f"{deparse(call)} is not an aggregate Deltaflow maintains")
        make, kind = made
        return make(column), kind if kind != "arg" else compiled.kind


@dataclass
class _Grouping:
    """What one GROUP BY translation collects."""

    #: the map before grouping: input columns of keys and arguments
    inputs: list[tuple[str, Compiled]] = field(default_factory=list)
    needs_map: bool = False
    #: group key SQL -> input column, and its kind
    keys: dict[str, str] = field(default_factory=dict)
    key_kinds: dict[str, str | None] = field(default_factory=dict)
    #: row key of a column that is a group key -> that key's SQL
    key_columns: dict[str, str] = field(default_factory=dict)
    #: aggregate call SQL -> (its column, the aggregate, its kind)
    aggs: dict[str, tuple[str, Aggregate, str | None]] = field(default_factory=dict)


#: aggregate name -> (maker, result kind; "arg" for the argument's own)
_AGGREGATE_MAKERS: dict[str, tuple[Callable[[str], Aggregate], str | None]] = {
    "count": (Count, "int"),
    "count distinct": (CountDistinct, "int"),
    "min": (Min, "arg"),
    "max": (Max, "arg"),
    # sum(int4) is int8 and sum(int8) numeric in Postgres; exact either way
    "sum": (Sum, None),
    "avg": (Avg, "numeric"),
}


# --- callables that the plan holds ----------------------------------------------------


class _Predicate:
    """WHERE or HAVING: a row is kept when the condition is TRUE (not NULL)."""

    def __init__(self, compiled: Compiled, clause: str) -> None:
        self._compiled = compiled
        self.__qualname__ = f"sql:{clause}({compiled.sql})"

    def __call__(self, row: Row) -> bool:
        return truth(self._compiled(row)) is True


class _Mapping:
    """A SELECT list (or the inputs of a group step): named expressions."""

    def __init__(self, items: list[tuple[str, Compiled]]) -> None:
        self._items = items
        self.__qualname__ = "sql:select(" + ", ".join(f"{c.sql} AS {n}" for n, c in items) + ")"

    def __call__(self, row: Row) -> Mapping[str, Any]:
        return {name: output(compiled(row)) for name, compiled in self._items}


def _guard_text(compiled: Compiled) -> Compiled:
    """min and max order text by code point; Postgres by the column's
    collation. Without COLLATE "C" a text value raises instead."""
    inner = compiled

    def guarded(row: Row) -> Any:
        value = inner(row)
        if isinstance(value, str):
            raise EvalError(
                'min and max of text depend on the collation: write min(column COLLATE "C"), '
                "code-point order, which is the order Deltaflow maintains"
            )
        return value

    return Compiled(compiled.sql, fn=guarded, kind=compiled.kind)


# --- parse-tree helpers ---------------------------------------------------------------


def _items(value: Any) -> tuple[Any, ...]:
    return tuple(value or ())


def _fields(ref: ast.ColumnRef) -> tuple[str, ...]:
    fields = _items(ref.fields)
    if not all(isinstance(f, ast.String) for f in fields):
        raise SqlError(f"{deparse(ref)}: list the columns instead of using *")
    return tuple(str(f.sval) for f in fields)


def _is_star(node: ast.Node) -> bool:
    return isinstance(node, ast.ColumnRef) and any(
        isinstance(f, ast.A_Star) for f in _items(node.fields)
    )


def _conjuncts(node: ast.Node) -> list[ast.Node]:
    if isinstance(node, ast.BoolExpr) and node.boolop == enums.BoolExprType.AND_EXPR:
        return [c for arg in _items(node.args) for c in _conjuncts(arg)]
    return [node]


_AGGREGATE_NAMES = frozenset({"count", "sum", "min", "max", "avg"})


def _aggregate_calls(node: Any) -> list[ast.FuncCall]:
    """The aggregate calls in an expression (not looking inside them)."""
    found: list[ast.FuncCall] = []

    def walk(n: Any) -> None:
        if isinstance(n, ast.FuncCall):
            name = str(_items(n.funcname)[-1].sval).lower()
            if n.agg_star or name in _AGGREGATE_NAMES:
                found.append(n)
                return
        if isinstance(n, ast.Node):
            for attr in n:
                walk(getattr(n, attr))
        elif isinstance(n, tuple | list):
            for x in n:
                walk(x)

    walk(node)
    return found


def _figure_colname(node: ast.Node) -> str:  # noqa: PLR0911 - one case per node type
    """Postgres's FigureColname: the name an unaliased output column gets."""
    if isinstance(node, ast.ColumnRef):
        last = _items(node.fields)[-1]
        return str(last.sval) if isinstance(last, ast.String) else "?column?"
    if isinstance(node, ast.FuncCall):
        return str(_items(node.funcname)[-1].sval)
    if isinstance(node, ast.TypeCast):
        inner = _figure_colname(node.arg) if node.arg is not None else "?column?"
        if inner != "?column?" or node.typeName is None:
            return inner
        return str(_items(node.typeName.names)[-1].sval)
    if isinstance(node, ast.CollateClause) and node.arg is not None:
        return _figure_colname(node.arg)
    if isinstance(node, ast.MinMaxExpr):
        return "greatest" if node.op == enums.MinMaxOp.IS_GREATEST else "least"
    if isinstance(node, ast.A_Expr) and node.kind == enums.A_Expr_Kind.AEXPR_NULLIF:
        return "nullif"
    return _FIXED_NAMES.get(type(node).__name__, "?column?")


_FIXED_NAMES = {"CaseExpr": "case", "CoalesceExpr": "coalesce"}


def _join_chain(items: tuple[Any, ...]) -> tuple[ast.RangeVar, list[tuple[ast.RangeVar, Any, Any]]]:
    """The FROM clause as its first table and the joins after it, in order:
    (table, join type, ON conditions). Refuses what is not a left-deep
    chain of inner and left joins of tables."""
    if len(items) != 1:
        raise SqlError("FROM with a comma list is not supported: write the joins with JOIN ... ON")
    chain: list[tuple[ast.RangeVar, Any, Any]] = []
    node = items[0]
    while isinstance(node, ast.JoinExpr):
        _check_join(node)
        assert isinstance(node.rarg, ast.RangeVar)
        chain.append((node.rarg, node.jointype, node.quals))
        node = node.larg
    if not isinstance(node, ast.RangeVar):
        raise SqlError("FROM must name tables: subqueries and functions are not supported")
    chain.reverse()
    return node, chain


def _check_join(node: ast.JoinExpr) -> None:
    if not isinstance(node.rarg, ast.RangeVar):
        raise SqlError(
            "only left-deep joins of tables are supported: t JOIN u ON ... JOIN v ON ..."
        )
    if node.isNatural or node.usingClause:
        raise SqlError("NATURAL joins and JOIN ... USING are not supported: write JOIN ... ON")
    jointype = node.jointype
    if jointype not in (enums.JoinType.JOIN_INNER, enums.JoinType.JOIN_LEFT):
        kind = jointype.name.removeprefix("JOIN_") if jointype is not None else "these"
        raise SqlError(
            f"{kind} joins are not supported: inner and left joins are "
            "(a right join is a left join with the tables swapped)"
        )
    if node.quals is None:
        raise SqlError("a join needs ON conditions (CROSS JOIN is not supported)")
