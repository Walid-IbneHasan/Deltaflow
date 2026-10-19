"""SQL expressions of a view, compiled to Python with PostgreSQL's semantics.

A view written in SQL (planner/sql.py) is translated into the dataflow plan;
its WHERE, HAVING, SELECT and GROUP BY expressions become Python functions
of a row. Each must return exactly what Postgres returns, so the supported
subset is deliberately small, and anything outside it raises `SqlError` at
registration rather than being approximated:

- column references; NULL, integer, numeric, string and boolean literals;
  typed literals and casts to int2/int4/int8, numeric[(p, s)], text,
  boolean, date, timestamp[tz] and float8;
- `+ - * / %` (and unary minus), `||`, comparisons, `AND OR NOT`,
  `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE/UNKNOWN`, `[NOT] IN (...)`,
  `[NOT] BETWEEN [SYMMETRIC]`, `IS [NOT] DISTINCT FROM`, `[NOT] LIKE`,
  CASE, COALESCE, NULLIF, GREATEST, LEAST, `abs`, `round`, `length`;
- `COLLATE "C"`, which ordering comparisons of text require: under any
  other collation the order is the locale's, which Python does not have.

Semantics that need care, each reproduced:

- **Types.** Postgres types expressions statically. Here every node has a
  static kind where one is known (`int`, `numeric`, `float`, `text`,
  `bool`, ...): from literals, casts, functions and operators, and from
  the columns' types when the caller supplies them. CASE, COALESCE,
  GREATEST and LEAST resolve a common kind as Postgres's select_common_type
  does, and promote every branch to it: COALESCE(numeric, float8) is
  float8 even when the numeric is not NULL. Where a kind is not known
  statically (columns, without their types), a value's runtime type
  decides: int is an integer, Decimal numeric, float float8. A string
  literal without a cast is `Untyped` and, as in Postgres, takes the type
  of what it meets.
- **Arithmetic.** Integer division and `%` truncate toward zero; numeric
  `+ - *` are exact with Postgres's result scales, and `/` uses
  select_div_scale (core/numeric.py). Integer results outside int8 raise.
  Overflow of a narrower column type (int2, int4) is not detected.
- **NULL.** Three-valued logic throughout; strict operators give NULL.
- **Errors and constant folding.** Postgres folds constant subexpressions
  while planning, with its own rules for which ones it reaches
  (eval_const_expressions): `CASE WHEN x THEN 1/0 END` raises even when no
  row takes the branch, while `FALSE AND 1/0 = 1` does not. Compilation
  folds the same way, so an expression raises exactly where Postgres's
  would; runtime evaluation then follows Postgres's order (AND and OR
  left to right, CASE lazily).
"""

from __future__ import annotations

import datetime as dt
import decimal
import math
import operator
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pglast import ast, enums, parse_sql
from pglast.stream import RawStream

from deltaflow.core import Row
from deltaflow.core.numeric import EXACT, numeric_div, positive_zero, round_half_away
from deltaflow.planner.dsl import PlanError

INT8_MIN, INT8_MAX = -(2**63), 2**63 - 1
INT_WIDTH = {
    "int2": 16,
    "smallint": 16,
    "int4": 32,
    "integer": 32,
    "int": 32,
    "int8": 64,
    "bigint": 64,
}
C_COLLATIONS = frozenset({"C", "POSIX", "ucs_basic"})


class SqlError(PlanError):
    """The SQL uses something Deltaflow does not maintain, or not exactly."""


class EvalError(RuntimeError):
    """Evaluating raised, as Postgres would (division by zero, overflow), or
    met a value whose result Postgres would decide differently than Python
    can (text ordering without COLLATE "C")."""


class Untyped(str):
    """A string literal without a cast: Postgres types it from its context."""


type Resolver = Callable[[tuple[str, ...]], str]
type Substitute = Callable[[ast.Node], str | None]


def deparse(node: ast.Node) -> str:
    return str(RawStream()(node))


# --- compiled nodes -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Compiled:
    """A compiled (sub)expression: a constant, a function of the row, or a
    constant whose folding raised (kept, so a parent that never reaches it,
    as Postgres would not, can drop it)."""

    sql: str
    fn: Callable[[Row], Any] | None = None
    value: Any = None
    error: EvalError | None = None
    collate_c: bool = False
    #: the static type, where known: int, numeric, float, text, bool, date,
    #: timestamp, timestamptz, or unknown (an untyped literal)
    kind: str | None = None

    @property
    def const(self) -> bool:
        return self.fn is None and self.error is None

    def __call__(self, row: Row) -> Any:
        if self.error is not None:
            raise self.error
        return self.value if self.fn is None else self.fn(row)

    def raise_error(self) -> None:
        if self.error is not None:
            raise self.error


def _const(sql: str, value: Any, *, collate_c: bool = False, kind: str | None = None) -> Compiled:
    return Compiled(sql, value=value, collate_c=collate_c, kind=kind)


def _fold(
    sql: str,
    args: Sequence[Compiled],
    evaluate: Callable[..., Any],
    *,
    strict: bool = True,
    collate_c: bool = False,
    kind: str | None = None,
) -> Compiled:
    """An operator over `args`: errors of its arguments propagate (Postgres
    folds every argument first), a strict operator with a NULL constant
    argument is NULL, and all-constant arguments fold now."""
    for arg in args:
        if arg.error is not None:
            return Compiled(sql, error=arg.error, collate_c=collate_c, kind=kind)
    if strict and any(a.const and a.value is None for a in args):
        return _const(sql, None, collate_c=collate_c, kind=kind)
    if all(a.const for a in args):
        try:
            return _const(sql, evaluate(*(a.value for a in args)), collate_c=collate_c, kind=kind)
        except EvalError as exc:
            return Compiled(sql, error=exc, collate_c=collate_c, kind=kind)
    fns = [_function(a) for a in args]
    if not strict:
        return Compiled(sql, fn=lambda row: evaluate(*(f(row) for f in fns)),
                        collate_c=collate_c, kind=kind)  # fmt: skip

    def strict_evaluate(row: Row) -> Any:
        values = [f(row) for f in fns]  # every argument, as Postgres evaluates them
        return None if any(v is None for v in values) else evaluate(*values)

    return Compiled(sql, fn=strict_evaluate, collate_c=collate_c, kind=kind)


def _function(c: Compiled) -> Callable[[Row], Any]:
    if c.fn is not None:
        return c.fn
    value = c.value
    return lambda _row: value


# --- static types ------------------------------------------------------------------

_NUMERIC_KINDS = frozenset({"int", "numeric", "float"})


def _arith_kind(a: str | None, b: str | None) -> str | None:
    if "float" in (a, b):
        return "float"
    a = b if a == "unknown" else a
    b = a if b == "unknown" else b
    if a is None or b is None:
        return None
    if a == "numeric" or b == "numeric":
        return "numeric"
    return "int" if a == b == "int" else None


def _common_kind(kinds: Sequence[str | None]) -> str | None:
    """select_common_type for the kinds reproduced: float8 over numeric over
    the integers; all untyped literals resolve to text."""
    known = {k for k in kinds if k not in (None, "unknown")}
    if not known:
        return "text" if kinds and all(k == "unknown" for k in kinds) else None
    if known <= _NUMERIC_KINDS:
        if "float" in known:
            return "float"
        if "numeric" in known:
            return "numeric"
        return None if None in kinds else "int"
    return known.pop() if len(known) == 1 else None


def _promote(value: Any, kind: str | None) -> Any:
    """A value of a branch converted to the common type."""
    if value is None or kind is None or isinstance(value, bool):
        return value
    if kind == "float" and isinstance(value, int | Decimal):
        return float(value)
    if kind == "numeric" and isinstance(value, int):
        return Decimal(value)
    return value


_SAMPLE = {
    "int": 0,
    "numeric": Decimal(0),
    "float": 0.0,
    "bool": False,
    "date": dt.date(2000, 1, 1),
}


def _unify(branches: list[Compiled]) -> tuple[list[Compiled], str | None]:
    """The branches of CASE, COALESCE, GREATEST or LEAST converted to their
    common kind. As in Postgres, the common type is of every branch, also
    those never reached, and an untyped literal that cannot take it fails
    at once (Postgres's parser raises): COALESCE(1, 'x') is an error."""
    kind = _common_kind([b.kind for b in branches])
    out: list[Compiled] = []
    for b in branches:
        if b.const and isinstance(b.value, Untyped) and kind in _SAMPLE:
            out.append(_const(b.sql, _coerce(b.value, _SAMPLE[kind]), kind=kind))
        elif b.const:
            out.append(_const(b.sql, _promote(b.value, kind), collate_c=b.collate_c, kind=kind))
        elif b.fn is not None and kind in ("float", "numeric") and b.kind != kind:
            out.append(Compiled(b.sql, fn=_promoting(b.fn, kind), collate_c=b.collate_c, kind=kind))
        else:
            out.append(
                Compiled(
                    b.sql, fn=b.fn, value=b.value, error=b.error, collate_c=b.collate_c,
                    kind=kind if b.error is None else b.kind,
                )
            )  # fmt: skip
    return out, kind


def _promoting(fn: Callable[[Row], Any], kind: str | None) -> Callable[[Row], Any]:
    return lambda row: _promote(fn(row), kind)


def _items(field: Any) -> tuple[Any, ...]:
    """A list field of a parse node (None when empty)."""
    return tuple(field or ())


# --- compilation ----------------------------------------------------------------


def compile_expr(
    node: ast.Node,
    resolve: Resolver,
    substitute: Substitute | None = None,
    kinds: Callable[[str], str | None] | None = None,
) -> Compiled:
    """Compile `node`. `resolve` maps a column reference to the row key that
    holds it; `substitute`, if given, may claim any node as a column of the
    row (an aggregate, or a GROUP BY expression, after grouping); `kinds`,
    if given, gives a row key's static kind. Raises SqlError for anything
    unsupported, and EvalError where Postgres would raise while planning."""
    compiled = _Compiler(resolve, substitute, kinds).node(node)
    compiled.raise_error()
    return compiled


def expression(sql: str, kinds: dict[str, str] | None = None) -> Callable[[Row], Any]:
    """`sql` (a single expression) over rows with unqualified column names,
    whose kinds `kinds` gives (if known); for tests and one-off use."""
    (raw,) = parse_sql(f"SELECT {sql}")
    stmt = raw.stmt
    assert isinstance(stmt, ast.SelectStmt)
    (target,) = _items(stmt.targetList)
    lookup = kinds.get if kinds is not None else None
    compiled = compile_expr(target.val, lambda fields: fields[-1], None, lookup)
    return lambda row: output(compiled(row))


def output(value: Any) -> Any:
    """A value as it leaves an expression: an untyped literal is text."""
    return str(value) if isinstance(value, Untyped) else value


class _Compiler:
    def __init__(
        self,
        resolve: Resolver,
        substitute: Substitute | None,
        kinds: Callable[[str], str | None] | None,
    ) -> None:
        self.resolve = resolve
        self.substitute = substitute
        self.kinds = kinds or (lambda _key: None)

    def node(self, node: Any) -> Compiled:
        if not isinstance(node, ast.Node):
            raise SqlError(f"expected an expression, found {node!r}")
        sql = deparse(node)
        if self.substitute is not None and (key := self.substitute(node)) is not None:
            return Compiled(sql, fn=lambda row: row[key], kind=self.kinds(key))
        handler = getattr(self, f"_{type(node).__name__}", None)
        if handler is None:
            raise SqlError(f"{_describe(node)} is not supported in a maintained view: {sql}")
        result: Compiled = handler(node, sql)
        return result

    # leaves

    def _ColumnRef(self, node: ast.ColumnRef, sql: str) -> Compiled:
        fields = tuple(str(f.sval) for f in _items(node.fields) if isinstance(f, ast.String))
        if len(fields) != len(_items(node.fields)):
            raise SqlError(f"{sql}: list the columns instead of using *")
        key = self.resolve(fields)
        if self.substitute is not None:
            # after grouping, a column must be a group key, which substitute claimed
            raise SqlError(
                f"column {sql} must appear in the GROUP BY clause or be used in an aggregate"
            )
        return Compiled(sql, fn=lambda row: row[key], kind=self.kinds(key))

    def _A_Const(self, node: ast.A_Const, sql: str) -> Compiled:
        if node.isnull:
            return _const(sql, None, kind="unknown")
        val = node.val
        if isinstance(val, ast.Integer):
            return _const(sql, val.ival, kind="int")
        if isinstance(val, ast.Float):
            text = str(val.fval)
            if re.fullmatch(r"\d+", text):
                n = int(text)  # too large for int4 only: int8, else numeric
                return (
                    _const(sql, n, kind="int")
                    if n <= INT8_MAX
                    else _const(sql, Decimal(n), kind="numeric")
                )
            return _const(sql, _numeric_in(text), kind="numeric")
        if isinstance(val, ast.String):
            return _const(sql, Untyped(str(val.sval)), kind="unknown")
        if isinstance(val, ast.Boolean):
            return _const(sql, bool(val.boolval), kind="bool")
        raise SqlError(f"literal {sql} is not supported")

    # casts and collation

    def _TypeCast(self, node: ast.TypeCast, sql: str) -> Compiled:
        type_name = node.typeName
        assert type_name is not None
        name = _type_name(type_name)
        cast = _cast_function(name, type_name, sql)
        return _fold(sql, [self.node(node.arg)], cast, kind=_CAST_KINDS.get(name))

    def _CollateClause(self, node: ast.CollateClause, sql: str) -> Compiled:
        names = [str(n.sval) for n in _items(node.collname)]
        if names[-1] not in C_COLLATIONS:
            raise SqlError(
                f'{sql}: only the C collation (COLLATE "C") is supported, whose order '
                "is code-point order; a locale's order is not reproduced"
            )
        inner = self.node(node.arg)
        return Compiled(sql, fn=inner.fn, value=inner.value, error=inner.error, collate_c=True,
                        kind=inner.kind)  # fmt: skip

    # operators

    def _A_Expr(self, node: ast.A_Expr, sql: str) -> Compiled:  # noqa: PLR0911
        kind = node.kind
        op = str(node.name[-1].sval) if node.name else ""
        K = enums.A_Expr_Kind
        if kind == K.AEXPR_OP:
            if node.lexpr is None:
                return self._unary(op, self.node(node.rexpr), sql)
            left, right = self.node(node.lexpr), self.node(node.rexpr)
            collate = left.collate_c or right.collate_c
            if op in _COMPARISONS:
                return _fold(
                    sql, [left, right], lambda a, b: _compare(op, a, b, collate), kind="bool"
                )
            if op in _ARITHMETIC:
                result = _arith_kind(left.kind, right.kind)
                return _fold(
                    sql, [left, right], lambda a, b: _promote(_arith(op, a, b), result),
                    kind=result,
                )  # fmt: skip
            if op == "||":
                return _fold(sql, [left, right], _concat, kind="text")
            raise SqlError(f"operator {op} is not supported: {sql}")
        if kind in (K.AEXPR_DISTINCT, K.AEXPR_NOT_DISTINCT):
            left, right = self.node(node.lexpr), self.node(node.rexpr)
            negate = kind == K.AEXPR_NOT_DISTINCT
            return _fold(sql, [left, right], lambda a, b: _distinct(a, b) != negate, strict=False,
                         kind="bool")  # fmt: skip
        if kind == K.AEXPR_NULLIF:
            left, right = self.node(node.lexpr), self.node(node.rexpr)
            return _fold(sql, [left, right], _nullif, strict=False, kind=left.kind)
        if kind == K.AEXPR_IN:
            return self._in(node, op, sql)
        if kind == K.AEXPR_LIKE:
            if op not in ("~~", "!~~"):
                raise SqlError(
                    f"{sql}: only LIKE and NOT LIKE are supported (ILIKE folds case by locale)"
                )
            if isinstance(node.rexpr, ast.FuncCall):
                raise SqlError(f"{sql}: LIKE ... ESCAPE is not supported (the escape is \\)")
            left, right = self.node(node.lexpr), self.node(node.rexpr)
            negate = op == "!~~"
            return _fold(sql, [left, right], lambda a, p: _like(a, p) != negate, kind="bool")
        if kind in (
            K.AEXPR_BETWEEN,
            K.AEXPR_NOT_BETWEEN,
            K.AEXPR_BETWEEN_SYM,
            K.AEXPR_NOT_BETWEEN_SYM,
        ):
            return self._between(node, sql)
        raise SqlError(f"{sql} is not supported in a maintained view")

    def _unary(self, op: str, arg: Compiled, sql: str) -> Compiled:
        if op == "-":
            return _fold(sql, [arg], _negate, kind=arg.kind)
        if op == "+":
            return _fold(sql, [arg], _number, kind=arg.kind)
        raise SqlError(f"prefix operator {op} is not supported: {sql}")

    def _in(self, node: ast.A_Expr, op: str, sql: str) -> Compiled:
        if not isinstance(node.rexpr, tuple | list):
            raise SqlError(f"{sql}: IN takes a list of values here, not a subquery")
        lhs = self.node(node.lexpr)
        items = [self.node(e) for e in node.rexpr]
        negate = op == "<>"
        collate = lhs.collate_c or any(i.collate_c for i in items)

        def evaluate(a: Any, *values: Any) -> bool | None:
            if a is None:
                return None
            seen_null = False
            for v in values:
                eq = _compare("=", a, v, collate)
                if eq is True:
                    return not negate
                seen_null = seen_null or eq is None
            return None if seen_null else negate

        return _fold(sql, [lhs, *items], evaluate, strict=False, kind="bool")

    def _between(self, node: ast.A_Expr, sql: str) -> Compiled:
        K = enums.A_Expr_Kind
        value = node.lexpr
        low, high = _items(node.rexpr)
        ge = ast.A_Expr(kind=K.AEXPR_OP, name=(ast.String(sval=">="),), lexpr=value, rexpr=low)
        le = ast.A_Expr(kind=K.AEXPR_OP, name=(ast.String(sval="<="),), lexpr=value, rexpr=high)
        lt = ast.A_Expr(kind=K.AEXPR_OP, name=(ast.String(sval="<"),), lexpr=value, rexpr=low)
        gt = ast.A_Expr(kind=K.AEXPR_OP, name=(ast.String(sval=">"),), lexpr=value, rexpr=high)
        B = enums.BoolExprType
        inside = ast.BoolExpr(boolop=B.AND_EXPR, args=(ge, le))
        outside = ast.BoolExpr(boolop=B.OR_EXPR, args=(lt, gt))
        if node.kind in (K.AEXPR_BETWEEN_SYM, K.AEXPR_NOT_BETWEEN_SYM):
            # Postgres: (a >= x AND a <= y) OR (a >= y AND a <= x), negated for NOT
            ge2 = ast.A_Expr(
                kind=K.AEXPR_OP, name=(ast.String(sval=">="),), lexpr=value, rexpr=high
            )
            le2 = ast.A_Expr(kind=K.AEXPR_OP, name=(ast.String(sval="<="),), lexpr=value, rexpr=low)
            swapped = ast.BoolExpr(boolop=B.AND_EXPR, args=(ge2, le2))
            either = ast.BoolExpr(boolop=B.OR_EXPR, args=(inside, swapped))
            if node.kind == K.AEXPR_NOT_BETWEEN_SYM:
                either = ast.BoolExpr(boolop=B.NOT_EXPR, args=(either,))
            return self.node(either)
        return self.node(inside if node.kind == K.AEXPR_BETWEEN else outside)

    def _BoolExpr(self, node: ast.BoolExpr, sql: str) -> Compiled:
        B = enums.BoolExprType
        if node.boolop == B.NOT_EXPR:
            (only,) = _items(node.args)
            return _fold(sql, [self.node(only)], lambda a: not _boolean(a), kind="bool")
        is_and = node.boolop == B.AND_EXPR
        decisive = not is_and  # AND stops at FALSE, OR at TRUE
        kept: list[Compiled] = []
        has_null = False
        for arg_node in _items(node.args):
            arg = self.node(arg_node)
            if arg.error is not None:
                return Compiled(sql, error=arg.error, kind="bool")
            if arg.const:
                value = _boolean(arg.value)
                if value is None:
                    has_null = True
                elif value == decisive:
                    return _const(sql, decisive, kind="bool")  # later arguments are never folded
                continue  # a non-decisive constant drops out
            kept.append(arg)
        if not kept:
            return _const(sql, None if has_null else not decisive, kind="bool")
        fns = [a.fn for a in kept]

        def evaluate(row: Row) -> bool | None:
            null = has_null
            for f in fns:
                assert f is not None
                v = _boolean(f(row))
                if v is None:
                    null = True
                elif v == decisive:
                    return decisive
            return None if null else not decisive

        return Compiled(sql, fn=evaluate, kind="bool")

    def _NullTest(self, node: ast.NullTest, sql: str) -> Compiled:
        is_null = node.nulltesttype == enums.NullTestType.IS_NULL
        return _fold(sql, [self.node(node.arg)], lambda a: (a is None) == is_null, strict=False,
                     kind="bool")  # fmt: skip

    def _BooleanTest(self, node: ast.BooleanTest, sql: str) -> Compiled:
        T = enums.BoolTestType
        test: dict[Any, Callable[[Any], bool]] = {
            T.IS_TRUE: lambda a: a is True,
            T.IS_NOT_TRUE: lambda a: a is not True,
            T.IS_FALSE: lambda a: a is False,
            T.IS_NOT_FALSE: lambda a: a is not False,
            T.IS_UNKNOWN: lambda a: a is None,
            T.IS_NOT_UNKNOWN: lambda a: a is not None,
        }
        check = test[node.booltesttype]
        return _fold(sql, [self.node(node.arg)], lambda a: check(_boolean(a)), strict=False,
                     kind="bool")  # fmt: skip

    # conditionals

    def _CaseExpr(self, node: ast.CaseExpr, sql: str) -> Compiled:
        subject = self.node(node.arg) if node.arg is not None else None
        whens: list[tuple[Compiled, Compiled]] = []
        for when in _items(node.args):
            cond = self.node(when.expr)
            if subject is not None:
                cond = _fold(f"{subject.sql} = {cond.sql}", [subject, cond],
                             lambda a, b: _compare("=", a, b, False), kind="bool")  # fmt: skip
            whens.append((cond, self.node(when.result)))
        default = (
            self.node(node.defresult) if node.defresult is not None
            else _const("NULL", None, kind="unknown")
        )  # fmt: skip
        results, kind = _unify([r for _, r in whens] + [default])
        default = results[-1]
        # Folding, as eval_const_expressions does: arms in order; an arm whose
        # test is constantly false or NULL is dropped without folding its
        # result; a constantly true one ends the CASE.
        arms: list[tuple[Compiled, Compiled]] = []
        for (cond, _), result in zip(whens, results[:-1], strict=True):
            if cond.error is not None:
                return Compiled(sql, error=cond.error, kind=kind)
            if cond.const:
                if _boolean(cond.value) is not True:
                    continue
                if result.error is not None:
                    return Compiled(sql, error=result.error, kind=kind)
                default = result
                break
            if result.error is not None:
                return Compiled(sql, error=result.error, kind=kind)
            arms.append((cond, result))
        else:
            if default.error is not None:
                return Compiled(sql, error=default.error, kind=kind)
        if not arms:
            return default
        otherwise = default

        def evaluate(row: Row) -> Any:
            for cond, result in arms:
                if _boolean(cond(row)) is True:
                    return result(row)
            return otherwise(row)

        return Compiled(sql, fn=evaluate, kind=kind)

    def _CoalesceExpr(self, node: ast.CoalesceExpr, sql: str) -> Compiled:
        args, kind = _unify([self.node(a) for a in _items(node.args)])
        # Folding: arguments in order; a NULL constant drops out, and a
        # non-NULL constant is the last one reached (later ones never fold).
        kept: list[Compiled] = []
        for arg in args:
            if arg.error is not None:
                return Compiled(sql, error=arg.error, kind=kind)
            if arg.const:
                if arg.value is None:
                    continue
                if not kept:
                    return arg
                kept.append(arg)
                break
            kept.append(arg)
        if not kept:
            return _const(sql, None, kind=kind)

        def evaluate(row: Row) -> Any:
            for arg in kept:
                v = arg(row)
                if v is not None:
                    return v
            return None

        return Compiled(sql, fn=evaluate, kind=kind)

    def _MinMaxExpr(self, node: ast.MinMaxExpr, sql: str) -> Compiled:
        greatest = node.op == enums.MinMaxOp.IS_GREATEST
        args, kind = _unify([self.node(a) for a in _items(node.args)])
        collate = any(a.collate_c for a in args)

        def evaluate(*values: Any) -> Any:
            best = None
            for v in values:
                if v is None:
                    continue
                if best is None or _compare(">" if greatest else "<", v, best, collate):
                    best = v
            return best

        return _fold(sql, args, evaluate, strict=False, kind=kind)

    # functions

    def _FuncCall(self, node: ast.FuncCall, sql: str) -> Compiled:
        name = str(_items(node.funcname)[-1].sval).lower()
        if node.agg_star or node.agg_distinct or node.over is not None or name in _AGGREGATES:
            raise SqlError(f"aggregate or window function {sql} is not allowed here")
        if node.agg_filter is not None or node.agg_order:
            raise SqlError(f"{sql}: FILTER and ORDER BY in a call are not supported")
        args = [self.node(a) for a in _items(node.args)]
        fn = _FUNCTIONS.get((name, len(args)))
        if fn is None:
            raise SqlError(
                f"function {name}() with {len(args)} argument(s) is not supported in a "
                f"maintained view: {sql}"
            )
        return _fold(sql, args, fn, kind=_function_kind(name, [a.kind for a in args]))


_AGGREGATES = frozenset({"count", "sum", "min", "max", "avg"})

_CAST_KINDS = {
    **dict.fromkeys(INT_WIDTH, "int"),
    "numeric": "numeric", "decimal": "numeric", "text": "text", "varchar": "text",
    "bool": "bool", "boolean": "bool", "date": "date", "timestamptz": "timestamptz",
    "timestamp": "timestamp", "float8": "float",
}  # fmt: skip


def _function_kind(name: str, args: list[str | None]) -> str | None:
    if name in ("length", "char_length", "character_length"):
        return "int"
    if name == "abs":
        return args[0] if args[0] != "unknown" else None
    if name == "round":
        if len(args) == 2:
            return "numeric"
        # round(integer) resolves to round(double precision)
        return {"int": "float", "numeric": "numeric", "float": "float"}.get(args[0] or "")
    return None


def _describe(node: ast.Node) -> str:
    return {
        "SubLink": "a subquery",
        "SQLValueFunction": "a function of the current time or session (CURRENT_DATE, ...)",
        "ParamRef": "a parameter",
        "A_ArrayExpr": "an array",
        "RowExpr": "a row constructor",
        "A_Indirection": "subscripting",
    }.get(type(node).__name__, type(node).__name__)


# --- values ---------------------------------------------------------------------

_COMPARISONS = frozenset({"=", "<>", "<", "<=", ">", ">="})
_ARITHMETIC = frozenset({"+", "-", "*", "/", "%"})


def _number(a: Any) -> Any:
    if isinstance(a, bool) or not isinstance(a, int | Decimal | float):
        raise EvalError(f"a number was expected, not {a!r}")
    return a


def _typed(a: Any, what: str) -> Any:
    """An operator or function of one argument cannot type an untyped literal."""
    if isinstance(a, Untyped):
        raise EvalError(f"{what}(unknown) is not unique: cast the literal")
    return a


def _check_int(n: int) -> int:
    if not INT8_MIN <= n <= INT8_MAX:
        raise EvalError("bigint out of range")
    return n


def _negate(a: Any) -> Any:
    a = _number(_typed(a, "-"))
    if isinstance(a, int):
        return _check_int(-a)
    if isinstance(a, Decimal):
        return positive_zero(-a)
    return -a


def _coerce(a: Any, like: Any) -> Any:
    """An untyped literal takes the type of the value it meets."""
    if not isinstance(a, Untyped) or like is None or isinstance(like, str):
        return a
    if isinstance(like, bool):
        return _bool_in(a)
    if isinstance(like, int):
        return _int_in(a, 64)
    if isinstance(like, Decimal):
        return _numeric_in(a)
    if isinstance(like, float):
        return _float_in(a)
    if isinstance(like, dt.datetime):
        raise EvalError(
            f"compare a timestamp with a typed literal (timestamptz '{a}+00'): an untyped "
            "one is read in the session's time zone"
        )
    if isinstance(like, dt.date):
        return _date_in(a)
    raise EvalError(f"cannot compare {type(like).__name__} with the literal '{a}'")


def _pair(a: Any, b: Any) -> tuple[Any, Any]:
    a, b = _coerce(a, b), _coerce(b, a)
    if isinstance(a, float) != isinstance(b, float) and _is_number(a) and _is_number(b):
        return float(a), float(b)  # numeric meets float8: compared as float8
    return a, b


def _is_number(v: Any) -> bool:
    return isinstance(v, int | Decimal | float) and not isinstance(v, bool)


_ORDER: dict[str, Callable[[Any, Any], bool]] = {
    "=": operator.eq, "<>": operator.ne, "<": operator.lt,
    "<=": operator.le, ">": operator.gt, ">=": operator.ge,
}  # fmt: skip


def _compare(op: str, a: Any, b: Any, collate_c: bool) -> bool | None:
    if a is None or b is None:
        return None
    a, b = _pair(a, b)
    _comparable(op, a, b, collate_c)
    try:
        return bool(_ORDER[op](a, b))
    except TypeError as exc:
        raise EvalError(str(exc)) from None


def _comparable(op: str, a: Any, b: Any, collate_c: bool) -> None:
    """Raise unless Python compares `a` and `b` as Postgres would."""
    if isinstance(a, str) and isinstance(b, str):
        if op not in ("=", "<>") and not collate_c:
            raise EvalError(
                'ordering text depends on the collation; compare with COLLATE "C" '
                "(code-point order), which is the order Deltaflow reproduces"
            )
        return
    if _is_number(a) and _is_number(b):
        return
    if type(a) is not type(b) and not (isinstance(a, dt.date) and isinstance(b, dt.date)):
        raise EvalError(f"cannot compare {type(a).__name__} with {type(b).__name__}")
    if isinstance(a, dt.datetime) != isinstance(b, dt.datetime) or (
        isinstance(a, dt.datetime) and (a.tzinfo is None) != (b.tzinfo is None)
    ):
        raise EvalError(
            "comparing dates with timestamps, or timestamps with and without a time zone, "
            "is not supported"
        )


def _distinct(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return (a is None) != (b is None)
    return bool(_compare("<>", a, b, True))


def _nullif(a: Any, b: Any) -> Any:
    return None if _compare("=", a, b, True) is True else a


def _arith(op: str, a: Any, b: Any) -> Any:
    if isinstance(a, Untyped) and isinstance(b, Untyped):
        raise EvalError(f"operator is not unique: unknown {op} unknown")
    a, b = _pair(a, b)
    a, b = _number(a), _number(b)
    if isinstance(a, float) or isinstance(b, float):
        if op == "%":
            raise EvalError("operator does not exist: double precision % double precision")
        if op == "/" and b == 0:
            raise EvalError("division by zero")
        return {"+": a + b, "-": a - b, "*": a * b}.get(op) if op != "/" else a / b
    if isinstance(a, int) and isinstance(b, int):
        if op in ("/", "%"):
            if b == 0:
                raise EvalError("division by zero")
            q = abs(a) // abs(b)
            q = q if (a >= 0) == (b > 0) else -q  # truncation toward zero
            return _check_int(q) if op == "/" else a - b * q
        return _check_int({"+": a + b, "-": a - b, "*": a * b}[op])
    if op == "%":
        raise EvalError("numeric % is not supported")
    if op == "/":
        try:
            return numeric_div(a, b)
        except ZeroDivisionError:
            raise EvalError("division by zero") from None
    x, y = Decimal(a), Decimal(b)
    try:
        result = {"+": EXACT.add, "-": EXACT.subtract, "*": EXACT.multiply}[op](x, y)
    except decimal.DecimalException:
        raise EvalError("value overflows numeric format") from None
    return positive_zero(result)


def _concat(a: Any, b: Any) -> str:
    return _text(a) + _text(b)


def _text(v: Any) -> str:
    """The text form of a value, as Postgres prints it, for the types whose
    form does not depend on session settings."""
    if isinstance(v, str):
        return str(v)
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
        return v.isoformat()
    raise EvalError(
        f"converting {type(v).__name__} to text depends on session settings; not supported"
    )


def _boolean(v: Any) -> bool | None:
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, Untyped):
        return _bool_in(v)
    raise EvalError(f"a boolean was expected, not {v!r}")


def _like(a: Any, pattern: Any) -> bool:
    a, pattern = _coerce(a, ""), _coerce(pattern, "")
    if not isinstance(a, str) or not isinstance(pattern, str):
        raise EvalError("LIKE compares text")
    return _match_text(a, 0, pattern, 0) == _LIKE_TRUE


_LIKE_TRUE, _LIKE_FALSE, _LIKE_ABORT = 1, 0, -1
_ESCAPE_AT_END = "LIKE pattern must not end with escape character"


def _match_text(t: str, ti: int, p: str, pi: int) -> int:  # noqa: PLR0911, PLR0912
    """Postgres's MatchText (like_match.c), statement for statement. A
    regular expression would match the same strings, but Postgres raises on
    a trailing escape only when matching reaches it: '' LIKE '\\' is false."""
    if len(p) - pi == 1 and p[pi] == "%":
        return _LIKE_TRUE
    while ti < len(t) and pi < len(p):
        if p[pi] == "\\":
            pi += 1
            if pi >= len(p):
                raise EvalError(_ESCAPE_AT_END)
            if p[pi] != t[ti]:
                return _LIKE_FALSE
        elif p[pi] == "%":
            pi += 1
            while pi < len(p):
                if p[pi] == "%":
                    pi += 1
                elif p[pi] == "_":
                    if ti >= len(t):
                        return _LIKE_ABORT
                    ti += 1
                    pi += 1
                else:
                    break
            if pi >= len(p):
                return _LIKE_TRUE
            if p[pi] == "\\":
                if len(p) - pi < 2:
                    raise EvalError(_ESCAPE_AT_END)
                first = p[pi + 1]
            else:
                first = p[pi]
            while ti < len(t):
                if t[ti] == first:
                    matched = _match_text(t, ti, p, pi)
                    if matched != _LIKE_FALSE:
                        return matched
                ti += 1
            return _LIKE_ABORT
        elif p[pi] == "_":
            ti += 1
            pi += 1
            continue
        elif p[pi] != t[ti]:
            return _LIKE_FALSE
        ti += 1
        pi += 1
    if ti < len(t):
        return _LIKE_FALSE
    while pi < len(p) and p[pi] == "%":
        pi += 1
    return _LIKE_TRUE if pi >= len(p) else _LIKE_ABORT


# --- functions ------------------------------------------------------------------


def _abs(a: Any) -> Any:
    a = _number(_typed(a, "abs"))
    if isinstance(a, int):
        return _check_int(abs(a))
    return a.copy_abs() if isinstance(a, Decimal) else abs(a)


def _round(a: Any, scale: Any = None) -> Any:
    """round(x) and round(x, s), resolved as Postgres resolves them: for an
    integer, round(x) is round(double precision) (the preferred type of the
    numeric category), and round(x, s) only exists for numeric."""
    a = _number(_typed(a, "round"))
    if scale is None:
        if isinstance(a, Decimal):
            return round_half_away(a, 0)
        return float(round(a))  # rint: half to even
    if isinstance(a, float):
        raise EvalError("function round(double precision, integer) does not exist")
    if isinstance(scale, bool) or not isinstance(scale, int):
        raise EvalError("round() takes an integer number of decimals")
    return round_half_away(a, scale)


def _length(a: Any) -> int:
    if not isinstance(a, str):
        raise EvalError("length() takes text")
    return len(a)


_FUNCTIONS: dict[tuple[str, int], Callable[..., Any]] = {
    ("abs", 1): _abs,
    ("round", 1): _round,
    ("round", 2): _round,
    ("length", 1): _length,
    ("char_length", 1): _length,
    ("character_length", 1): _length,
}


# --- input functions and casts ----------------------------------------------------------


def _int_in(text: str, bits: int) -> int:
    if not re.fullmatch(r"\s*[+-]?\d+\s*", text):
        raise EvalError(f'invalid input syntax for type integer: "{text}"')
    return _int_range(int(text), bits)


def _int_range(n: int, bits: int) -> int:
    if not -(2 ** (bits - 1)) <= n < 2 ** (bits - 1):
        raise EvalError({16: "smallint", 32: "integer", 64: "bigint"}[bits] + " out of range")
    return n


def _numeric_in(text: str) -> Decimal:
    if not re.fullmatch(r"\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*", text):
        raise EvalError(f'invalid input syntax for type numeric: "{text}"')
    value = Decimal(text.strip())
    if value.as_tuple().exponent > 0:  # type: ignore[operator]
        value = value.quantize(Decimal(1), context=EXACT)  # 1e3 is 1000, scale 0
    return positive_zero(value)


def _float_in(text: str) -> float:
    if not re.fullmatch(r"\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*", text):
        raise EvalError(f'invalid input syntax for type double precision: "{text}"')
    return float(text)


def _bool_in(text: str) -> bool:
    """Postgres's boolin: true/false/yes/no and prefixes, on/off, 1/0."""
    t = text.strip().lower()
    if t in ("1", "0"):
        return t == "1"
    if t in ("on", "off"):
        return t == "on"
    for word, value in (("true", True), ("false", False), ("yes", True), ("no", False)):
        if t and word.startswith(t):
            return value
    raise EvalError(f'invalid input syntax for type boolean: "{text}"')


def _date_in(text: str) -> dt.date:
    if not re.fullmatch(r"\s*\d{4}-\d{2}-\d{2}\s*", text):
        raise EvalError(f"date literal {text!r}: only ISO YYYY-MM-DD is supported")
    try:
        return dt.date.fromisoformat(text.strip())
    except ValueError:
        raise EvalError(f'date/time field value out of range: "{text}"') from None


def _timestamp_in(text: str, *, zoned: bool) -> dt.datetime:
    t = text.strip().replace(" ", "T", 1)
    try:
        value = dt.datetime.fromisoformat(t)
    except ValueError:
        raise EvalError(f"timestamp literal {text!r}: only ISO 8601 is supported") from None
    if zoned and value.tzinfo is None:
        raise EvalError(
            f"timestamptz literal {text!r} needs an explicit offset (+00): without one it is "
            "read in the session's time zone"
        )
    if not zoned and value.tzinfo is not None:
        raise EvalError(f"timestamp literal {text!r} has an offset, which Postgres would ignore")
    return value


def _type_name(type_name: ast.TypeName) -> str:
    if type_name.arrayBounds:
        raise SqlError("array types are not supported")
    return str(_items(type_name.names)[-1].sval).lower()


def _cast_function(name: str, type_name: ast.TypeName, sql: str) -> Callable[[Any], Any]:  # noqa: PLR0911
    if name in INT_WIDTH:
        bits = INT_WIDTH[name]
        return lambda v: _to_int(v, bits)
    if name in ("numeric", "decimal"):
        typmods = [int(m.val.ival) for m in _items(type_name.typmods)]
        if len(typmods) > 2:
            raise SqlError(f"{sql}: numeric takes at most precision and scale")
        return lambda v: _to_numeric(v, *typmods)
    if name in ("text", "varchar") and not type_name.typmods:
        return _to_text
    if name in ("bool", "boolean"):
        return _to_bool
    if name == "date":
        return _to_date
    if name in ("timestamptz", "timestamp"):
        zoned = name == "timestamptz"
        return lambda v: _to_timestamp(v, zoned)
    if name in ("float8",):
        return _to_float
    raise SqlError(f"{sql}: a cast to {name} is not supported")


def _to_int(v: Any, bits: int) -> int:
    if isinstance(v, bool):
        if bits != 32:
            raise EvalError("only boolean to integer (int4) is a cast")
        return int(v)
    if isinstance(v, int):
        return _int_range(v, bits)
    if isinstance(v, Decimal):
        return _int_range(int(round_half_away(v, 0)), bits)
    if isinstance(v, float):
        return _int_range(round(v), bits)  # rint: half to even
    if isinstance(v, str):
        return _int_in(v, bits)
    raise EvalError(f"cannot cast {type(v).__name__} to integer")


def _to_numeric(v: Any, precision: int | None = None, scale: int = 0) -> Decimal:
    if isinstance(v, bool):
        raise EvalError("cannot cast boolean to numeric")
    if isinstance(v, int):
        value = Decimal(v)
    elif isinstance(v, Decimal):
        value = v
    elif isinstance(v, str):
        value = _numeric_in(v)
    elif isinstance(v, float):
        # float8_numeric: printed with DBL_DIG (15) significant digits, then read
        if math.isnan(v) or math.isinf(v):
            raise EvalError("casting NaN or infinity to numeric is not supported")
        value = _numeric_in(format(v, ".15g"))
    else:
        raise EvalError(f"a cast from {type(v).__name__} to numeric is not supported")
    if precision is None:
        return value
    value = round_half_away(value, scale)
    if value != 0 and value.copy_abs().adjusted() + 1 > precision - scale:
        raise EvalError("numeric field overflow")
    return value


def _to_text(v: Any) -> str:
    return _text(v)


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v != 0
    if isinstance(v, str):
        return _bool_in(v)
    raise EvalError(f"cannot cast {type(v).__name__} to boolean")


def _to_date(v: Any) -> dt.date:
    if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
        return v
    if isinstance(v, str):
        return _date_in(v)
    raise EvalError(f"a cast from {type(v).__name__} to date is not supported")


def _to_timestamp(v: Any, zoned: bool) -> dt.datetime:
    if isinstance(v, dt.datetime) and (v.tzinfo is not None) == zoned:
        return v
    if isinstance(v, str):
        return _timestamp_in(v, zoned=zoned)
    raise EvalError(f"a cast from {type(v).__name__} to timestamp is not supported")


def _to_float(v: Any) -> float:
    if isinstance(v, bool):
        raise EvalError("cannot cast boolean to double precision")
    if isinstance(v, int | Decimal | float):
        return float(v)
    if isinstance(v, str):
        return _float_in(v)
    raise EvalError(f"cannot cast {type(v).__name__} to double precision")
