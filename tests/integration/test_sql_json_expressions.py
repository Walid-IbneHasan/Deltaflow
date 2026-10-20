"""json, jsonb and array expressions in SQL views evaluate exactly as
Postgres evaluates them: the same value (typed the same), NULL where
Postgres gives NULL, an error where it raises.

Rows live in a temporary table, so Postgres cannot fold them into the
expression as constants."""

from __future__ import annotations

import random
from typing import Any

import psycopg
import pytest

from deltaflow.core import Row
from deltaflow.core.jsontext import JsonError, jsonb_in
from deltaflow.core.values import to_array
from deltaflow.planner.sqlexpr import EvalError, expression
from deltaflow.replication.changes import register_value_loaders
from tests.integration.test_json_values import document

KINDS = {"id": "int", "d": "jsonb", "j": "json", "a": "int[]", "t": "text[]", "g": "int[]"}
EXPRESSIONS = [
    "d -> 'kind'", "d ->> 'kind'", "d -> 0", "d ->> -1", "d -> 'body' -> 0",
    "d #> '{body,a}'", "d #>> '{body,0}'", "d #> '{}'", "d #>> '{body,-1}'",
    "j -> 'kind'", "j ->> 'kind'", "j -> 1", "j #>> '{body,b}'",
    "d ? 'kind'", "d ? 'a'", "(d -> 'body') ? 'x'",
    "d = '{\"kind\": \"a\"}'::jsonb", "d -> 'kind' = '1'::jsonb", "d <> d",
    "(d ->> 'kind')::int + 1", "(d ->> 'kind')::numeric", "d::text", "j::text", "j::jsonb",
    "(d -> 'kind')::text", "COALESCE(d ->> 'kind', 'none')", "d ->> 'kind' = 'a'",
    "('{\"z\": [1, 2.50]}'::jsonb -> 'z') -> 1", "'[1,2'::jsonb", "'{\"a\":1}'::json -> 'a'",
    "a[1]", "a[2] + 1", "a[0]", "a[NULL]", "g[1][2]", "g[1]", "g[2][1]", "t[1] || '!'",
    "cardinality(a)", "cardinality(g)", "array_length(g, 1)", "array_length(g, 2)",
    "array_length(a, 2)", "1 = ANY(a)", "NULL = ANY(a)", "-2 = ANY(a)", "2 > ALL(a)",
    "0 <> ALL(a)", "'x' = ANY(t)", "'b c' = ANY(t)", "a = a", "t = t",
]  # fmt: skip


def rows(rng: random.Random, n: int) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = []
    while len(out) < n:
        body = document(rng)
        kind = rng.choice(['"a"', "1", "1.0", '"12"', "null", "[1]", '{"x": 1}'])
        text = rng.choice([f'{{"kind": {kind}, "body": {body}}}', body, kind])
        try:
            jsonb_in(text)
        except JsonError:
            continue
        a = [rng.choice([1, -2, 3, None]) for _ in range(rng.randint(0, 3))]
        t = [rng.choice(["x", "b c", "", None]) for _ in range(rng.randint(0, 3))]
        width = rng.randint(1, 3)
        g = [[rng.randint(0, 5) for _ in range(width)] for _ in range(rng.randint(1, 2))]
        nulls = rng.random() < 0.1
        out.append((len(out), None if nulls else text, document(rng), a, t, g))
    return out


def theirs(conn: psycopg.Connection[tuple[Any, ...]], expr: str, rid: int) -> tuple[str, Any]:
    try:
        with conn.transaction(), conn.cursor(binary=True) as cur:
            cur.execute(f"SELECT {expr} FROM pg_temp.it_json_rows WHERE id = %s", (rid,))
            row = cur.fetchone()
    except psycopg.DataError:
        return ("error", None)
    except psycopg.errors.UndefinedFunction:
        return ("error", None)
    assert row is not None
    return ("ok", to_array(row[0]))


def ours(fn: Any, row: Row) -> tuple[str, Any]:
    try:
        value = fn(row)
    except EvalError:
        return ("error", None)
    return ("ok", None if value is None else value)


def _raises(exc: EvalError) -> Any:
    def fn(row: Row) -> Any:
        raise exc

    return fn


@pytest.mark.parametrize("seed", range(3))
def test_json_and_array_expressions_match_postgres(dsn: str, seed: int) -> None:
    rng = random.Random(seed)
    data = rows(rng, 60)
    mismatches = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        register_value_loaders(conn.adapters)
        conn.execute(
            "CREATE TEMP TABLE it_json_rows (id int, d jsonb, j json, a int[], t text[], g int[][])"
        )
        for r in data:
            conn.execute(
                "INSERT INTO pg_temp.it_json_rows VALUES (%s, %s::jsonb, %s::json, %s, %s, %s)", r
            )
        with conn.cursor(binary=True) as cur:
            cur.execute("SELECT * FROM pg_temp.it_json_rows ORDER BY id")
            assert cur.description is not None
            names = [c.name for c in cur.description]
            engine_rows = [Row(zip(names, map(to_array, rec), strict=True)) for rec in cur]
        for expr in EXPRESSIONS:
            try:
                fn = expression(expr, KINDS)
            except EvalError as exc:  # raised while folding, as Postgres's planner does
                fn = _raises(exc)
            for row in engine_rows:
                mine, truth = ours(fn, row), theirs(conn, expr, row["id"])
                if mine != truth:
                    mismatches.append(f"{expr} on {dict(row)!r:.200}: ours {mine!r}, PG {truth!r}")
    assert mismatches == [], "\n".join(mismatches[:25])
