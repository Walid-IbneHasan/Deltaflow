"""json and jsonb exactly as Postgres has them, with Postgres as the oracle:
which texts it accepts, jsonb's canonical text, and ->, ->>, #>, #>> on
both types, over random documents (duplicate keys, escapes, unicode,
numbers in every form, random whitespace)."""

from __future__ import annotations

import contextlib
import json
import random
from typing import Any

import psycopg
import pytest

from deltaflow.core.jsontext import JsonError, jsonb_in, parse
from deltaflow.core.values import Json, Jsonb, get, get_path, get_path_text, get_text

KEYS = ["a", "b", "aa", "é", "", "a b", 'q"', "0"]
NUMBERS = [
    "0",
    "-0",
    "1",
    "-12",
    "1.50",
    "0.001",
    "1e3",
    "-2.5E-2",
    "1E+2",
    "12345678901234567890.1",
]
STRINGS = [
    "",
    "x",
    "é€",
    "tab\\tnew\\nline",
    'q\\"',
    "\\u00e9",
    "\\ud83d\\ude00",
    "back\\\\slash",
    "\\/",
    # json keeps these; de-escaping them (->>, a cast to jsonb) fails
    "\\ud800",
    "lone \\udc00 low",
    "\\u0000",
]


def document(rng: random.Random, depth: int = 0) -> str:
    """A random JSON text, with random whitespace and duplicate keys."""

    def ws() -> str:
        return rng.choice(["", "", " ", "  ", "\n", "\t "])

    roll = rng.random()
    if depth < 3 and roll < 0.3:
        pairs = [
            f'{ws()}"{rng.choice(KEYS).replace(chr(34), chr(92) + chr(34))}"{ws()}:{ws()}'
            f"{document(rng, depth + 1)}{ws()}"
            for _ in range(rng.randint(0, 4))
        ]
        return "{" + ",".join(pairs) + "}"
    if depth < 3 and roll < 0.55:
        return (
            "["
            + ",".join(f"{ws()}{document(rng, depth + 1)}{ws()}" for _ in range(rng.randint(0, 4)))
            + "]"
        )
    if roll < 0.75:
        return f'"{rng.choice(STRINGS)}"'
    if roll < 0.9:
        return rng.choice(NUMBERS)
    return rng.choice(["true", "false", "null"])


def ours(fn: Any, *args: Any) -> tuple[str, Any]:
    try:
        value = fn(*args)
    except JsonError:
        return ("error", None)
    if isinstance(value, Json | Jsonb):
        value = value.text
    return ("ok", value)


def theirs(
    conn: psycopg.Connection[tuple[Any, ...]], query: str, params: tuple[Any, ...]
) -> tuple[str, Any]:
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
    except psycopg.DataError:
        return ("error", None)
    assert row is not None
    return ("ok", row[0])


@pytest.mark.parametrize("seed", range(4))
def test_operators_match_postgres(dsn: str, seed: int) -> None:
    rng = random.Random(seed)
    mismatches = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        for _ in range(250):
            text = document(rng)
            key = rng.choice(KEYS)
            index = rng.randint(-3, 3)
            path = [rng.choice([*KEYS, "0", "1", "-1", "x"]) for _ in range(rng.randint(0, 3))]
            docs: dict[str, Json | Jsonb] = {"json": Json(text)}
            checks: list[tuple[str, tuple[str, Any], str, tuple[Any, ...]]] = [
                ("jsonb text", ours(jsonb_in, text), "SELECT %s::jsonb::text", (text,))
            ]
            # Not every json is a jsonb; "jsonb text" checks Postgres agrees.
            with contextlib.suppress(JsonError):
                docs["jsonb"] = Jsonb(jsonb_in(text))
            for kind, doc in docs.items():
                for op, fn, arg, cast in (
                    ("-> key", get, key, ""),
                    ("->> key", get_text, key, ""),
                    ("-> int", get, index, "::int"),
                    ("->> int", get_text, index, "::int"),
                    ("#> path", get_path, path, "::text[]"),
                    ("#>> path", get_path_text, path, "::text[]"),
                ):
                    sql_op = op.split()[0]
                    query = f"SELECT (%s::{kind} {sql_op} %s{cast})::text"
                    checks.append((f"{kind} {op}", ours(fn, doc, arg), query, (text, arg)))
            for name, mine, query, params in checks:
                truth = theirs(conn, query, params)
                if mine != truth:
                    mismatches.append(f"{name} {params!r}: ours {mine!r}, Postgres {truth!r}")
    assert mismatches == [], "\n".join(mismatches[:20])


TRICKY = [
    "",
    " ",
    "01",
    "-",
    "-0",
    "1.",
    ".5",
    "1e",
    "1e+",
    "+1",
    "1x",
    "0x10",
    "1.5e3",
    "tru",
    "true1",
    "nul",
    '"',
    '"\\x"',
    '"\\u12"',
    '"\\u0000"',
    '"\\ud800"',
    '"\\udc00"',
    '"\\ud800\\u0041"',
    '"a\tb"',
    '{"a"}',
    '{"a":}',
    "{,}",
    "[1,]",
    "[,1]",
    "{} {}",
    '{"a":1,}',
    "  [1 , 2 ]  ",
    '{"a":1 "b":2}',
    "NaN",
    "Infinity",
    "[[[[]]]]",
    '"\\u00e9"',
    '"\\/"',
]


def test_what_postgres_accepts_and_rejects(dsn: str) -> None:
    mismatches = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        for text in TRICKY:
            for kind, mine in (
                ("json", ours(parse, text)),
                ("jsonb", ours(jsonb_in, text)),
            ):
                truth = theirs(conn, f"SELECT %s::{kind}::text", (text,))
                got = mine[0]
                if kind == "jsonb" and got == "ok":
                    got = "ok" if mine[1] == truth[1] else f"text {mine[1]!r}"
                if got != truth[0]:
                    mismatches.append(f"{kind} {text!r}: ours {mine!r}, Postgres {truth!r}")
    assert mismatches == [], "\n".join(mismatches)


def test_jsonb_equality_is_postgres_equality(dsn: str) -> None:
    pairs = [("1.0", "1"), ('{"a":1,"b":2}', '{"b":2,"a":1}'), ("[1,2]", "[2,1]"),
             ('{"a":1,"a":2}', '{"a":2}'), ('"1"', "1"), ("0", "-0"), ("1e2", "100.0")]  # fmt: skip
    with psycopg.connect(dsn, autocommit=True) as conn:
        for left, right in pairs:
            ours_equal = Jsonb(jsonb_in(left)) == Jsonb(jsonb_in(right))
            with conn.cursor() as cur:
                cur.execute("SELECT %s::jsonb = %s::jsonb", (left, right))
                row = cur.fetchone()
            assert row is not None
            assert ours_equal == row[0], (left, right)
            if ours_equal:
                assert hash(Jsonb(jsonb_in(left))) == hash(Jsonb(jsonb_in(right)))
    assert json.loads("1") == 1  # the json module is only used to keep imports honest
