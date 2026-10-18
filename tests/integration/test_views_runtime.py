"""Views against a real Postgres: source validation, snapshot hand-off, and
one view result checked against SQL after each kind of change."""

from __future__ import annotations

import asyncio
import hashlib
import random
import string
from collections.abc import Callable, Mapping
from typing import Any

import psycopg
import pytest

from deltaflow.aio import run
from deltaflow.core import Frontier, Row
from deltaflow.devtools.seed import seed_database
from deltaflow.planner import Deltaflow
from deltaflow.runtime import Runner, UnsupportedSourceError, describe_table
from deltaflow.sinks import MemorySink
from deltaflow.state import TableSchema
from tests.sql_oracle import first_difference, query_multiset

type Sql = psycopg.AsyncConnection[tuple[Any, ...]]

BIG = "".join(random.Random(1).choices(string.ascii_letters, k=20_000))


def big_order(r: Row) -> bool:
    return bool(r["amount"] > 1000)


def bio_digest(r: Row) -> Mapping[str, Any]:
    bio = r["bio"]
    return {
        "id": r["id"],
        "name": r["name"],
        "bio_md5": None if bio is None else hashlib.md5(bio.encode()).hexdigest(),
    }


def order_with_customer(r: Row) -> Mapping[str, Any]:
    bio = r["customers.bio"]
    return {
        "id": r["orders.id"],
        "amount": r["amount"],
        "country": r["country"],
        "bio_md5": None if bio is None else hashlib.md5(bio.encode()).hexdigest(),
    }


VIEWS: dict[str, tuple[Callable[[Deltaflow, MemorySink], object], str]] = {
    "orders_with_customer": (
        lambda df, s: (
            df.view("orders_with_customer")
            .source("public.orders")
            .join("public.customers", on=("customer_id", "id"))
            .map(order_with_customer)
            .sink(s)
        ),
        "SELECT o.id, o.amount, c.country, md5(c.bio) AS bio_md5"
        " FROM orders o JOIN customers c ON c.id = o.customer_id",
    ),
    "big_orders": (
        lambda df, s: (
            df.view("big_orders")
            .source("public.orders")
            .filter(big_order)
            .select("id", "customer_id", "amount")
            .sink(s)
        ),
        "SELECT id, customer_id, amount FROM orders WHERE amount > 1000",
    ),
    "customer_bios": (
        lambda df, s: df.view("customer_bios").source("public.customers").map(bio_digest).sink(s),
        "SELECT id, name, md5(bio) AS bio_md5 FROM customers",
    ),
}


async def assert_views_match(sql: Sql, sinks: dict[str, MemorySink], frontier: Frontier) -> None:
    for name, (_, query) in VIEWS.items():
        expected = await query_multiset(sql, query)
        sink = sinks[name]
        assert sink.frontier == frontier
        assert sink.result == expected, f"{name}: {first_difference(sink.result, expected)}"


async def _each_change_kind(dsn: str, *, full_identity: bool) -> None:
    await seed_database(dsn, seed=3, customers=6, orders=30)
    df = Deltaflow()
    sinks = {name: MemorySink() for name in VIEWS}
    for name, (register, _) in VIEWS.items():
        register(df, sinks[name])
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
        if full_identity:
            await sql.execute("ALTER TABLE customers REPLICA IDENTITY FULL")
            await sql.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        await sql.execute("UPDATE customers SET bio = %s WHERE id = 2", (BIG,))
        async with Runner(df, dsn, publication="deltaflow_it_views", status_interval=0.5) as runner:
            await assert_views_match(sql, sinks, runner.engine.frontier)
            statements = [
                ("INSERT INTO orders VALUES (5000, 1, 'paid', 4321.00, NULL, now())", ()),
                ("UPDATE orders SET amount = amount + 2000 WHERE id = 5000", ()),
                ("UPDATE orders SET customer_id = 2 WHERE id = 5000", ()),  # join key moves
                ("UPDATE customers SET country = 'JP' WHERE id = 2", ()),  # fans out to orders
                # Leaves the out-of-line bio of customer 2 untouched: the
                # stream carries the placeholder, the view must keep the bio.
                ("UPDATE customers SET name = 'Renamed' WHERE id = 2", ()),
                ("UPDATE customers SET bio = %s WHERE id = 3", (BIG[::-1],)),
                ("UPDATE orders SET id = 6000 WHERE id = 5000", ()),
                ("DELETE FROM orders WHERE id = 6000", ()),
                ("DELETE FROM customers WHERE id = 1", ()),  # cascades to its orders
                ("TRUNCATE orders", ()),
            ]
            for stmt, params in statements:
                await sql.execute(stmt, params)
                async with asyncio.timeout(10):
                    txn = await runner.next_transaction()
                await assert_views_match(sql, sinks, Frontier.at(txn.end_lsn))
            resolved = runner.engine.arrangements["public.customers"].counters
            assert resolved["unchanged_toast_resolved"] >= 1


def test_views_track_each_kind_of_change(dsn: str) -> None:
    run(_each_change_kind(dsn, full_identity=False))


def test_views_track_each_kind_of_change_with_replica_identity_full(dsn: str) -> None:
    # FULL sends the whole old row; the arrangement cross-checks it against
    # its own copy instead of trusting either blindly.
    run(_each_change_kind(dsn, full_identity=True))


SCRATCH_DDL = """
DROP SCHEMA IF EXISTS m2src CASCADE;
CREATE SCHEMA m2src;
CREATE TABLE m2src.plain (
    id bigint PRIMARY KEY, a int, gone int, g int GENERATED ALWAYS AS (a * 2) STORED
);
ALTER TABLE m2src.plain DROP COLUMN gone;
CREATE TABLE m2src.composite (a int, b int, v text, PRIMARY KEY (b, a));
CREATE TABLE m2src.nopk (a int);
CREATE TABLE m2src.full_pk (id int PRIMARY KEY, v text);
ALTER TABLE m2src.full_pk REPLICA IDENTITY FULL;
CREATE TABLE m2src.full_nopk (v text);
ALTER TABLE m2src.full_nopk REPLICA IDENTITY FULL;
CREATE TABLE m2src.by_index (id int PRIMARY KEY, code text NOT NULL, v text);
CREATE UNIQUE INDEX by_index_code ON m2src.by_index (code);
ALTER TABLE m2src.by_index REPLICA IDENTITY USING INDEX by_index_code;
CREATE TABLE m2src.nothing (id int PRIMARY KEY);
ALTER TABLE m2src.nothing REPLICA IDENTITY NOTHING;
CREATE VIEW m2src.a_view AS SELECT * FROM m2src.plain;
"""


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        # Dropped and generated columns are not sent by pgoutput, so they are
        # not part of the arrangement's schema either.
        ("m2src.plain", TableSchema("m2src.plain", ("id", "a"), ("id",))),
        ("m2src.composite", TableSchema("m2src.composite", ("a", "b", "v"), ("b", "a"))),
        ("m2src.full_pk", TableSchema("m2src.full_pk", ("id", "v"), ("id",))),
        ("m2src.by_index", TableSchema("m2src.by_index", ("id", "code", "v"), ("code",))),
    ],
)
def test_describe_supported_sources(dsn: str, table: str, expected: TableSchema) -> None:
    async def go() -> TableSchema:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(SCRATCH_DDL)
            return await describe_table(sql, table)

    assert run(go()) == expected


@pytest.mark.parametrize(
    ("table", "message"),
    [
        ("m2src.nopk", "no primary key"),
        ("m2src.full_nopk", "REPLICA IDENTITY FULL but no primary key"),
        ("m2src.nothing", "REPLICA IDENTITY NOTHING"),
        ("m2src.a_view", "not an ordinary table"),
        ("m2src.missing", "does not exist"),
    ],
)
def test_unsupported_sources_are_rejected_before_streaming(
    dsn: str, table: str, message: str
) -> None:
    async def go() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute(SCRATCH_DDL)
            await describe_table(sql, table)

    with pytest.raises(UnsupportedSourceError, match=message):
        run(go())


@pytest.mark.parametrize(
    ("ddl", "message"),
    [
        ("CREATE PUBLICATION m2pub FOR TABLE customers", "does not include public.orders"),
        (
            "CREATE PUBLICATION m2pub FOR TABLE orders WITH (publish = 'insert, update')",
            "does not publish every operation",
        ),
        ("CREATE PUBLICATION m2pub FOR TABLE orders WHERE (amount > 5)", "row filter"),
        ("CREATE PUBLICATION m2pub FOR TABLE orders (id, amount)", "only some columns"),
    ],
)
def test_publications_that_would_drop_changes_are_rejected(
    dsn: str, ddl: str, message: str
) -> None:
    async def go() -> None:
        await seed_database(dsn, seed=1, customers=2, orders=2)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sql:
            await sql.execute("DROP PUBLICATION IF EXISTS m2pub")
            await sql.execute(ddl)
        df = Deltaflow()
        df.view("v").source("public.orders").sink(MemorySink())
        runner = Runner(df, dsn, publication="m2pub", manage_publication=False)
        try:
            await runner.start()
        finally:
            await runner.close()

    with pytest.raises(UnsupportedSourceError, match=message):
        run(go())
