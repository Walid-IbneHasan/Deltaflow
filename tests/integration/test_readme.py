"""The README's walkthrough, run as a reader would: its tables, its views
file, its changes, and the results it shows. Each snippet below must
appear in README.md as written, so the two cannot drift apart."""

from __future__ import annotations

import subprocess
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from psycopg.conninfo import make_conninfo

from tests.integration.test_cli import ROOT, deltaflow, rows, spawn, wait_for, wait_inactive

README = (ROOT / "README.md").read_text(encoding="utf-8")

TABLES = """\
CREATE TABLE customers (id int PRIMARY KEY, name text, country text);
CREATE TABLE orders (id int PRIMARY KEY, customer_id int, amount numeric, status text);

INSERT INTO customers VALUES (1, 'Anna', 'NL'), (2, 'Ben', 'DE'), (3, 'Chloe', 'NL');
INSERT INTO orders VALUES
  (1, 1, 50, 'paid'),
  (2, 2, 20, 'paid'),
  (3, 3, 30, 'paid'),
  (4, 1, 10, 'cancelled');
"""

VIEWS = """\
CREATE VIEW analytics.revenue_by_country AS
SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue
FROM orders o JOIN customers c ON c.id = o.customer_id
WHERE o.status <> 'cancelled'
GROUP BY c.country;
"""

RUN = "deltaflow run views.sql --slot shop"
QUERY = "SELECT * FROM analytics.revenue_by_country ORDER BY country;"

CHANGES = """\
INSERT INTO customers VALUES (4, 'Dana', 'BE');
INSERT INTO orders VALUES (5, 4, 70, 'paid');
UPDATE orders SET status = 'cancelled' WHERE id = 1;
"""

# The rows, and how the README shows psql printing them.
BEFORE = [("DE", 1, Decimal(20)), ("NL", 2, Decimal(80))]
BEFORE_SHOWN = """\
 DE      |      1 |      20
 NL      |      2 |      80
"""
AFTER = [("BE", 1, Decimal(70)), ("DE", 1, Decimal(20)), ("NL", 1, Decimal(30))]
AFTER_SHOWN = """\
 BE      |      1 |      70
 DE      |      1 |      20
 NL      |      1 |      30
"""

DATABASE = "readme_try_it"


def wait_rows(
    dsn: str, expected: list[tuple[Any, ...]], proc: subprocess.Popen[bytes], timeout: float = 30
) -> None:
    deadline = time.monotonic() + timeout
    while (found := rows(dsn, QUERY)) != expected:
        assert proc.poll() is None, f"deltaflow exited with {proc.returncode}"
        assert time.monotonic() < deadline, f"{found} never became {expected}"
        time.sleep(0.1)


def test_the_readme_walkthrough(dsn: str, tmp_path: Path) -> None:
    for snippet in (TABLES, VIEWS, RUN, QUERY, CHANGES, BEFORE_SHOWN, AFTER_SHOWN):
        assert snippet in README, f"README.md no longer shows:\n{snippet}"
    # A database of its own, as in the walkthrough; dropping it drops a
    # slot left over from an earlier run.
    with psycopg.connect(dsn, autocommit=True) as sql:
        sql.execute(f"DROP DATABASE IF EXISTS {DATABASE} WITH (FORCE)")
        sql.execute(f"CREATE DATABASE {DATABASE}")
    shop = make_conninfo(dsn, dbname=DATABASE)
    with psycopg.connect(shop, autocommit=True) as sql:
        sql.execute(TABLES)
    views = tmp_path / "views.sql"
    views.write_text(VIEWS, encoding="utf-8")
    state = tmp_path / "state"
    log = tmp_path / "worker.log"
    proc = spawn(
        log, "run", str(views), "--slot", "shop", "--dsn", shop, "--state-dir", str(state),
        "--http-port", "-1", "--log-format", "text",
    )  # fmt: skip
    try:
        wait_for(log, r"worker pid=\d+ (bootstrapped);", proc)
        wait_rows(shop, BEFORE, proc)
        with psycopg.connect(shop, autocommit=True) as sql:
            sql.execute(CHANGES)
        wait_rows(shop, AFTER, proc)
    finally:
        proc.terminate()
        proc.wait(timeout=30)
    wait_inactive(shop, "shop")
    reset = deltaflow("reset", "--dsn", shop, "--slot", "shop", "--state-dir", str(state))
    assert reset.returncode == 0, reset.stderr
