# Deltaflow

[![CI](https://github.com/Walid-IbneHasan/Deltaflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Walid-IbneHasan/Deltaflow/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/deltaflow-pg)](https://pypi.org/project/deltaflow-pg/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

**Deltaflow keeps the results of your SQL queries up to date, live, inside
PostgreSQL.** You write a view in SQL. Deltaflow keeps a real table with
its result, and updates it within a fraction of a second of every change,
without running the query again.

```sh
pip install deltaflow-pg
```

## Why?

Say a shop's database has millions of orders, and a dashboard shows the
revenue per country:

```sql
SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue
FROM orders o JOIN customers c ON c.id = o.customer_id
WHERE o.status <> 'cancelled'
GROUP BY c.country;
```

The usual ways to serve it both have a cost:

- **Run the query each time** the dashboard opens. It reads every order,
  so it gets slower as the shop grows.
- **Store the result** (a materialized view) and refresh it every few
  minutes. In between it is out of date, and every refresh still reads
  every order.

Deltaflow does something else. PostgreSQL already writes every change to a
log, which it uses for backups and replicas. Deltaflow reads that log as it
is written and updates only what each change touches. A new €50 order from
the Netherlands adds €50 to the Netherlands row. Cancelling an order takes
its amount away again. The result is an ordinary table that is always
current, and the work for each change stays small, however big the tables
grow.

It is like a scoreboard operator who adds a point when a goal is scored,
instead of rewatching the whole match to count the goals.

## How it fits together

```
 your app ---- writes ----> PostgreSQL ---- change log ----> Deltaflow
                                ^                                |
                                +-- updates the result tables ---+

 your dashboard ---- reads the result tables ----> PostgreSQL
```

- **Deltaflow is a program that runs next to your database.** Nothing is
  installed inside PostgreSQL.
- **The results are normal tables.** Read them with any tool or language.
- **It survives crashes.** Stopped or killed at any moment, it carries on
  where it left off, and no change is lost or counted twice.

## Try it in five minutes

You need [Docker](https://www.docker.com/) for a throwaway PostgreSQL, and
Python 3.12 or newer.

**1. Start PostgreSQL** with its change log switched on (`wal_level=logical`):

```sh
docker run -d --name shop-db -e POSTGRES_PASSWORD=secret -p 5432:5432 postgres:17 -c wal_level=logical
```

If port 5432 is already in use, write `-p 5433:5432`, and `5433` instead of
`5432` in the commands below.

**2. Create two tables with some data.** Open a SQL prompt:

```sh
docker exec -it shop-db psql -U postgres
```

and paste:

```sql
CREATE TABLE customers (id int PRIMARY KEY, name text, country text);
CREATE TABLE orders (id int PRIMARY KEY, customer_id int, amount numeric, status text);

INSERT INTO customers VALUES (1, 'Anna', 'NL'), (2, 'Ben', 'DE'), (3, 'Chloe', 'NL');
INSERT INTO orders VALUES
  (1, 1, 50, 'paid'),
  (2, 2, 20, 'paid'),
  (3, 3, 30, 'paid'),
  (4, 1, 10, 'cancelled');
```

Keep this prompt open.

**3. Write the result you want** in a file called `views.sql`:

```sql
CREATE VIEW analytics.revenue_by_country AS
SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue
FROM orders o JOIN customers c ON c.id = o.customer_id
WHERE o.status <> 'cancelled'
GROUP BY c.country;
```

**4. Install and start Deltaflow** in a second terminal, in the folder with
`views.sql`:

```sh
pip install deltaflow-pg
deltaflow run views.sql --slot shop --dsn postgresql://postgres:secret@localhost:5432/postgres --log-format text
```

Deltaflow asks PostgreSQL to check the view, creates the table
`analytics.revenue_by_country`, fills it from the rows already there, and
then follows every change. Leave it running. (`--slot shop` names the
bookmark Deltaflow keeps in PostgreSQL's change log.)

**5. Read the result** at the SQL prompt:

```sql
SELECT * FROM analytics.revenue_by_country ORDER BY country;
```

```
 country | orders | revenue
---------+--------+---------
 DE      |      1 |      20
 NL      |      2 |      80
(2 rows)
```

The cancelled €10 order is not counted.

**6. Change the data:**

```sql
INSERT INTO customers VALUES (4, 'Dana', 'BE');
INSERT INTO orders VALUES (5, 4, 70, 'paid');
UPDATE orders SET status = 'cancelled' WHERE id = 1;
```

and read the result again:

```
 country | orders | revenue
---------+--------+---------
 BE      |      1 |      70
 DE      |      1 |      20
 NL      |      1 |      30
(3 rows)
```

Belgium appeared with its first order, and Anna's €50 order left the
Netherlands' total when it was cancelled. Deltaflow did not run the query
again: it applied the three changes, a fraction of a second after each.

**7. Clean up.** Stop Deltaflow with Ctrl+C, then:

```sh
deltaflow reset --slot shop --dsn postgresql://postgres:secret@localhost:5432/postgres
docker rm -f shop-db
```

`reset` removes Deltaflow's bookmark from PostgreSQL and its working files
from `.deltaflow/shop`. Run it whenever you stop using Deltaflow for good:
while the bookmark exists, PostgreSQL keeps every change Deltaflow has not
read yet, and that can fill the disk.

## Use it with your own database

**What PostgreSQL needs**

- **Version** 15, 16, 17 or 18.
- **The change log switched on:** `wal_level = logical`. On your own
  server, run `ALTER SYSTEM SET wal_level = logical;` and restart
  PostgreSQL. On a hosted PostgreSQL it is a setting of the service; on
  AWS RDS, for example, the parameter `rds.logical_replication = 1`.
- **A database user** with the `REPLICATION` permission, that owns the
  tables your views read (Deltaflow creates a publication of them) and
  the result tables.
- **A primary key** on every table your views read (or
  `REPLICA IDENTITY USING INDEX`).
- **Free replication slots:** two per Deltaflow process while it loads,
  one after that. PostgreSQL's defaults (10) are plenty to start with.

**Connect and run**

```sh
export DELTAFLOW_DSN='host=db.example.com user=deltaflow dbname=shop sslmode=verify-full'
deltaflow run views.sql --slot shop --state-dir /var/lib/deltaflow/shop
```

- **The connection** comes from `--dsn`, the `DELTAFLOW_DSN` variable, or
  the usual `PGHOST`, `PGUSER` and other `PG*` variables. Settings such as
  `sslmode` work exactly as in `psql`. A variable keeps the password off
  the command line.
- **`--slot`** names Deltaflow's bookmark in PostgreSQL. Each Deltaflow
  process needs its own.
- **`--state-dir`** is where Deltaflow keeps its working state (by default
  `.deltaflow/<slot>`). Put it on a disk that survives restarts; without
  it, Deltaflow loads the tables again from the start.
- **Changing a view:** edit `views.sql` and restart. Only the changed view
  is rebuilt.
- **`deltaflow status`** shows each bookmark, how much change log it holds
  back, and how far each view has got.
- **`deltaflow reset --slot shop`** when you stop using it (see step 7).

## What a view can contain

| Works | Example |
|---|---|
| Filters | `WHERE o.status <> 'cancelled' AND o.amount BETWEEN 10 AND 100` |
| Joins on equal columns | `JOIN customers c ON c.id = o.customer_id`, and `LEFT JOIN` |
| Totals per group | `count(*)`, `count(DISTINCT x)`, `sum`, `min`, `max`, `avg`, with `GROUP BY` and `HAVING` |
| Calculations | `+ - * / %`, `CASE`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `round`, `abs`, `length`, casts |
| Conditions | `LIKE`, `IN (...)`, `IS NULL`, `IS DISTINCT FROM` |
| json, jsonb, arrays | `data->>'type'`, `tags[1]`, `x = ANY (tags)` |
| Unique rows | `SELECT DISTINCT` |

Every view returns exactly what PostgreSQL itself returns for the same
query: the same values, the same NULLs, and an error where PostgreSQL
raises one. Tests check this against PostgreSQL.

**Not supported yet:** subqueries, `WITH`, `UNION`, `ORDER BY` and
`LIMIT`, window functions, `RIGHT` and `FULL` joins, and functions outside
the list above (such as `now()`). Deltaflow refuses such a view when it
starts and says why. To compare text with `<` or `>`, or take `min` or
`max` of text, add `COLLATE "C"`. The full list is in
[docs/sql.md](docs/sql.md).

## More examples

Put as many views in `views.sql` as you like; one Deltaflow process keeps
them all up to date.

**Every customer's spending, including customers with no orders yet:**

```sql
CREATE VIEW analytics.spend AS
SELECT c.id AS customer, count(o.id) AS orders, COALESCE(sum(o.amount), 0) AS spent
FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.id;
```

**A live copy of only the paid orders:**

```sql
CREATE VIEW analytics.paid_orders AS
SELECT o.id, o.customer_id, o.amount
FROM orders o
WHERE o.status = 'paid';
```

A view without `GROUP BY` must include the primary key of the table it
reads (`o.id` here); it becomes the result table's key.

**Reading your own write.** The result tables follow your writes within a
fraction of a second. When an app must see its own change at once, it can
wait for it:

```sql
INSERT INTO orders VALUES (6, 2, 15, 'paid');
SELECT _deltaflow.wait_for(pg_current_wal_lsn(), ARRAY['revenue_by_country'], '5 seconds');
SELECT * FROM analytics.revenue_by_country;
```

`wait_for` returns `true` once the view includes everything written before
it was called, or `false` after 5 seconds
([docs/read-your-writes.md](docs/read-your-writes.md)).

**Views written in Python.** Instead of SQL, views can be Python steps in a
module. Create the result table first:

```sql
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE analytics.orders_by_status (status text PRIMARY KEY, orders bigint, revenue numeric);
```

```python
# app/views.py
from deltaflow.planner import Count, Deltaflow, Sum
from deltaflow.sinks import PostgresSink


def registry() -> Deltaflow:
    df = Deltaflow()
    (
        df.view("orders_by_status")
        .source("public.orders")
        .group_by("status")
        .agg(orders=Count(), revenue=Sum("amount"))
        .sink(PostgresSink("analytics.orders_by_status"))  # PRIMARY KEY (status)
    )
    return df
```

```sh
deltaflow run app.views:registry --slot shop
```

## Run it in production

**With Docker.** The image is built for both Intel/AMD and ARM servers:

```sh
docker run -d --name deltaflow \
  -e DELTAFLOW_DSN='host=db user=deltaflow dbname=shop sslmode=verify-full' \
  -v deltaflow-state:/var/lib/deltaflow \
  -v "$PWD/views.sql:/etc/deltaflow/views.sql:ro" \
  -p 9187:9187 \
  ghcr.io/walid-ibnehasan/deltaflow run /etc/deltaflow/views.sql --slot shop \
    --state-dir /var/lib/deltaflow/shop --http-host 0.0.0.0
```

**On Kubernetes,** with the Helm chart:

```sh
kubectl create secret generic shop-db \
  --from-literal=dsn='host=db1,db2 user=deltaflow dbname=shop target_session_attrs=read-write'
helm install shop oci://ghcr.io/walid-ibnehasan/charts/deltaflow \
  --set database.existingSecret=shop-db \
  --set-file views.sql=views.sql
```

What else is there:

- **Monitoring.** Port 9187 serves Prometheus metrics (`/metrics`), a
  liveness check (`/healthz`) and a readiness check (`/readyz`, ready once
  the views are live). Ready-made alerting rules are in
  [`deploy/helm/deltaflow/files/deltaflow-alerts.yml`](deploy/helm/deltaflow/files/deltaflow-alerts.yml).
- **Several workers.** `deltaflow cluster` shares the work of the views
  between processes. When one dies, the others take over its share in
  about 3 seconds ([docs/scale-out.md](docs/scale-out.md)).
- **Database failover.** With PostgreSQL 17 or newer and several hosts in
  the connection, Deltaflow carries on with the promoted standby when the
  primary fails; in the test, 21 seconds later, with nothing reloaded
  ([docs/failover.md](docs/failover.md)).

More in [docs/deploy.md](docs/deploy.md) and
[docs/operations.md](docs/operations.md).

## How fast is it?

Measured with `make bench` on a 6-core desktop (AMD Ryzen 5 5600X, 16 GB),
with PostgreSQL 16 and four views (two joins, a total and a `DISTINCT`)
over 1,000 customers and 10,000 orders:

| Where it ran | Changes per second, sustained | Results visible within (99% of changes) |
|---|---|---|
| Linux container | **1,603** | **0.15 s** at 386 and 770 changes/s, **0.2 s** at 1,151 changes/s |
| Directly on Windows | 935 | 0.75 to 1 s, except one save of its state that stalled for 27 s on slow file writes |

Run it on Linux. The engine has become faster since (from 2,619 to 4,787
changes per second, the engine alone), but the full benchmark has not been
run again. That is enough for small and medium applications; much busier
systems will need the native core that is planned. Details:
[docs/milestones/M6.md](docs/milestones/M6.md).

## Status

**Version 0.1.1, beta.** It is tested hard: more than 600 tests,
including:

- every view compared with PostgreSQL's own answer after thousands of
  random changes;
- the process killed at every risky moment, with nothing lost or doubled;
- a database failover under load;
- CI on PostgreSQL 15, 16, 17 and 18.

It has not yet run in production outside its own tests, so try it on
something that is not critical first. What is not done yet is listed in
[docs/milestones/production.md](docs/milestones/production.md#not-done).

## Working on Deltaflow

Requires Docker, [uv](https://docs.astral.sh/uv/) and GNU make.

```sh
make up      # Postgres 16 (DELTAFLOW_PG_VERSION=15..18) with wal_level=logical and TLS on :55432
make seed    # customers + orders, deterministic
make load    # randomized inserts/updates/deletes; prints a replayable seed
make check   # ruff, mypy --strict, pytest
make tail    # print the decoded replication stream
make views   # filtered, join and aggregate views following the stream (run `make load` alongside)
make worker  # durable worker: four views into analytics.*, survives kill -9; metrics on :9187
make console # live operator graph and lag: http://localhost:3017
make differential  # acceptance tests (DELTAFLOW_SEED=N to replay a seed)
make chaos   # kill -9 the worker during load; sinks must equal a recompute
make bench   # capacity and freshness benchmark (re-seeds the database)
make cluster-worker ID=w1  # sharded worker; start w2, w3 alongside, kill any of them
make cluster-reset         # drop the cluster's slots, partial tables, leases and state
make failover              # failover under load on a Postgres 17 primary and standby
make image chart alerts    # the container image; the Helm chart; the alerting rules
```

How it works inside: deltas, operators and state
([docs/delta-algebra.md](docs/delta-algebra.md)), the replication reader
([docs/replication.md](docs/replication.md)), exactly-once results
([docs/exactly-once.md](docs/exactly-once.md)). Design decisions are in
[`docs/decisions/`](docs/decisions/).

How it was built, milestone by milestone:

| Milestone | Notes |
|---|---|
| M0: harness (compose Postgres, seed, load generator, CI) | [notes](docs/milestones/M0.md) |
| M1: hand-written `pgoutput` replication reader | [notes](docs/milestones/M1.md), [design](docs/replication.md) |
| M2: delta algebra, frontiers, map/filter views, in-memory sink | [notes](docs/milestones/M2.md), [design](docs/delta-algebra.md) |
| M3: RocksDB state with a memory budget, incremental join | [notes](docs/milestones/M3.md), [design](docs/join.md) |
| M4: group_by with count/sum/min/max (exact under deletes), distinct | [notes](docs/milestones/M4.md), [design](docs/aggregates.md) |
| M5: exactly-once Postgres sink, checkpointed state, `kill -9` chaos suite | [notes](docs/milestones/M5.md), [design](docs/exactly-once.md) |
| M6: metrics, JSON logs, Next.js console, backpressure, measured benchmark | [notes](docs/milestones/M6.md), [design](docs/operations.md) |
| M7: scale-out: sharded views, Postgres leases with epoch fencing, failover | [notes](docs/milestones/M7.md), [design](docs/scale-out.md) |
| Hardening: TLS, idle sources, restarts, the `deltaflow` command, left joins, avg and count distinct | [notes](docs/milestones/hardening.md) |
| Views written in SQL, verified against Postgres | [docs/sql.md](docs/sql.md), [decision 0008](docs/decisions/0008-sql-views.md) |
| Changed views rebuilt alone, without a bootstrap | [exactly-once.md](docs/exactly-once.md) |
| Bootstrap in chunks: VACUUM keeps working, a stopped load resumes | [decision 0009](docs/decisions/0009-chunked-snapshots.md) |
| Real schemas: json, jsonb, arrays; ADD COLUMN; partitioned tables; large transactions | [docs/schemas.md](docs/schemas.md) |
| Read-your-writes: `_deltaflow.wait_for` | [docs/read-your-writes.md](docs/read-your-writes.md) |
| Failover: Postgres 17 failover slots, several hosts, timeline checks | [docs/failover.md](docs/failover.md) |
| Packaging: wheel, container image, Helm chart, alerts, CI on Postgres 15–18 | [docs/deploy.md](docs/deploy.md) |
| Throughput: queued transactions applied as one step, faster hot paths | [operations.md](docs/operations.md#applying-queued-transactions-as-one-step-after-m7) |
| The production round: everything since hardening, what testing found, what is not done | [notes](docs/milestones/production.md) |

## License

Apache License 2.0: [LICENSE](LICENSE) and [NOTICE](NOTICE). Changes by
version are in [CHANGELOG.md](CHANGELOG.md).
