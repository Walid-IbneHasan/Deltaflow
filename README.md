# Deltaflow

Incremental view maintenance over PostgreSQL logical replication. Deltaflow
reads the `pgoutput` stream from a replication slot and turns row changes into
signed multiset deltas. Those deltas flow through a dataflow operator graph
that keeps registered views up to date. The cost of an update is proportional
to the size of the change, not the size of the tables.

## Status

| Milestone | State |
|---|---|
| M0 — harness (compose Postgres, seed, load generator, CI) | done — [notes](docs/milestones/M0.md) |
| M1 — hand-written `pgoutput` replication reader | done — [notes](docs/milestones/M1.md), [design](docs/replication.md) |
| M2 — delta algebra, frontiers, map/filter views, in-memory sink | done — [notes](docs/milestones/M2.md), [design](docs/delta-algebra.md) |
| M3 — RocksDB state with a memory budget, incremental join | done — [notes](docs/milestones/M3.md), [design](docs/join.md) |
| M4 — group_by with count/sum/min/max (exact under deletes), distinct | done — [notes](docs/milestones/M4.md), [design](docs/aggregates.md) |
| M5 — exactly-once Postgres sink, checkpointed state, `kill -9` chaos suite | done — [notes](docs/milestones/M5.md), [design](docs/exactly-once.md) |
| M6 — metrics, JSON logs, Next.js console, backpressure, measured benchmark | done — [notes](docs/milestones/M6.md), [design](docs/operations.md) |
| M7 — scale-out: sharded views, Postgres leases with epoch fencing, failover | done — [notes](docs/milestones/M7.md), [design](docs/scale-out.md) |

Views can filter, map, project, inner-join and aggregate source tables.
Operator state lives in RocksDB under a memory budget, and checkpoints
make it survive crashes. Results go to Postgres tables exactly once, each
with a watermark committed in the same transaction. This table
is updated only when a milestone's acceptance test passes.

```python
df = Deltaflow()
(
    df.view("orders_per_customer")
    .source("public.orders")
    .join("public.customers", on=("customer_id", "id"))
    .filter(lambda r: r["status"] != "cancelled")
    .group_by("customers.country")
    .agg(count=Count(), revenue=Sum("orders.amount"))
    .sink(sink := MemorySink())
)
async with Runner(df, dsn, memory_budget=64 << 20) as runner:
    await runner.run()  # sink.result == the view, exactly, as of sink.frontier
```

## Benchmark (measured)

`make bench` (method: [`benchmarks/throughput.py`](benchmarks/throughput.py)).
The workload is the worker's four views: a join + group_by, a join, a
global aggregate and a distinct, over 1,000 customers and 10,000 orders. It
writes exactly-once to Postgres sinks and checkpoints every 10 s. The
machine is an AMD Ryzen 5 5600X (6 cores), 16 GB, Windows 11 with Docker
Desktop, and PostgreSQL 16.15.

"Sustained changes/second" is the time to catch up a 20,814-change backlog,
sink writes and checkpoints included. Freshness runs from the source commit
to the sink commit that made the change visible. Percentiles are histogram
bucket upper bounds.

| Run | Sustained changes/s | p99 freshness at 25% / 50% / 75% load |
|---|---|---|
| Linux container on this machine | **1,603** (438 txn/s) | **≤ 0.15 s / ≤ 0.15 s / ≤ 0.2 s** (386 / 770 / 1,151 changes/s) |
| Native on the Windows host | **935** (256 txn/s) | ≤ 0.75 s / **≤ 30 s** / ≤ 1.0 s (226 / 449 / 549 changes/s) |

The Windows 50% figure is real. One checkpoint stalled for 27 s, because
the host took up to 31 s to write a single RocksDB file. On Linux, on the
same hardware, the slowest checkpoint took 0.118 s. Details, evidence and
everything else measuring found:
[docs/milestones/M6.md](docs/milestones/M6.md).

## Quick start

Requires Docker, [uv](https://docs.astral.sh/uv/) and GNU make.

```sh
make up      # Postgres 16 with wal_level=logical and TLS on localhost:55432
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
```

## Run your own views

Write the views in a module, create their sink tables, and point the
`deltaflow` command at them:

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
deltaflow run app.views:registry --slot app     # one durable worker; metrics on :9187
deltaflow status                                # slots and the WAL they retain, watermarks, leases
deltaflow reset --slot app                      # drop the slot and the state (refused while in use)
deltaflow cluster app.views:sharded --worker-id w1 --slot-prefix app   # one worker of a cluster
```

Some details:

- **Connection settings** come from `--dsn`, or `$DELTAFLOW_DSN`, or
  libpq's `PG*` variables. They are resolved as libpq resolves them,
  `sslmode` included, for the replication connection too.
- **Restarts.** `run` restarts after transient failures, resuming from its
  checkpoint, so results stay exactly-once.
- **Clusters.** A cluster needs a sharded registry
  (`Deltaflow(shards=N, shard_by=...)`) and, per view, a partial table
  `<table>__shards` with a leading `shard` column. `cluster` creates the
  merge view `<table>` over it ([docs/scale-out.md](docs/scale-out.md)).

Design decisions are in [`docs/decisions/`](docs/decisions/), and per-milestone
notes are in [`docs/milestones/`](docs/milestones/).
