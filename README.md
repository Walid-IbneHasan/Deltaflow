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
| M6–M7 | not started |

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

## Quick start

Requires Docker, [uv](https://docs.astral.sh/uv/) and GNU make.

```sh
make up      # Postgres 16 with wal_level=logical on localhost:55432
make seed    # customers + orders, deterministic
make load    # randomized inserts/updates/deletes; prints a replayable seed
make check   # ruff, mypy --strict, pytest
make tail    # print the decoded replication stream
make views   # filtered, join and aggregate views following the stream (run `make load` alongside)
make worker  # durable worker: four views into analytics.*, survives kill -9
make differential  # acceptance tests (DELTAFLOW_SEED=N to replay a seed)
make chaos   # kill -9 the worker during load; sinks must equal a recompute
```

Design decisions are in [`docs/decisions/`](docs/decisions/), and per-milestone
notes are in [`docs/milestones/`](docs/milestones/).
