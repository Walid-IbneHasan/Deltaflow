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
| M2–M7 | not started |

There are no views yet: so far Deltaflow reads and decodes the change stream correctly. This table is updated only when a
milestone's acceptance test passes.

## Quick start

Requires Docker, [uv](https://docs.astral.sh/uv/) and GNU make.

```sh
make up      # Postgres 16 with wal_level=logical on localhost:55432
make seed    # customers + orders, deterministic
make load    # randomized inserts/updates/deletes; prints a replayable seed
make check   # ruff, mypy --strict, pytest
make tail    # print the decoded replication stream
make differential  # replay acceptance test (DELTAFLOW_SEED=N to replay a seed)
```

Design decisions are in [`docs/decisions/`](docs/decisions/), and per-milestone
notes are in [`docs/milestones/`](docs/milestones/).
