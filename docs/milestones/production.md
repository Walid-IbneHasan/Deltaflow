# After hardening: production readiness

Status: done, 2026-10-20, with the gaps listed at the end. This round
had one goal: what would someone running Deltaflow for a business hit
first, and what would stop them? Each item follows the project's rules:
tests first, a mutation or a run without the fix to show the test is not
vacuous, and real output.

## What was lacking, and what was done

| # | Gap | Fix | Evidence |
|---|---|---|---|
| 1 | **Views could only be written in Python** | `deltaflow run views.sql`: `CREATE VIEW` statements, checked by Postgres before anything streams (each is created as a temporary view in a rolled-back transaction); missing sink tables are created with Postgres's types and the view's key | Random expressions against Postgres's own answers (20,000 in a soak); a differential test of SQL views against Postgres after every commit. [docs/sql.md](../sql.md), [decision 0008](../decisions/0008-sql-views.md) |
| 2 | **Any change of views meant a full bootstrap** | Changed and new views are built alone, from the checkpoint's copy of the source rows, at its position; the slot and the other views are untouched | The unchanged views' sink rows are not rewritten (their `xmin` is the same after the restart); a crash between the two renames that swap the checkpoint loses nothing. [exactly-once.md](../exactly-once.md) |
| 3 | **A bootstrap held back VACUUM on every table** for the whole load, and a crash started it over | Tables load a primary-key page at a time, each page with its own snapshot; per key, the newest writer wins; the load is checkpointed | The per-key rule against a model of the table under random interleavings (two mutations fail it); VACUUM removes rows deleted during a chunked load; a stopped load resumes without reading a row twice. [Decision 0009](../decisions/0009-chunked-snapshots.md) |
| 4 | **json, jsonb and arrays stopped the pipeline**; a schema change went unnoticed; partitioned tables and huge transactions were not handled | Exact values, and Postgres's json operators; `ADD COLUMN` is followed, and other changes stop the runner; partitioned tables via their root; large transactions in parts, with sinks that spill to disk | 10,000 random JSON documents against Postgres; the spilling sink against the in-memory one (property test). [docs/schemas.md](../schemas.md) |
| 5 | **No read-your-writes**: an application could read a view before its own write arrived | `_deltaflow.wait_for(target, views, timeout)`, callable by any role | Median 63 ms with a 30 s checkpoint interval; without the wake-up it times out. [docs/read-your-writes.md](../read-your-writes.md) |
| 6 | **A failover of the source lost the runner's slot and position** | Failover slots (Postgres 17+), several hosts in the DSN, and a check of the system identifier and timeline on every resume | `make failover`: the primary is killed under load and the standby promoted; the runner reconnects and resumes, with no bootstrap, and every view equals its SQL on the new primary. [docs/failover.md](../failover.md) |
| 7 | **Nothing to install or deploy** but the repository | A wheel, a container image (non-root, hash-locked dependencies), a Helm chart, `/readyz`, alerting rules, CI on Postgres 15 to 18 | The chart lints and renders in both modes; promtool checks the rules and passes their unit tests; the image builds and runs. [docs/deploy.md](../deploy.md) |
| 8 | **Throughput** | Queued transactions applied as one step; faster hot paths | Engine only, on recorded transactions: 2,619 changes/s before, 4,787 after with batches of 32. Every view after every batch equals applying the transactions one by one (property test). [operations.md](../operations.md#applying-queued-transactions-as-one-step-after-m7) |

## Test results

The final state (ruff, mypy --strict, pytest) on the Windows host with
Postgres 16, in parts (the host was short of memory for one long run):

| Part | Result |
|---|---|
| unit | 494 passed, 1 skipped (POSIX file modes) |
| integration | 98 passed, 1 skipped (a stalled consumer: Linux only, M6) |
| chaos (`kill -9`, scale-out) | 10 passed |
| differential | 6 passed |
| failover (`make failover`, Postgres 17) | 1 passed |

The differential run came before the last commit (the host preference),
which changes nothing for a DSN with one host; the other parts ran after
it.

The other Postgres versions, run locally during the round:

- **15:** every integration module alone, each on a new server: 97
  passed, 1 skipped (after the fixes below).
- **17:** chaos, differential and integration: 112 passed, 1 skipped,
  1 failed. The failure was a real bug, fixed (below). The failover test
  runs on Postgres 17 too.
- **18:** chaos, differential and integration on the final code: 114
  passed, 1 skipped.

## Found along the way

- **`x IN (NULL)` raised where Postgres returns NULL.** The random
  comparison with Postgres found `(i / i) IN (NULL)` raising division
  by zero, where Postgres returns NULL. Postgres's parser turns a
  one-item IN into `x = item`, a strict operator, and the planner folds
  it to NULL without evaluating x. With several items, the ones without
  columns become one `= ANY (...)` and the rest separate comparisons,
  evaluated in order. The compiler now builds the same parts. The test
  values come from Postgres, and seven of them failed before the change.
- **A replay after a rebuild could stop the runner.** It happened once,
  on Postgres 17 on a loaded machine. A rebuilt view starts at the
  checkpoint while the other sinks are further on. A replay flush then
  included the sinks already past it, and moving their watermarks back
  was refused. A flush now writes only the sinks behind it. A new test
  reproduces it every time and failed before the change.
- **After a failover, every new connection waited on the dead primary.**
  libpq tries the hosts in order, and a start opens three connections,
  each first waiting `connect_timeout` on the old primary. Every later
  restart would do the same, for as long as that host stays down. The
  runner and the cluster worker now move the server that answered to the
  front of their host lists. In the failover test the runner resumed 45 s
  after the kill without this, and 21 s with it. The test's own
  connections had no `connect_timeout` and waited minutes on the dead
  port; with one, the test takes 36 s instead of 310 s.
- **Two tests passed only because of earlier tests.** On a new Postgres
  15, two CLI tests cleared tables nothing had created yet. Every
  integration module now passes on its own on a new server.
- **The random expression generator produced ill-typed SQL**
  (`round(round(1), 0)`: there is no `round(float8, int)`). Postgres
  refuses it before a view could ever contain it; the comparison now
  leaves such expressions out.
- **JSON in Postgres has quirks, and the 10,000-document comparison
  found three:** jsonb's raw scalar answers an index; json accepts lone
  surrogates but fails every operator on such a document; json's `->`
  returns the last of duplicate keys. The scanner reproduces all three.
- **Concurrent cluster workers' DDL collided in the catalog**
  (`CREATE ... IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`). Deltaflow's
  DDL now runs under an advisory lock.
- **From Git Bash on Windows, the data mount for Postgres 18
  (`/var/lib/postgresql`) is rewritten into a Windows path.** The
  compose file says to set `MSYS_NO_PATHCONV=1`.

## Not done

- **End-to-end benchmark.** `make bench` was not measured again in this
  round: the run was stopped when the host ran short of memory. The
  README's end-to-end figures are M6's.
- **A native core.** What is left of the per-change cost is spread over
  many small Python calls, so the next large step is native code for the
  operators (Rust, DBSP-style). There is no Rust toolchain on this
  machine.
- **Publishing.** The wheel and image build, but nothing is on PyPI or a
  container registry: that needs the owner's accounts.
- **A license.** The repository has none, so the package metadata has
  none; that is the owner's decision.
- **A checkpoint still holds the GIL** (M6): on a host with slow file
  I/O it can freeze the worker for its duration. `wal_sender_timeout`
  and the cluster lease TTL must allow for it.
