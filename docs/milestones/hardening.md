# After M7: hardening

Status: done, 2026-10-19. This round of work came after the seven
milestones, with one goal: find what Deltaflow lacked for real use, and fix
the gaps that hurt most. Sources:

- the "not done" lists in every milestone and design doc;
- reading the code paths a user would hit first: connecting, idling,
  failing, running their own views.

Each item below follows the project's rules: tests first, a mutation or a
run without the fix to show the test is not vacuous, and real output.

## What was lacking, and what was done

| # | Gap | Fix | Evidence |
|---|---|---|---|
| 1 | **The replication connection ignored `sslmode`.** `sslmode=require` connected in plaintext, possibly sending the password in the clear, while psycopg connections in the same process were encrypted. No managed Postgres could be used | TLS with libpq's settings and semantics (`replication/conninfo.py`); anything it cannot honour is refused | 39 settings tests; 11 negotiation tests against a scripted server; 7 TLS tests against the compose server, which now runs TLS, so the whole suite streams encrypted. [Decision 0002, amendment](../decisions/0002-raw-replication-protocol.md) |
| 2 | **An idle source pinned WAL forever.** With the views' tables quiet and the server busy, the slot never advanced, and Postgres kept all WAL since the last published change | Keepalives between transactions become progress; at each checkpoint the runner advances, flushes the watermark, checkpoints and acknowledges | Integration test writes 2 MB of unpublished WAL; the slot passes it with 0 transactions applied. Without the fix the slot stays at its bootstrap position. [exactly-once.md](../exactly-once.md#idle-sources-moving-a-without-a-transaction) |
| 3 | **A dropped connection ended the worker**, and a failing cluster shard was retried on every heartbeat | `DurableRunner.serve()` restarts in-process after transient failures, with backoff. Sinks start a fresh session on every bind. Cluster retries back off | Walsender and sink connection terminated mid-load: restarts ≥ 2, one bootstrap, sinks exact. Without the sink reset the restart fails at once. Cluster: at most 5 attempts in 5 s, where there were 43 |
| 4 | **Only the demo views could run.** No command for your own views, and cluster workers had no metrics | The `deltaflow` command: `run`, `cluster`, `status`, `reset`. `prepare_cluster()` for any sharded registry. Per-shard metrics merged at one endpoint | 6 unit tests; 4 end-to-end tests drive the command as a subprocess. The demo worker now runs through the same code |
| 5 | **Inner joins only** | `how="left"`: left outer joins, maintained incrementally, sharded exactly | Property test against a naive LEFT JOIN; two mutations fail it. Differential test, both directions, against Postgres's LEFT JOIN after each of 8,014 commits. [join.md §1b](../join.md#1b-the-left-outer-join-added-after-m7) |
| 6 | **No `avg`, no `count(distinct)`** | `Avg` exactly as Postgres computes it, display scale included; `CountDistinct` | 600 random averages from Postgres must match in value and number of decimals; two mutations fail it. Differential test against SQL after each of 2,513 commits. [aggregates.md §6](../aggregates.md#6-avg-and-countdistinct-added-after-m7) |

`make check` on the final state (ruff, mypy --strict, pytest), on the
Windows host:

```
================= 421 passed, 2 skipped in 895.54s (0:14:55) ==================
```

Both skips depend on the platform:

- the stalled-consumer test runs on Linux, as in CI (M6);
- a file-mode test is POSIX only.

## Found along the way

- **The SCRAM exchange could be cut short.** A server that started SCRAM
  and then sent AuthenticationOk, without the final message proving it
  knows the password, was accepted. An impostor could have passed as the
  server, with or without TLS. It is now refused, with a test.
- **Bytes injected after the TLS answer** (CVE-2021-23222) cannot be read
  as server messages. TLS is negotiated on the raw socket, reading exactly
  one byte of the answer. There is a test with a scripted server that
  injects them.
- **What TLS costs, measured.**
  - Latency: none. Commit to receive was 1.2 ms median with and without
    TLS.
  - Memory: a one-off 6 to 10 MiB, the first time each kind of connection
    is made (libpq's OpenSSL and Python's). The memory acceptance test went
    over its allowance once with TLS: 65.1 MiB of growth against 64. On the
    same seed, growth was 55.1 MiB without TLS and 61.6 MiB with it. Its
    baseline is now taken after one connection of each kind; growth is then
    50.2 MiB, within the original allowance.
- **`abs()` on a Decimal rounds to 28 digits.** The average's weight
  computation used it; it now uses `copy_abs()`. Testing against Postgres
  found this, through the test's own inexact sum.
- **A prediction of mine was wrong, and Postgres settled it.** The spot
  check claimed avg(10²⁰, 0, 0) prints 33333333333333333333.333333. It
  prints 33333333333333333333: the quotient already has 16 significant
  digits. The test now holds Postgres's answer.
- **The first `deltaflow status` on this machine** showed two slots from
  earlier benchmark and demo runs. Nothing had consumed them for 15 hours,
  and they retained 2.8 GB and 2.4 GB of WAL:

  ```
  slots (3)
    slot              active  pid  confirmed_flush_lsn  lag_bytes   wal_status
    deltaflow_bench   False   -    0/E1F02378           2367253984  extended
    deltaflow_worker  False   -    0/C5F7A0A8           2836525232  extended
  ```

  They were left as they are: they are demo state, and `make worker`
  resumes from `deltaflow_worker`. `deltaflow reset --slot deltaflow_bench`
  drops one.

## Demo

```sh
make up && make seed
uv run deltaflow status --dsn postgresql://deltaflow:deltaflow@localhost:55432/deltaflow
uv run pytest tests/integration/test_cli.py   # run, status, reset and cluster, end to end
```

The end-to-end tests run the command as a user would, against the compose
server:

- `run` on a module of views, with load, watermarks, `/metrics` and
  `/api/stats` checked, and the sink compared with SQL;
- `status --json` and the text form;
- `reset`, which is refused while the slot is in use;
- `cluster` with two shards, checking:
  - both shards' metrics, labelled, at one endpoint;
  - the merge view the command created, compared with SQL.

## Still not done, and why

- **One reader feeding many shards.** A redesign that decision 0007 marks
  as the way beyond tens of shards.
- **An exchange operator, and re-sharding.** Both need a shuffle protocol
  with its own frontiers.
- **A checkpoint that does not hold the GIL.** rocksdict exposes neither a
  GIL-free checkpoint nor RocksDB's "disable file deletions", which a
  hand-rolled one would need.
- **Schema evolution.** A view's functions take whole rows, so which
  columns a view depends on cannot be known. A schema change still raises
  `SchemaChangedError`.
- **Full outer joins**, `string_agg`, percentiles, and `Avg`/`CountDistinct`
  in sharded views (their partials do not combine).
- **SCRAM channel binding** (`channel_binding=require` is refused). It
  needs the hash algorithm of the server certificate's signature, which
  Python's ssl module does not expose.
- **Sum's display scale.** Sum keeps the largest scale it has ever held,
  where Postgres uses the largest of the current values. The values are
  equal; only trailing zeros may differ.
- **The console shows single workers only.** A cluster worker's
  `/api/stats` has a different shape (one entry per shard), which the
  console does not read yet.
