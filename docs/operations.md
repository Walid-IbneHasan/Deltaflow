# Operating Deltaflow: metrics, logs, console, backpressure, performance

This is written for a reviewer who will push back. Every claim names the test
or measurement behind it. Measured numbers are in the README and
[milestones/M6.md](milestones/M6.md).

## Metrics (`GET /metrics`, Prometheus text format)

The durable worker serves them on `--http-port` (default 9187, bound to
127.0.0.1). Format and server are hand-written
([decision 0006](decisions/0006-metrics-without-prometheus-client.md)).

A cluster worker (`deltaflow cluster`) serves the same metrics for every
shard it runs, at one endpoint and each labelled `shard="<n>"`. A family
appears once, which the format requires, so the shards cannot simply be
listed one after another (`observability.metrics.merge`). The worker adds
its own metrics:

- `deltaflow_cluster_owned_shards`;
- `deltaflow_cluster_shard_events_total{event,shard}` (acquired, released,
  failed);
- `deltaflow_cluster_zombies_terminated_total`.

Its `/api/stats` gives each shard's runner stats under `shards`, keyed by
shard number.

| Metric | What it measures |
|---|---|
| `deltaflow_end_to_end_lag_seconds` (histogram) | **freshness**: per transaction, source commit → commit of the sink transaction that made it visible |
| `deltaflow_operator_deltas_total{operator,direction}` | **per-operator throughput**: deltas into and out of each operator (rates via `rate()`) |
| `deltaflow_operator_seconds_total{operator}` | time spent in each operator |
| `deltaflow_state_bytes{kind}` | **state size**: SST and blob files on disk, memtables, cache usage |
| `deltaflow_slot_lag_bytes` | **slot lag**: WAL the server retains for the slot (`pg_current_wal_lsn() − confirmed_flush_lsn`), polled every 2 s on its own connection. It stays bounded even while the views' tables are idle: the runner acknowledges keepalive progress at every checkpoint (`idle_advances` event) |
| `deltaflow_apply_lag_bytes` | WAL received but not yet applied by the engine |
| `deltaflow_stream_queued` | decoded transactions waiting for the engine; at its bound (32) the reader stops reading: backpressure |
| `deltaflow_lsn{position}` | engine frontier, sink watermark, checkpoint LSN |
| `deltaflow_sink_flush_seconds`, `deltaflow_checkpoint_seconds` | durations |
| `deltaflow_transactions_total`, `deltaflow_source_changes_total`, `deltaflow_sink_rows_written_total`, `deltaflow_runner_events_total{event}` | counts, including bootstraps (a full recompute is never silent) |
| `deltaflow_resident_memory_bytes` | RSS |

Two notes on how these are measured.

**Freshness spans two clocks.** The commit time comes from the Postgres
server, the flush time from the worker. The benchmark measures the offset
between them (0.0 ms on the benchmark machine) and reports it with every
run. Clock skew between machines would show up directly in this metric.

**Replayed transactions are not freshness samples.** After a resume, the
transactions at or below the sink watermark were visible long before. The
first version counted them anyway, with their old commit times, when a
flush covered both replayed and new transactions. That is fixed: only
transactions past the previous watermark are observed.

`test_metrics_and_stats_report_the_run` checks, after a real load, that the
transaction count, freshness observations, per-operator counters,
checkpoint events, state size and slot lag are all exposed and non-trivial.

## Logs

One JSON object per line on stderr (`--log-format json`, the default), with
event fields as top-level keys:

    {"ts": "…", "level": "info", "logger": "deltaflow.runtime.durable", "msg": "checkpoint", "lsn": "0/C560DA18", "seconds": 0.063}
    {"ts": "…", "level": "warning", "logger": "deltaflow.runtime.durable", "msg": "bootstrapping from a fresh snapshot: no state checkpoint", "reason": "no state checkpoint"}

## Console (`console/`, Next.js)

`make console`, then open http://localhost:3017. It polls the worker's
`GET /api/stats` once a second and shows:

- **the operator graph**: sources, joins, filters, maps, aggregates and
  sinks, laid out by longest path from a source;
- **per operator**: deltas in/s and out/s, and how busy it is (share of
  wall time spent in it). Edges light up when data flows;
- **runner metrics**: freshness p50/p99, frontier, watermark, checkpoint,
  apply lag, slot lag, queue depth against its bound, state size, cache
  against budget, RSS.

`?api=http://host:port` points it at another worker. The rate and layout
logic is unit-tested with `node --test` (`console/lib/graph.test.ts`).
CI type-checks and builds it.

## Bootstrapping a large database

A bootstrap loads the tables in chunks by default (decision 0009): one
primary-key page at a time, each with its own exported snapshot, while the
stream runs. No transaction stays open longer than one page, so VACUUM keeps
working during the load.

**What a load needs from the server.** Beside its slot and walsender, a
loading runner uses one more of each: a temporary slot per page, created
and dropped in turn on a second replication connection. Size
`max_wal_senders` and `max_replication_slots` for two per runner (two per
shard in a cluster) while loads run.

**What stalls a load.** Creating a page's slot waits for the transactions
open at that moment to end. A session idle in a transaction for an hour
stalls the load for an hour. The runner logs a warning every 10 s while it
waits, naming the oldest open transactions (pid, state, age, query). The
remedy is the server's: `idle_in_transaction_session_timeout`, or ending the
session.

**Watching it.** `GET /api/stats` reports `"loading"`: per table, rows and
pages read and whether it is done. The `deltaflow_runner_events_total` events
`loads` and `load_resumes` count loads started and continued. The load is
checkpointed every `--checkpoint-interval`, and the slot is acknowledged up
to each checkpoint, so `deltaflow_slot_lag_bytes` stays bounded during a long
load. A stopped or crashed load continues from its last checkpoint.

**Page size** (`--snapshot-chunk-rows`, default 20,000). Larger pages mean
fewer slot creations, but each page holds back VACUUM for as long as it takes
to read. `--snapshot single` reads everything in one transaction. That is
fastest for small databases, where holding back VACUUM for a few seconds
does not matter.

## Backpressure under a slow sink

Nothing in the pipeline buffers without bound:

1. a slow flush (the sink cannot commit) blocks the run loop;
2. the engine stops taking transactions from the stream's queue;
3. the queue fills to `max_pending`, and the reader stops reading the socket;
4. TCP flow control stops the walsender.

A separate heartbeat task keeps sending standby status updates, so the
server does not drop the connection after `wal_sender_timeout`.

`test_a_slow_sink_applies_backpressure_and_survives_wal_sender_timeout`
lowers `wal_sender_timeout` to 2 s and locks a sink table for 8 s, four
times the timeout, under load. It checks that:

- the queue reaches exactly its bound (32/32);
- RSS does not grow;
- the replication connection survives;
- after the lock is released everything catches up, and every sink equals
  its SQL.

Two things found while making that test honest:

- **The first version tested nothing.** `docker-compose.yml` passed
  `-c wal_sender_timeout=60s`, and command-line settings override
  `ALTER SYSTEM`, so the timeout was never lowered. The flag is gone, and
  the test now asserts the setting took effect. With heartbeats disabled it
  then fails, as it should.
- **Docker Desktop on Windows breaks the premise.** Its port forwarding
  stops relaying client → server data while the server → client direction
  is backed up, so heartbeats never reach the walsender, and the server
  drops the connection after the timeout. The same stalled consumer run
  from a Linux container on the compose network kept its connection
  (`reply_time` kept advancing through the stall). The test therefore runs
  on Linux (CI) and is skipped on win32, with that reason.

  Consequence for Windows + Docker Desktop: a consumer stalled longer than
  `wal_sender_timeout` is disconnected. The runner exits, and a restart
  resumes exactly-once (M5). The runner does not reconnect by itself: run
  it under a supervisor.

## Performance: what measuring found

`make bench` (`benchmarks/throughput.py`) has two parts.

- **Capacity:** a backlog of transactions commits while the worker is
  stopped. Its catch-up is then timed, from the moment its endpoint
  answers until every sink's watermark passes a marker. Nothing waits on
  the load generator, so this is the engine's ceiling, with sink writes and
  checkpoints included.
- **Freshness:** load arrives at fixed fractions of capacity, and the
  freshness histogram is read afterwards.

Four problems were found by measuring, not by reading code:

1. **Checkpoints queued behind compactions (7.7 s each).** With one RocksDB
   background job (the M3 memory tuning), the memtable flush every
   checkpoint needs waited for a running compaction. A catch-up of 400
   transactions took 37 s, 31 of them in checkpoints. With two background
   jobs: 3.5 s. The M3 memory test still passes.
2. **Per-row sink statements cost about 45 ms a batch.** Every
   `executemany` (psycopg pipeline mode) cost about 45 ms whether it
   carried 8 rows or 143, while a simple query took 0.36 ms. Flushes took
   about 366 ms and were 19 of a 26 s catch-up. Now each flush writes one
   multi-row `INSERT … ON CONFLICT … RETURNING` and one `DELETE … WHERE
   (key) IN (VALUES …) RETURNING` per sink, and the drift check matches
   the returned keys. Capacity went from about 420 to about 1,230
   changes/s on Windows.
3. **Freshness counted replays** (above).
4. **Multi-second file I/O stalls on the Windows host.** Some checkpoints
   took 20–58 s there. RocksDB's own log shows why: a background flush
   took 31 s to write one 6.7 MB blob file, while the next flush wrote a
   1.3 MB one in 0.3 s. Raw `write` + `fsync` of 6 MB files from Python
   took at most 238 ms, idle or under Postgres load, so this is not plain
   disk contention. On-access antivirus scanning is enabled on that host,
   and a scan of a file being appended to is the most likely cause; that
   is not proven (it needs admin-level tracing). The same benchmark in a
   Linux container on the same machine had 21 checkpoints, the slowest
   0.118 s.

**One hypothesis was wrong, and is recorded as such.** Checkpoints run in
a worker thread (`asyncio.to_thread`). A single A/B run suggested the thread
itself caused the stalls (57.8 s threaded, 0.37 s direct), so the call was
briefly made directly on the event loop. A later run showed a 29 s stall
with the direct call: the slowness had nothing to do with threads.

**A second explanation was wrong too, and is corrected here.** The backpressure
test once failed in CI (Python 3.14): `terminating walsender process due to
replication timeout`. A checkpoint had stopped the heartbeats for longer
than the test's 2 s `wal_sender_timeout`. I moved the checkpoint back into
a worker thread "so heartbeats keep running", and CI went green. That
reasoning was false. **rocksdict holds the GIL for the whole checkpoint.**
During a 1.1 s checkpoint in a thread, the main thread made exactly one
iteration, so heartbeats (asyncio or thread) wait for it either way. CI
passed because the next runs' checkpoints were fast.

What is actually true:

- **A slow checkpoint freezes the whole worker process.** On Linux that was
  0.05–0.12 s. On the Windows host, sometimes 20–30 s.
- **Correctness is never at stake.** A frozen process cannot write, and on
  waking it is fenced (M7) or finds its slot gone.
- **Availability needs margin.** `wal_sender_timeout` and the cluster lease
  TTL must exceed the slowest checkpoint the host produces.

The real fix would be a checkpoint that releases the GIL, or runs out of
process. rocksdict offers neither, and a hand-rolled checkpoint (flush plus
hard links) is unsafe without RocksDB's "disable file deletions", which
rocksdict does not expose. Not done.

**What a checkpoint costs everyone else:** the run loop waits for it, so
freshness of transactions arriving during a checkpoint includes it. On
Linux that was 0.05–0.12 s every 10 s. On the Windows host, rarely, tens
of seconds.

### Applying queued transactions as one step (after M7)

The engine's per-change cost is Python's. A profile of replayed
transactions shows no single hotspot. The time is spread across join
output, RocksDB reads, key encoding and row hashing. Two changes help:

- **Hot paths.** Rows are built without copying their dict, and a column
  name is resolved once per row shape. The join caches its qualified
  column names and reads a stored count without unpickling the row. Key
  encoding caches short strings, small varints and each row shape's
  sorted column names.
- **Batches.** When the stream has already queued transactions behind
  the one at hand, the runner applies up to 256 of them as one step
  (`Engine.apply_batch`, `max_batch_transactions`). Their changes are
  consolidated, so a key changed several times is read and written
  once. The runner never waits to fill a batch. Under light load each
  transaction is still its own step, so batching changes nothing for
  freshness and helps exactly when the worker is behind: a backlog, a
  burst, a restart. Batches form only past every sink's watermark,
  since a batch stamped past a watermark must not hold replayed changes.

Measured on 2,840 recorded `pgoutput` transactions (12,034 changes)
through the worker's four views, engine only (no sinks, no checkpoints),
on the Windows host. Each figure is the median of three runs, all taken
at the same time:

| Code | changes/s | |
|---|---|---|
| before | 2,619 | |
| hot paths, one transaction at a time | 3,276 | 1.25× |
| batches of 32 | 4,787 | 1.83× |
| batches of 256 | 4,857 | 1.85× |

The end-to-end `make bench` figures in the README are M6's; they were
not measured again in this round (the host ran short of memory). The
next large step would be native code for the operators, since what is
left is spread over many small Python calls. It is not done.
