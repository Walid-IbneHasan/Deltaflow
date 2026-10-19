# Exactly-once: why `kill -9` neither loses nor duplicates

This is written for a reviewer who will push back. Every claim names the test
that checks it. The design choice behind it is
[decision 0005](decisions/0005-durable-state-checkpoints.md).

## The three positions

At any moment there are three LSNs, and the whole argument is about their
order:

| Position | Where it lives | Made durable by |
|---|---|---|
| **A**: slot acknowledged | Postgres (`confirmed_flush_lsn`) | standby status updates |
| **C**: state checkpoint | `state/checkpoints/<C>/` with a `COMPLETE` marker | `fsync` of the marker |
| **W**: sink watermark, per view | `_deltaflow.watermarks` | the same Postgres transaction as the sink rows |

**Invariant: A ≤ C ≤ W.**

- *C ≤ W.* A checkpoint is taken only at the engine's own frontier E, and
  only if E ≤ W. `DurableRunner._checkpoint` asserts this.
- *A ≤ C.* The slot is acknowledged only after a checkpoint completes, and
  only up to it (`_checkpoint` → `stream.ack(C)`).
- **W only moves forward.** `sinks.postgres.flush` refuses a watermark lower
  than the current one, and a flush while replaying is a no-op (§4).

## What a restart does

1. **Restore the state to C.** Delete `live/`, then rebuild it from the
   newest complete checkpoint: SST and blob files are hard-linked, the small
   mutable files copied. Everything the crashed process did after C is gone,
   including a transaction it had applied halfway. That is why the RocksDB
   WAL can stay off.
2. **Stream from C.** The slot holds everything after A, and A ≤ C, so
   every transaction after C is still available. The stream skips those
   with `end_lsn ≤ C`.
3. **Replay (C, W] without writing it.** The engine applies those
   transactions to the restored state, since the state needs them. Each
   sink drops the resulting deltas, because its table already reflects every
   transaction up to W.
4. **After W, carry on.** Each flush writes the net row changes and moves W,
   atomically.

**Nothing is lost.** Every transaction after C is replayed into the state.
Every transaction after W reaches the sink, because the sink drops only
those at or below W.

**Nothing is applied twice.** The state is never ahead of C after a
restore. A sink row and the watermark that covers it commit in one
transaction, so a crash either keeps both or neither:

| Crash point | State after restart | Sink | Replay |
|---|---|---|---|
| before the sink commit | C | rows and watermark rolled back together, still W_old | (C, W_old] dropped; the rest written |
| after the sink commit | C | rows and new W both durable | (C, W] dropped; the rest written |

**Determinism is what makes "dropped" safe.** The replay of (C, W] has to
produce the same deltas as the original run, or the state after replay would
differ from what the sink reflects. Operators are deterministic functions of
their state and input (user functions must be too, see
[delta-algebra.md](delta-algebra.md) §5), and the replayed transactions are
byte-identical WAL. `test_replay_after_restore_produces_exactly_the_original_changes`
checks that: replaying after a restore emits exactly the deltas the original
run emitted.

## Bootstrap is the other way to recover

With no usable checkpoint, the runner logs a warning, counts a bootstrap,
and rebuilds from scratch:

- drop the slot;
- create it with an exported snapshot;
- load the snapshot;
- **replace** every sink table's contents and set W to the snapshot's
  consistent point, in one transaction;
- checkpoint there.

Reasons for no usable checkpoint:
- first start;
- the slot is missing;
- the views changed (plan fingerprint);
- a watermark is missing or behind the checkpoint.

A crash anywhere inside bootstrap leaves no complete checkpoint, so the next
start bootstraps again. The replace makes that idempotent.

**Limit:** the fingerprint covers the views' structure and their functions'
*names*, not their bodies. Changing what a lambda computes needs
`make worker-reset`.

## The sink never trusts itself

Writes are classified against what the table must already contain:

| Within one flush, a key… | Means | Written as | The table must report |
|---|---|---|---|
| first appears as a retraction | the row existed before | update or delete | a row updated / deleted |
| first appears as an addition | the row is new | insert | a row inserted |

The checks use upserts with `RETURNING (xmax = 0)` (true for a fresh insert)
and deletes with `RETURNING`. Any mismatch is drift between the view and the
table. The flush raises, its transaction rolls back, and the watermark stays
put (`test_drift_between_view_and_table_is_refused_and_rolled_back`). A
second row for a key without a retraction is refused in the sink before
anything is written.

## §4. The bug the chaos suite found

The first version flushed periodically during the replay of (C, W]. Nothing
needed writing, since those deltas are dropped, but the flush still wrote
the watermark, set to the engine's frontier. That is **below W** during a
replay, so the watermark moved backwards. The rest of the replay then looked
new, was applied again, and the next delete found no row.
`test_crash_at_each_critical_point[flush:after_commit]` failed with:

    SinkError: analytics.order_details: delete of (587,) did not match the table (got None)

Three fixes:

- a flush while E ≤ W is a no-op (`replayed_flushes_skipped`);
- checkpoints use the engine's frontier, never the sinks' position;
- `flush` refuses to move a watermark backwards.

The deterministic regression test
`test_resume_far_behind_the_sinks_replays_without_applying_twice` fails
without the first fix, in 16 seconds, on the third guard.

## Idle sources: moving A without a transaction

**The problem.** A slot advances only when the consumer acknowledges.
Before this change the runner acknowledged only checkpoints, and the
engine's frontier moved only with applied transactions. Now suppose the
views' tables are quiet while the rest of the database is busy. Postgres
sends the slot nothing: since PostgreSQL 15 it skips transactions with no
published changes. So C never moves, A never moves, and the server keeps
every byte of WAL written since the last published change. On a busy
server that fills the disk. This is the well-known heartbeat problem of
CDC tools.

**What the server does say.** Between transactions, keepalives carry the
position up to which the walsender has decoded. Every transaction
committing before that point has already been sent, or skipped for having
no published changes. Postgres's own subscriber acknowledges these
positions. A keepalive *inside* a transaction is different: the
walsender sends a whole transaction while decoding its commit record, so
the position is past a transaction the consumer has only partly received.
Acknowledging it could lose that transaction.

**What the runner does.** The stream turns keepalives between
transactions into `Progress(P)` events, in stream order
(`replication/stream.py`, `progress=True`). The runner remembers the
latest P. When a checkpoint is due and nothing is pending, it:

1. advances the engine to P, which is valid: every transaction before P
   has been applied (stream order), and any applied after it already moved
   the frontier past P;
2. flushes the sinks through P. No rows change, only the watermark: W = P;
3. checkpoints at P, so C = P;
4. acknowledges P, so A = P.

This is the same order as for transactions, so A ≤ C ≤ W holds and the
argument above is unchanged. Every later transaction commits at or after
P, so the frontier still admits it. The cost is one watermark flush and
one checkpoint per checkpoint interval while idle.

**Tests.**

- `test_an_idle_source_does_not_pin_wal_on_a_busy_server` writes about
  2 MB of WAL to an unpublished table, then checks:
  - the slot's `confirmed_flush_lsn` passes the end of that WAL, with
    zero transactions applied;
  - a later published change still arrives;
  - the views stay exact.

  Without progress events it fails: the slot stays at its bootstrap
  position.
- The unit tests in `test_stream_progress.py` cover:
  - a keepalive inside a transaction is not progress;
  - repeated and lower positions are not reported;
  - progress is opt-in.

## The chaos suite

`tests/chaos/test_kill9.py` runs the real worker
(`deltaflow.devtools.worker`, four views) as a subprocess. A sustained load
runs against the source tables throughout.

- **`test_kill_9_at_random_moments`:** ten hard kills at random times,
  SIGKILL on Linux and TerminateProcess on Windows, each followed by a
  restart.
- **`test_crash_at_each_critical_point`:** one run per window too narrow to
  hit by timing: after the slot is created, after the sink bootstrap, with
  the state changed but the frontier not advanced, before and after the sink
  commit, a checkpoint written but not complete, and after a checkpoint
  before the slot ack. The crash is `os._exit` at the n-th pass, which
  skips cleanup exactly as `kill -9` does.

Afterwards a final worker catches up past a marker transaction. Every sink
table must equal its plain-SQL definition as a multiset, keyed tables must
have no duplicate keys, and the single-row table must have exactly one row.

**It is not vacuous.** Making the sink re-apply replayed transactions makes
the random-kill test fail: the worker hits drift and never catches up.

## What is not covered

- **Concurrent writers to a sink table.** Anyone else writing the table
  is detected as drift, not tolerated.
- **Sinks in a different database than the source.** `sink_dsn` exists,
  but the exactly-once argument holds per database. One flush transaction
  covers all of a runner's sinks, so they must share one.
- **Disk loss of the state directory.** That forces a bootstrap, which is
  correct but rebuilds from the snapshot.
