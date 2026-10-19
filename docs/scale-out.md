# Scale-out: why sharded results are exact, and why failover is safe

This is written for a reviewer who will push back. Every claim names the test
that checks it. The design choices are in
[decision 0007](decisions/0007-scale-out.md).

## 1. Rows to shards

`Deltaflow(shards=N, shard_by={"public.orders": "customer_id",
"public.customers": "id"})`. A row belongs to shard
`hash(canonical(shard key)) mod N`. The canonical encoding (state/codec.py)
gives equal values equal bytes, so `1`, `1.0` and `Decimal("1.00")` share a
shard.

Each shard's engine sees the whole stream and keeps, in its arrangements,
only the rows it owns. When an UPDATE changes a row's shard key:

- **the old shard** has the row: it retracts it and does not keep the new
  version (`moved_out`);
- **the new shard** does not have the row: it inserts the new version
  (`moved_in`);
- **every other shard** has neither, and ignores the change.

One case cannot be handled. A row moving in with an unchanged-TOAST
placeholder has its value on the shard it came from, so it raises
(`test_moving_into_a_shard_with_an_unchanged_toast_value_is_refused`). In
the seed schema, orders move (their customer changes) but have no large
columns, and customers (with large bios) are sharded by their primary key
and never move.

**What sharding costs the consistency checks.** Unsharded, an UPDATE or
DELETE of a missing key raises, because it means a lost change. Sharded,
it is usually another shard's row and is ignored. The check is weaker by
necessity.

## 2. Why per-shard results merge exactly

| View shape | Why the partials combine exactly |
|---|---|
| filter / map / join | a joined row exists only where both sides' shard key hashes; a co-partitioned join (both sides sharded by the join key) puts every matching pair on one shard. Partials are disjoint; the union is the result |
| distinct | `DISTINCT` of the union |
| group_by + Count / Sum / Min / Max | Count and Sum add across shards; the min of the shards' minima is the minimum (and so for max); a NULL partial is skipped, as SQL does |

Anything else is refused at registration (`test_views_that_cannot_be_sharded_exactly_are_refused`):

- a join not on both shard columns;
- a map before the join (it could change the key);
- a second join;
- any step after an aggregation (the merge must see the aggregate columns
  unchanged).

The merge is generated as a Postgres view over the partial table
(`planner/merge.py`, `merge_view_sql`). The same rules are implemented in
Python (`merge_partials`) for the property test:

- **`test_merged_shards_equal_the_unsharded_engine`** runs three shard
  engines and one unsharded engine over random transactions. The workload
  includes a join, all four aggregates, a global aggregate, distinct, rows
  moving between shards, deletes and unchanged TOAST. After **every**
  transaction the merged partials equal the unsharded results. Keeping
  moved-out rows on their old shard makes it fail.
- In the acceptance test, merging `Min` partials with `max()` makes it
  fail. The global `smallest` comes out as the largest of the shards'
  minima.

## Running a cluster

```sh
deltaflow cluster app.views:sharded --worker-id w1 --slot-prefix app   # on each worker
```

`app.views:sharded` is a function returning a fresh sharded registry: every
shard runs its own copy, with its own sinks. Before starting, the worker
runs `prepare_cluster`, under an advisory lock so that workers starting
together do it once:

- the coordination tables;
- each view's merge view `<table>` over the partial table
  `<table>__shards`. You create the partial table: a leading `shard`
  column, which also leads its primary key, then the view's columns;
- a publication covering the views' tables. Missing tables are added,
  nothing is dropped.

`deltaflow status` shows the workers' heartbeats and the shard leases.
`deltaflow reset --slot-prefix app` drops the shard slots and state, and
is refused while any of them is in use.

## 3. Shards to workers, and what happens when one dies

Each worker (`runtime/cluster.py`) loops every `heartbeat` seconds:

1. **heartbeat** into `_deltaflow.workers`, from its own thread and
   connection;
2. **work out which shards are its own:** `owner(shard, live workers)`,
   rendezvous hashing over the workers whose heartbeat is younger than
   `ttl`;
3. **acquire** each of its shards it does not hold yet: take the
   session-level advisory lock (class, shard), bump the shard's epoch, and
   start its DurableRunner, fenced by (shard, epoch);
4. **release** shards that belong to someone else now (a worker joined):
   stop the runner cleanly with a final checkpoint, then unlock.

**A worker is killed** (the acceptance test):

- its sessions close, so its advisory locks are freed at once;
- its heartbeat stops, and after `ttl` the survivors' rendezvous gives its
  shards to them;
- they take the locks and bump the epochs. Each shard's runner resumes from
  the dead owner's last checkpoint (shared state root) and replays its
  persistent slot from there. The sinks drop what is at or below their
  watermark. Exactly-once is the M5 argument, per shard.

**A shard pipeline fails on a live worker** (a lost connection, a
missing sink table):

- the worker releases the shard and takes it again at a new epoch;
- it waits first, starting at 0.5 s and doubling up to 30 s, and starts
  over after a minute of running. Retrying on every heartbeat would bump
  the epoch and reconnect several times a second for as long as the cause
  lasted. `test_a_failing_shard_is_retried_with_backoff` counts the
  attempts: at most 5 in 5 s, where one per heartbeat was 43.

**A worker hangs but its connections stay open** (the dangerous case):

- its heartbeat stops, the survivors want its shards, and the advisory
  lock is still held;
- the would-be owner sees that the lock's holder has a stale heartbeat and
  terminates *all* of that worker's sessions, which are tagged
  `application_name = deltaflow:<worker>`. Terminating only the lock
  session would not be enough, because a flush the stale worker left open
  still holds its share lock on the lease row. A replication slot still
  held by the old walsender is taken the same way;
- the epoch bump runs under `lock_timeout = 2s`. If the lease row is still
  busy, the worker lets go of the advisory lock, terminates the previous
  owner's sessions if that owner is not live, and retries on the next tick.
  The coordinator loop never blocks behind a dead worker's transaction;
- **fencing does the rest**: the hung worker's next flush finds a newer
  epoch under `FOR SHARE` and raises before writing anything.

Tests:

- `test_a_flush_with_a_stale_epoch_is_refused_and_writes_nothing` checks
  that no row and no watermark is written.
- `test_a_hung_lock_holder_is_evicted_and_its_shard_taken_over` checks
  that the zombie's lock backend is terminated, and that the shard is taken
  at epoch 6, one past the zombie's 5.

## 4. What testing found

- **Heartbeats on the event loop made busy workers look dead.** With
  several shards bootstrapping in one process, CPU-bound snapshot loads
  held the loop for seconds. Heartbeats went stale, so peers terminated
  the busy worker's lock connection, and it crashed. Fencing kept the data
  correct throughout, but shards churned: 511 acquisitions in one run. The
  heartbeat now runs in a thread, and losing the lock connection drops all
  shards and reconnects instead of crashing. The same start-up now shows
  10 acquisitions and 4 releases, all of them the rebalancing as workers
  join.
- **Re-seeding silently emptied the publication.** `DROP TABLE` removes a
  table from its publications, and set-up only created the publication if
  it was missing. Every shard then refused to start: the M2 check "the
  publication does not include public.orders" doing its job. Set-up now
  re-adds missing tables (`ALTER PUBLICATION … ADD TABLE`; never DROP while
  others stream).

- **A slow checkpoint freezes the whole worker, heartbeat thread included.**
  rocksdict holds the GIL during a checkpoint. On the Windows host, where
  checkpoints sometimes take 20–30 s, a demo run with three workers
  produced:
  - a lopsided assignment (one worker held 5 of 6 shards, because its
    peers were frozen when it started);
  - a 24.9 s failover.

  The results were still exact. The cluster CLI's default TTL is therefore
  10 s, and should exceed the host's slowest checkpoint.
- **Failover waited on a dead worker's open transaction.** In demo runs on
  the Windows host, failover took 24.9 s and then 40.8 s, against a 10 s
  TTL. The new owner's epoch bump queued behind a flush transaction the
  killed worker had left open: it still held its `FOR SHARE` lock on the
  lease row until Postgres noticed the dead client. Two changes fixed it:
  - eviction now terminates every session of the stale worker, found by
    `application_name`, not just its lock session;
  - the bump runs under a lock timeout, with retry.

  The same demo now fails over in 10.6 s, about the TTL. The acceptance
  test (TTL 3 s) fails over in 3.2 s.

## Not done (M7)

- **One reader feeding many shards.** Every shard decodes the whole WAL
  itself (decision 0007).
- **Re-partitioning between stages.** No exchange operator, so no
  aggregate of a join keyed differently from the join.
- **Changing the number of shards.** N is fixed at registration; changing
  it means re-bootstrapping every shard.
- **Shared state across machines.** On one machine the state root is
  shared and takeovers resume. Elsewhere, a takeover bootstraps the shard
  from a snapshot.
