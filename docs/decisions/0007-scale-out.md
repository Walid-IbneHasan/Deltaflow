# 0007 — Scale-out: independent shard pipelines, leases in Postgres, no Raft (M7)

Status: accepted, 2026-10-19

The user asked for the recommended options to be taken without stopping
at each fork. These are the forks and the choices made.

## Fork 1: how shards get their data

**(a) Every shard is an independent durable pipeline with its own
replication slot.** It decodes the whole WAL and keeps only its rows.
Taking a shard over is exactly an M5 resume (or bootstrap) of that shard:
nothing new to get right.

**(b) One reader per worker, or a central router, fanning changes out to
shards.** That decodes the WAL once. But the router's progress must then be
reconciled with every shard's checkpoint and watermark. Moving a shard
means handing it over mid-stream, and the router is either a single point
of failure or needs its own failover.

**Chosen: (a).** It reuses the exactly-once machinery unchanged, and a
shard's correctness depends on nothing but its own slot, state and sink
rows. The cost:
- **N slots means N logical decodings of the same WAL.** Server CPU and
  WAL retention both grow with N.
- **Each shard reads the whole snapshot** at bootstrap.

That is fine at 6–16 shards and not at hundreds; (b) is the path beyond.

## Fork 2: what can be sharded

Only what is *exact* shard by shard, checked at registration:

- **joins co-partitioned on both sides' shard columns**, so every matching
  pair lives on one shard;
- **aggregates as the last step**, merged in a Postgres view: Count and Sum
  are summed, Min is the min of mins, Max the max of maxes, all exact;
- **distinct last**, merged with DISTINCT.

Refused:
- a map before a join (it could change the key);
- a second join;
- steps after an aggregation.

The alternative, re-partitioning between stages (an exchange operator, as
in Flink or differential dataflow), would allow arbitrary plans. It would
also need a shuffle protocol with its own frontiers. Not done.

## Fork 3: coordination, and why not Raft

The spec asks that this be defended: leader election via Postgres advisory
locks with lease renewal, rather than Raft.

**What is needed** is mutual exclusion per shard, with *fencing*: once a
shard has moved, the old owner must not write, even if it is still running
(hung, paused, partitioned).

**Postgres already serialises every write that matters.** A shard's only
side effect is its sink transaction, and that transaction commits in the
same Postgres that holds the lease. Fencing therefore needs no consensus
of its own:

1. **Owning a shard** means holding the advisory lock (class, shard) on a
   session. Postgres releases it when the session dies, at once for a
   killed process.
2. **Taking a shard bumps its epoch** in `_deltaflow.shard_leases`, under the
   row's exclusive lock.
3. **Every sink flush starts with** `SELECT … FROM shard_leases WHERE
   shard = $1 AND epoch = $2 FOR SHARE`, in the same transaction as its
   rows and watermark. If the epoch has moved on, the flush raises, and a
   bump cannot interleave with a flush. The old owner's writes are
   rejected by the same database that decides ownership.
4. **A hung holder** (lock held, heartbeat stale) has all its sessions
   terminated by the would-be owner. They are found by
   `application_name = deltaflow:<worker>`. Its replication slot is taken
   the same way. Step 3 makes this safe even if the hung process wakes up.

**Raft** would elect a leader among the workers. But the leader still could
not prevent a deposed worker from writing to Postgres, unless the sink
checked something in Postgres, which is step 3. Raft would add a second
source of truth that has to agree with the first, a log and membership
changes to operate, and nothing the Postgres lease cannot already
guarantee. If the sink were not transactional, or not in one database,
the argument would fail and real consensus or a fencing service would be
needed. That is the condition under which this decision should be
revisited.

## Consequences

- **Postgres is the coordinator.** If it is down, nothing runs anyway: it
  is also the source and the sink.
- **Failover takes about the TTL** (3 s in the tests), plus the shard's
  resume time.
- **Takeover resumes from the previous owner's checkpoints only if the
  state root is shared** (as in the tests, on one machine). On separate
  machines without shared storage, the new owner bootstraps the shard from
  a snapshot: correct, but slower.
- **Heartbeats run in their own thread.** A busy event loop (snapshot
  loads) once made live workers look dead, and they were evicted. Fencing
  kept the data correct, but it cost availability.
