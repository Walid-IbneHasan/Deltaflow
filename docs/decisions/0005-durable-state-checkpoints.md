# 0005 — Durable state by checkpoints at transaction boundaries (M5)

Status: accepted, 2026-10-19

## Context

M5 needs exactly-once results that survive `kill -9`. The choice made
at the end of M4 was to **resume from persisted state** instead of
rebuilding from a fresh snapshot on every restart. A rebuild costs time proportional
to the source tables; resuming costs time proportional to the gap since the
crash.

What remained open is how to make the state consistent at a known LSN. One
transaction writes many keys across arrangements, join indices and
aggregate groups, and a crash halfway through must not leave half of it.

## Options

**(a) One atomic RocksDB write batch per transaction.** Every write of a
transaction goes into a batch, committed with an "applied LSN" key.
Operators read their own writes within a transaction (a join probes what
the other side just folded in; an aggregate reads its group back), so the
batch needs a read-your-writes overlay. Python has no sorted structure in
the stdlib that makes prefix scans over such an overlay cheap, and the
alternatives cost a dependency or O(n) scans. The WAL must also be on, and
every transaction pays an fsync or group commit.

**(b) Checkpoints at transaction boundaries.** The live database runs with
the WAL off; partial transactions there do not matter. Every few seconds,
between transactions and after the sink is flushed, RocksDB's `Checkpoint`
makes a consistent copy: memtables are flushed, and SST and blob files are
hard-linked. A crash discards `live/`, the newest complete checkpoint is
restored, and the slot replays from it.

## Decision

(b). It needs no read-your-writes overlay and no per-transaction fsync, and
the replication slot already is the redo log. The cost is replaying up to
`checkpoint_interval` seconds of transactions after a crash, and the slot
retaining WAL for that long.

## Consequences

- **Exactly-once rests on A ≤ C ≤ W:** slot acknowledged ≤ checkpoint ≤
  sink watermark. See [docs/exactly-once.md](../exactly-once.md).
- **Replay must be deterministic**, which it already is: operators are
  deterministic, and user functions must be (delta-algebra.md §5).
- **Durable runs need durable sinks.** A `MemorySink` cannot be restored
  and is refused by `DurableRunner`.
- **The state layout is versioned only implicitly.** A change of views
  (plan fingerprint) forces a bootstrap. A change of Deltaflow's own
  storage format would need the same; no migration exists.
- **Disk.** Two checkpoints are kept. They share immutable files through
  hard links, so the extra disk is mostly what changed between them.
