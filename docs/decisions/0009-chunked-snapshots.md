# 0009 — Chunked snapshots: bootstrap without holding back VACUUM

Status: accepted, 2026-10-20

A bootstrap used to read every source table inside one REPEATABLE READ
transaction on the slot's exported snapshot. That transaction holds the
server's xmin for the whole load. While it runs, VACUUM cannot remove a
dead row anywhere on the server, in any table, so tables and indexes bloat
for as long as the load takes. On a large database that is hours, and a
database team will refuse to run the tool.

The same bootstrap also could not make progress durable. A crash at 90 %
started over from zero, and the slot kept every byte of WAL written since
the load began.

## Fork 1: how a chunk is tied to a position in the stream

The table is read in chunks, each in its own short transaction, while the
replication stream runs. Each chunk shows the table at some moment. The
stream has to know which of its transactions that moment includes.

**(a) Low and high watermarks around the read**, as DBLog (Netflix),
Debezium's incremental snapshots and Flink CDC do: read
`pg_current_wal_lsn()` before and after the chunk's SELECT, and apply the
stream's changes between the two over the chunk.

This is not exact in Postgres. A transaction's commit record goes into the
WAL before the transaction becomes visible, and with synchronous
replication the gap is as long as the standby takes to answer. A
transaction can therefore commit before the low watermark and still be
missing from the chunk. Its change is then applied to nothing, and lost.
It is rare, and silent: the "no silent approximation" rule excludes it.

**(b) A temporary logical slot per chunk**, created with an exported
snapshot. Postgres's snapshot builder guarantees the correspondence that
(a) can only approximate. The exported snapshot contains exactly the
transactions that commit before the slot's consistent point CP, and none
that commit at or after it. That is the same guarantee the single-snapshot
bootstrap relied on, applied per chunk. Each chunk is read in a
transaction of its own, so xmin is held for one chunk, not for the whole
load.

Costs:
- one more replication connection and slot (`max_wal_senders`,
  `max_replication_slots`) while loading;
- one slot creation per chunk. Creation waits for the transactions running
  at that moment to finish: milliseconds on OLTP workloads, but an open
  idle-in-transaction session stalls it. The loader names the blocking
  sessions in its log.

**Chosen: (b).** Exact, and it uses Postgres's own mechanism.

## Fork 2: merging chunks with the stream

The stream runs from the main slot's position C0, while chunk i shows the
table as of its own CP_i (CP_i > C0, a different point for every chunk).

**(a) Range membership.** Chunk i covers the key range (b_{i-1}, b_i]. A
change to key k is applied if k's range is loaded and the change commits
at or after that range's CP. This needs Python to order keys exactly as the
primary-key index does, and for text it cannot: a key's collation (glibc or
ICU) decides the order, and the index pages chunks in that order.

**(b) Latest writer wins, per key.** With the default replica identity, an
INSERT or UPDATE carries the whole new row and a DELETE carries the key, so
every change to a key *overwrites* it. A chunk read at CP_i is also an
overwrite of every key it returns, as of CP_i. The loader records, per key,
the position of its last writer (its *mark*). A change at L to key k:

- **k has a mark w, L < w:** a chunk newer than the change already holds
  k; the change is ignored;
- **k has a mark w, L ≥ w:** the arrangement holds k exactly as of w, and
  every change since has been applied. The change is applied as when live,
  with the same consistency checks (an UPDATE of a missing key still
  raises);
- **k has no mark:** no chunk has read k yet, so a later chunk will read it
  and overwrite it. An UPDATE or DELETE is skipped. An INSERT is applied,
  and its position becomes k's mark: the key may lie in a range already
  read, where no later chunk will see it.

A chunk merges into the arrangement unconditionally. The stream applies
nothing during a read, and every change applied before it commits before
the chunk's CP. Keys outside the chunk need no tombstones. A key the
arrangement holds that the chunk shows as gone must have been deleted
after its mark and before CP, and that delete arrives later and is applied.

**Chosen: (b).** It needs no key comparison in Python, so collations,
enums and composite keys page in the index's own order. It keeps the
stream's consistency checks for every key that has a mark.

A change the loader cannot apply goes on a *re-read* list: an UPDATE whose
unchanged TOAST values it does not hold yet, or a sharded row moving in.
The key is then read again with a fresh exported snapshot, as a one-key
chunk. Being a newer writer, the re-read settles the key exactly.

**Going live.** Once every table has been read, the re-read list is empty
and the stream has passed the last chunk's CP, every change older than any
chunk has arrived. The arrangements then equal the tables as of the
engine's frontier. The views are built from the arrangements: the same
mini-plan that rebuilds a changed view (engine.py, `_rebuild`). Then every
sink's contents are replaced at that position, as the old bootstrap did.

## Fork 3: progress during a load

**(a) Nothing durable until live.** It is simple, but a crash restarts the
load, and the slot pins WAL for the whole load.

**(b) Checkpoints during the load.** The state (arrangements, marks,
re-read list) and the paging position of every table are checkpointed at
transaction boundaries, like live state. The slot is acknowledged up to
the checkpoint. A restart restores the checkpoint, replays the stream from
it, and reads the next chunk. The views have no state yet, and the sinks
are replaced when the load goes live, so no watermark is involved.

**Chosen: (b).** A crashed load resumes where it stopped, and WAL is
released as the load proceeds.

## What stays

`snapshot="single"` keeps the one-transaction bootstrap: fastest for small
databases, where holding xmin for seconds does not matter. The default is
`"chunked"`.
