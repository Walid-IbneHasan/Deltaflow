# How the replication reader works, and why it is correct

This is written for a reviewer who will push back. Every claim names the test
that checks it.

## Pipeline

```
TCP ── wire.py ── CopyData ── protocol.py ── XLogData(wal_start, payload)
                                                │
                              pgoutput.py: bytes → Begin/Relation/Insert/…/Commit
                                                │
                              changes.py: TransactionAssembler → typed Transaction
                                                │
                              stream.py: bounded queue → consumer → ack(end_lsn)
```

## 1. Nothing is emitted before COMMIT

`TransactionAssembler` buffers changes between `BEGIN` and `COMMIT` and
returns a `Transaction` only from the `COMMIT` arm. A transaction whose
`COMMIT` never arrives can never reach a consumer. This holds whether it
aborted, or the connection dropped mid-transaction.

pgoutput without `streaming` already omits aborted transactions, because the
server decodes a transaction only once it has committed. We do not rely on
that: the guarantee comes from the assembler. Consistency checks:

- `COMMIT`'s commit LSN must equal `BEGIN`'s final LSN;
- `BEGIN` must not nest;
- no change may arrive outside a transaction.

Tests: `test_nothing_is_emitted_before_commit` and
`test_protocol_violations_raise` (unit), plus
`test_rolled_back_transaction_is_never_delivered` (integration). Also the
differential replay: every run includes ~5% rolled-back transactions, and any
leak would show up as extra or changed rows.

**Cost:** the largest transaction is held in memory. Spilling large
transactions, or using protocol v2 streaming, is out of scope for M1.

## 2. Unchanged TOAST values

When an UPDATE does not modify an out-of-line (TOASTed) column, the server
does not re-read that value. It sends the marker `u` instead of the data.
Turning `u` into NULL, or into an empty string, would blank the column
downstream. That is the classic CDC bug.

The decoder keeps `u` as a distinct sentinel, `UNCHANGED_TOAST`. Value typing
passes it through untouched, and consumers must treat it as "keep the
previous value". The replay test does exactly that: it drops those columns
from the `SET` list.

Tests:

- `test_update_without_old_tuple_keeps_unchanged_toast_placeholder` (unit).
- `test_update_leaving_toasted_column_untouched` (integration). This test
  fails if the decoder maps `u` to NULL; the mutation was tried.
- The differential replay. On seed 12345 it saw 43 unchanged-TOAST
  placeholders across 59 out-of-line rows. Mapping `u` to NULL makes it fail
  with a row diff.

## 3. What an UPDATE/DELETE tells you about the old row

The old row depends on the table's replica identity:

| identity | UPDATE, key unchanged | UPDATE, key changed | DELETE |
|---|---|---|---|
| default (PK) | no old tuple | `K`: key columns only | `K`: key columns only |
| FULL | `O`: whole old row | `O` | `O` |

In a `K` tuple the non-key columns are sent as NULL placeholders. They are
*not* the old values. The assembler therefore reduces a `K` tuple to its key
columns (`old == {"id": 3}`), so no consumer can mistake a placeholder for a
real NULL. `RowUpdate.key` / `RowDelete.key` always give the identity of the
row *before* the change.

Tests: `test_primary_key_update_sends_old_key` and
`test_replica_identity_full_sends_whole_old_row` (integration), plus
`test_update_keeps_unchanged_toast_sentinel_and_key_only_old_row` (unit).

Retracting a row from a view needs the whole old row. The reader delivers
exactly what the server sends. Since M2, views get full old rows (and
unchanged TOAST values) from Deltaflow's own arrangement of each source
table. See [decision 0003](decisions/0003-source-arrangements.md) and
[delta-algebra.md](delta-algebra.md) §2.

## 4. LSNs and acknowledgement (at-least-once)

- Every change carries the `wal_start` of its XLogData message.
- Every transaction carries `commit_lsn` and `end_lsn`.
- The consumer calls `stream.ack(txn.end_lsn)` once it has *durably* processed
  the transaction.
- Status updates report `flush = apply = acked LSN`, and the server stores
  `flush` as the slot's `confirmed_flush_lsn`. A transaction whose `end_lsn`
  is at or below that point is never sent again.
- Nothing else is ever reported as flushed. In particular, keepalive
  `wal_end` positions are never auto-acknowledged.

Consequences:

- After a crash, everything not acknowledged is re-sent, so delivery is
  **at-least-once**.
- The server persists `confirmed_flush_lsn` lazily, so even acknowledged
  transactions can come back after a *server* crash.
- Exactly-once is therefore the sink's job (M5): it writes its own watermark
  in the same transaction as the data and drops anything at or below it.
- An idle reader on a busy server retains WAL until something on its
  publication commits and gets acknowledged. This is safe, at the cost of
  disk. Acknowledging keepalive positions when nothing is in flight would fix
  it; that is deferred until it can be tested properly.

Tests: `test_ack_advances_confirmed_flush_lsn` and
`test_resuming_a_slot_skips_acknowledged_transactions` (both integration).
The second acknowledges one of two received transactions, reconnects, and
checks that only the unacknowledged one comes back.

## 5. Heartbeats and backpressure

The reader task pushes transactions into a bounded queue (`max_pending`). If
the consumer is slow, the queue fills and the reader stops reading the
socket. The kernel's TCP buffers then fill, and the walsender blocks. That
pushes backpressure all the way to the server, with no unbounded buffering in
our process.

A separate heartbeat task sends status updates every `status_interval`.
Without it, a blocked reader would stop replying and the server would drop
the connection after `wal_sender_timeout`. Keepalives with
reply-requested are answered immediately.

Shutdown never cancels the reader in the middle of a message. `close()` sends
CopyDone and lets the reader drain to the server's own CopyDone and
`ReadyForQuery`.

**Not yet covered by a dedicated test:** a consumer slower than
`wal_sender_timeout`. That belongs in the M6 backpressure work.

## 6. Initial snapshot to stream hand-off

`create_slot(..., export_snapshot=True)` returns a snapshot name. A
`REPEATABLE READ` transaction that runs `SET TRANSACTION SNAPSHOT` sees
exactly the database as of the slot's consistent point. The slot then streams
exactly the transactions that commit after that point. Copy the snapshot,
then stream, and nothing is missed or duplicated.

The exported snapshot stays valid only until the next command on the
replication connection, so the copy must finish before `START_REPLICATION`.
The differential test does this.

## 7. Typed values

Values are typed by column OID through psycopg's text loaders: int, numeric
(scale preserved), timestamptz, bool, bytea, and so on. A type with no loader
(e.g. a user enum) comes through as `str` holding Postgres's own text form.
That is lossless, not an approximation.

Timestamps arrive as UTC because the startup packet pins `TimeZone=UTC`. The
loader honours the offset either way.

## Not handled (by design, for now)

- Protocol v2+ features: streaming of in-progress transactions, two-phase
  commit, binary tuple format. They are never requested; if they appear, the
  decoder raises instead of guessing.
- Schema changes are followed via RELATION messages: later changes decode
  against the new column list. Mapping a view's columns onto a changed table
  is a planner concern. Since M2 a column change on a live source raises
  `SchemaChangedError`.
- TLS on the replication connection. See decision 0002.
