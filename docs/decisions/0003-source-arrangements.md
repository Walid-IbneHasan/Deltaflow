# 0003 — Full old rows come from Deltaflow's own arrangement of each source (M2)

Status: accepted, 2026-10-18 (fork (b) from the end of M1)

## Context

An UPDATE is the delta pair (old row, −1), (new row, +1), and a DELETE is
(old row, −1). Every operator downstream needs the *whole* old row to
retract. Moving an order to another customer has to subtract its `amount`
from the old customer's sum. With the default replica identity, pgoutput
sends much less:

| change | what arrives |
|---|---|
| UPDATE, key unchanged | the new row only; untouched out-of-line columns are the `UNCHANGED_TOAST` placeholder |
| UPDATE, key changed | the new row, plus the old row's key columns |
| DELETE | the old row's key columns |

## Options

**(a) `REPLICA IDENTITY FULL` on every source table.** Postgres writes the
whole old row into the WAL, so Deltaflow needs no copy. But every UPDATE and
DELETE on the *source* database gets a larger WAL record, and the user has
to `ALTER` their tables for a reader's sake. Out-of-line values left
untouched by an UPDATE still arrive as placeholders in the new row, so (a)
alone does not solve TOAST.

**(b) Deltaflow keeps a keyed copy (an "arrangement") of each source
table.** It is loaded from the slot's exported snapshot and maintained from
the stream. The old row and the unchanged TOAST values are both looked up
there. Postgres is untouched. The cost is storage proportional to the source
tables.

## Decision

(b). It is one mechanism for both missing pieces (old rows and unchanged
TOAST), it leaves the source database alone, and M3's join indices are the
same kind of keyed state.

## Consequences

- **Memory.** In M2 every source table is held in memory. This is the
  dominant cost of a view until M3 moves arrangements onto the on-disk
  state backend under a memory budget.
- **Every start is a full snapshot.** Arrangements are not persisted in
  M2, so the runner always uses a temporary slot and re-reads the snapshot.
  Durable state and resume from a watermark are M5.
- **Consistency checking for free.** The arrangement knows what is there,
  so the stream cannot silently disagree with it. It raises
  `SourceStateError` on any of these:
  - an INSERT of an existing key;
  - an UPDATE or DELETE of a missing key;
  - an UPDATE that moves a row onto an existing key;
  - a placeholder in an INSERT or in a key column;
  - under FULL, an old row that differs from the stored one.
- **A key is required.** Sources need a primary key (or `REPLICA IDENTITY
  USING INDEX`). A table without one is rejected before streaming, and so
  are `REPLICA IDENTITY NOTHING`, FULL without a primary key, and
  non-ordinary tables.
- **FULL still works.** Under FULL, the arrangement keys rows by the primary
  key and cross-checks the server's old row against its own copy. Tested in
  `test_views_track_each_kind_of_change_with_replica_identity_full`.
- **Schema changes raise.** If a source's columns change mid-stream,
  `SchemaChangedError` is raised instead of the old rows being guessed into
  the new shape.
