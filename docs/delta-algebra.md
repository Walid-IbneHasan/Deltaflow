# Deltas, timestamps and frontiers, and why views are exact

This is written for a reviewer who will push back. Every claim names the test
that checks it. The replication side (what a `Transaction` is and why it is
complete) is in [replication.md](replication.md).

## Pipeline

```
Transaction (commit_lsn, end_lsn, changes)
  │  state/arrangement.py   change → whole-row (row, ±1), old rows and TOAST filled in
  ▼
Delta(row, time=commit_lsn, diff)   consolidated per source
  │  operators/             Source → Filter / Map … (linear, time-preserving)
  ▼
Sink.accept(batch)                  pending until its time is complete
  │  runtime/engine.py      advance every frontier to end_lsn
  ▼
Sink.advance(frontier)              complete times folded into the visible result
```

## 1. The delta model

Every record is `(row, time, diff)`. A collection, such as a table or a view
result, is the sum of all its deltas: a multiset whose multiplicities are all
positive.

- An INSERT is `(new, t, +1)`.
- A DELETE is `(old, t, −1)`.
- An UPDATE is both, at the same time `t`.

`consolidate` merges equal `(row, time)` pairs by summing their diffs and
drops zeros.

The algebra laws are property-tested (Hypothesis, `tests/unit/core/`):

- multisets form an abelian group;
- consolidation is canonical and idempotent, ignores arrival order, can be
  done incrementally, and accumulates to the same collection at every
  frontier;
- accumulation is a homomorphism.

Consolidation is also what makes a view see whole transactions: an INSERT
followed by a DELETE of the same row in one transaction cancels out, so
the view never sees it. That matches SQL, where no other session sees it
either.

## 2. Where the old row comes from

pgoutput does not send the old row (see
[decision 0003](decisions/0003-source-arrangements.md)), so each source table
has an *arrangement*: a copy of the table keyed by its replica identity. An
UPDATE's retraction is the stored row, not anything reconstructed from the
message. Unchanged out-of-line values are filled in from the same stored row.

Tests:

- `test_accumulated_deltas_equal_the_table_after_every_change`
  (property-based, default and FULL identity, with unchanged-TOAST
  placeholders). After every change, the accumulated deltas equal a model
  of the table, and every retraction is exactly a row that was there.
- **Mutation 1:** building the retraction from the message's new values
  instead of the stored row (what you get without old rows) makes the M2
  differential fail at the first update. The retraction then cancels the
  insertion and the stale row stays in the view.
- **Mutation 2:** turning unchanged TOAST into NULL makes it fail at
  transaction #62 (seed 12345): the `customer_bios` digest shows the blanked
  bio.

## 3. Timestamps are commit LSNs

A delta's time is the `commit_lsn` of the transaction it came from.

*Why that is a valid timestamp.* Logical decoding delivers each transaction
whole, at its commit, in commit order. Interleaved concurrent transactions
are reordered into commit order by the server's reorder buffer. Commit
LSNs therefore strictly increase along the stream, and two transactions
never share one. Concurrent transactions that both committed are
serialised exactly as Postgres serialised their commits, which is also
the order in which a fresh `SELECT` sees them.

*Why advancing the frontier to `end_lsn` is sound.* `end_lsn` is the first
byte after the transaction's commit record. Any later transaction's commit
record is written after that, so its `commit_lsn ≥ end_lsn`. Once a
transaction is applied, no delta can arrive at its `commit_lsn` or earlier.
That is the definition of the frontier having passed it.

This argument is not taken on trust. `Engine.apply` raises
`FrontierViolationError` if a transaction commits behind the current
frontier (test: `test_a_transaction_behind_the_frontier_is_rejected`), and
`Source.process` and `MemorySink.accept` check every delta against their
frontiers.

*The snapshot.* The exported snapshot contains everything committed before
the slot's consistent point, and the slot streams everything committed
after it. Snapshot rows are installed at time 0. `Engine.start` then
advances every frontier to the consistent point, which completes the
snapshot as one atomic state. The first streamed transaction commits at or
after the consistent point; if one did not, `apply` would raise rather
than double-count it.

*No separate Timestamp type.* With one replication stream, time is totally
ordered and an LSN carries all the information. Partially ordered time
(iteration, multiple independent streams) is not needed by any milestone so
far.

## 4. Frontiers

A frontier is a lower bound on the time of every delta that may still
arrive. A time `t` is complete once `t < frontier`. Because time is totally
ordered, a frontier is one LSN, or `CLOSED` when nothing more can arrive.
(In general a frontier is an antichain; over a total order it has at most
one element.)

- **Sources** own their frontier. The engine advances all of them to
  `end_lsn` after each transaction. `MutableFrontier.advance` refuses to
  move backwards, since that would un-complete results already published.
- **Operators** derive theirs as the *meet* (minimum) of their inputs'
  frontiers. An operator's output can be complete only where all of its
  inputs are. Tested: `test_meet_is_a_semilattice` and
  `test_frontier_of_a_derived_operator_is_the_meet_of_its_inputs`.
- **Sinks** hold deltas until the frontier passes their time and only then
  fold them into the visible result. A view's result is therefore always a
  state the database actually passed through, as of a whole transaction.
  It is never half of a transaction, and never a mix of two. Tested:
  `test_deltas_become_visible_only_once_their_time_is_complete`.

*Why the meet matters even though M2 has one stream.* M2 views have a single
input, so the meet is trivially that input's frontier. In M3, a join of
`orders` and `customers` has two inputs. If one input lagged, emitting the
join at the other input's frontier would publish a result missing the
lagging side's changes: approximately right, which is wrong. The meet
rule makes that impossible by construction. Both of today's sources ride
the same replication stream and advance together, but the operator does
not rely on that.

*Idle frontiers.* A frontier advances only when a transaction arrives. A
view on an idle database is correct but its frontier stops moving, so
freshness (now minus frontier) grows while correctness does not change.
Advancing on keepalive positions is deferred (see replication.md §4) and
belongs with the M6 lag metrics.

## 5. Map and filter are linear, and must be deterministic

Both operators act on each delta independently and keep its time and diff,
so `op(a + b) = op(a) + op(b)` and `op(−a) = −op(a)`. Linearity is what
makes "apply to the deltas, then accumulate" equal "accumulate, then apply
to the whole collection" at every frontier. That equality is the
incremental-equals-recompute property, and it is property-tested for three
operators, including a non-injective projection
(`tests/unit/operators/test_map_filter.py`).

The retraction of an UPDATE re-runs the function on the *old* row, so it
cancels the earlier output only if the function returns the same result
both times. **User functions must be deterministic:** no clocks, no random
numbers, no mutable globals.

What is and is not caught:

- A non-deterministic **map** retracts a row the view never contained, the
  multiplicity goes negative, and `MemorySink` raises
  `NegativeMultiplicityError` instead of publishing (test:
  `test_a_non_deterministic_map_is_caught_not_published`).
- A non-deterministic **filter** that accepted a row on insertion and
  rejects it on retraction just drops the retraction. The row stays in the
  view and nothing goes negative. That case is **not detected**. Detecting
  it would mean storing every filter decision per key, which is a state
  cost the spec's linear operators should not pay. The requirement is
  documented instead.

Rows are immutable (`core.Row`), so a function cannot corrupt the
arrangement by mutating the row it is handed.

## 6. NULLs: Python predicates are not SQL WHERE clauses

SQL's `WHERE` keeps a row only if the predicate is TRUE, so NULL ("unknown")
drops the row. Python has no unknown: `None > 5` raises, and `None != 'x'`
is `True`. A predicate ported from SQL therefore has to say what it does
with NULLs, and the differential test's pairs do so explicitly. For
example, `note IS NULL OR status = 'cancelled'` becomes
`r["note"] is None or r["status"] == "cancelled"`.

`Filter` insists the predicate return a `bool`. A predicate that returns
`None`, the natural result of `r.get(...)`, raises `TypeError` instead of
being read as False (test: `test_filter_requires_a_bool`).

## 7. Value equality across the two load paths

Snapshot rows are read with psycopg's binary loaders; streamed rows are
decoded from pgoutput's text form with the text loaders. A row that came
from the snapshot is later retracted using a row the stream produced, so
the two paths must produce *equal* Python values. Otherwise the retraction
would not cancel. The `orders_all` identity view in the differential test
checks exactly this, across `bigint`, `text`, `numeric(12,2)` and
`timestamptz`:

- `numeric` arrives as `Decimal`. Equality, and therefore hashing, ignores
  trailing zeros.
- `timestamptz` arrives as aware `datetime`, which compares by instant
  whatever the session time zone.

**Known gap: NaN.** In Python, `float('nan') != float('nan')` and
`Decimal('NaN') != Decimal('NaN')`, but SQL treats NaN as equal to itself.
A NaN in a source column would make its own retraction miss and raise
`NegativeMultiplicityError`. It fails loudly, it is not silently wrong, but
it is unsupported. The seed schema has no floats.

## What the M2 differential test does and does not show

It shows that after every one of 1,000 lockstep transactions, plus a
600-transaction concurrent burst, eight views equal their SQL definitions
exactly, as multisets (see [milestones/M2.md](milestones/M2.md)).

It compares only at frontiers, so it cannot see whether a sink ever exposed
a partial transaction. That property is covered by the sink unit tests and
the engine property test, which check `sink.frontier == end_lsn` and the
result after every transaction.
