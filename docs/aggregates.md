# Aggregates: group_by, count, sum, min/max under retraction, distinct

This is written for a reviewer who will push back. Every claim names the test
that checks it. The delta model is in [delta-algebra.md](delta-algebra.md),
the state store in [join.md](join.md).

## 1. How a group changes

`GroupBy` keeps one record per group in the state store, under the
canonical encoding of the group key. The record holds:

- the number of rows in the group;
- one state per aggregate;
- **the output row it last emitted.**

A batch at time `t` is split by group. Each touched group's record is
updated and its output recomputed from the record. The operator then emits
`(old output, −1), (new output, +1)`. It emits only the retraction if the
group became empty, only the addition if it is new, and nothing if the
output did not change.

The retraction is the *stored* output, not a recomputation of "what the
output was". That makes the retraction cancel exactly what was emitted
earlier, whatever happened in between.

A batch spanning several times is processed one time after another, in
order, each emitting at its own time
(`test_a_batch_spanning_times_emits_per_time`). As in the join, a delta
older than the state already folded in raises `FrontierViolationError`.

SQL semantics, all tested against Postgres:

| Case | Behaviour |
|---|---|
| NULL group keys | form one group (the canonical encoding encodes NULL as a value) |
| `Count()` | `COUNT(*)`: rows, including all-NULL rows |
| `Count(col)`, `Sum`, `Min`, `Max` | ignore NULL values |
| `Sum`/`Min`/`Max` of no non-NULL values | NULL |
| a group with no rows | disappears |
| a global aggregate (`.agg()`, no keys) | always exactly one row: `(0, NULL, …)` over an empty input |

The global row exists before any input. The engine emits operators'
`initial()` deltas at time 0 when it is built, so even an empty table's
global aggregate is visible once the snapshot completes
(`test_a_global_aggregate_over_an_empty_table_has_one_row`).

## 2. Count and Sum: invertible, so they keep a number

Count and Sum form groups: every update has an inverse, so a retraction
just subtracts. Their state is a number, or for Sum a pair
(non-NULL count, total). The non-NULL count is what tells "sum of nothing",
which is NULL, apart from "sum that happens to be 0".

**Exactness, not approximation.**

- Python ints are exact.
- Decimal sums run in a private context of 1000 significant digits that
  *traps* `Inexact`. A sum that cannot be held exactly raises
  `AggregateError` instead of rounding. The default 28-digit context
  rounds silently: `Decimal(10**30) + Decimal("1E-30") == Decimal(10**30)`
  is true there, and `test_sum_is_exact_or_refuses` checks the operator
  gets it right.
- **Floats are refused** by `Sum`. Float addition is not associative, so
  an incremental float sum after a million updates need not equal SQL's
  recompute: approximately right, i.e. wrong. Bools are refused too
  (`True` is an int in Python; `sum(bool)` is a type error in SQL).

The refusal happens when the first float arrives, not at registration.
The DSL cannot know a column's type before a map has run. This is the one
place where "raise at registration" is "raise at the first value", and the
error says why.

## 3. Min and Max under retraction

Min and max are not invertible. Knowing the current minimum and that it was
just deleted says nothing about the next one. The operator must keep
enough to answer without going back to the source, which here means every
value.

**State.** Per Min/Max aggregate there is a keyspace of entries:

    aggregate index ‖ canonical group key ‖ order-preserving value  →  count ‖ value

- **The minimum is the first entry** under `aggregate index ‖ group key`,
  and the **maximum the last**. Both are found with one seek in the sorted
  LSM, without scanning the group.
- **Inserting a value** increments its entry's count.
- **Retracting a value** decrements it, and deletes the entry at zero.
- **Deleting the current extreme** deletes or decrements the first or last
  entry, and the next seek finds the new extreme. Equal values share an
  entry with a count, so deleting one of two equal minima leaves the other.

Cost: O(log n) per change and per extreme lookup. Storage is proportional
to the number of distinct (group, value) pairs. That is the price of exact
answers under deletes, and it lives on disk under the memory budget like
all other state.

**The order-preserving encoding** (`codec.encode_ordered`) makes byte order
equal value order:

- **Numbers.** An exact (sign, exponent, digits) encoding. Negatives are
  bit-inverted, with a terminator so that `−1.2 > −1.23`. int, float and
  Decimal share one order, as in Python and SQL, and `1.5`, `1.50` and
  `Decimal("1.5")` encode identically.
- **Timestamps** sort as UTC microseconds, whatever their zone. Dates,
  times and intervals have their own encodings.
- **Text** sorts by code point, i.e. UTF-8 byte order. That is Postgres's
  `"C"` collation, **not** the database's default (`en_US.utf8` here),
  which orders `"B" < "a"` differently. The differential test compares
  text min/max with `min(name COLLATE "C")`. Matching a locale collation
  would mean reimplementing ICU/glibc collation and is not attempted.

Tests (property-based):

- `test_ordered_encoding_preserves_order_and_equality` checks every type;
- `test_sorting_by_encoding_sorts_numbers` checks mixed int/float/Decimal;
- `test_aggregates_equal_recompute_after_every_step` makes random changes,
  with removals biased to the group's current min or max, grouped and
  global.

Mutations caught:

- keeping zero-count entries makes deleted extremes linger, and the unit
  property test fails;
- reading `Max` from the first entry fails it too;
- the differential test catches the zero-count mutation at transaction #1.

## 4. Distinct

`Distinct` is a count per distinct row: canonical row encoding mapped to a
count. It emits `+1` when a row's count goes from 0 to positive and `−1`
when it returns to 0; anything in between changes nothing visible. The
retraction emits the stored row, so it cancels exactly. NULLs compare
equal for DISTINCT, as in SQL, because the canonical encoding treats NULL
as a value (`test_distinct_equals_the_set_of_present_rows`).

## 5. Composition

Aggregates are operators like any other:

- the spec's example aggregates the output of a join, filtered;
- a filter or map after an aggregate works like HAVING;
- an aggregate of an aggregate is allowed (the `country_size_histogram`
  view).

A change to one customer's country moves all of that customer's orders,
and possibly the max, from one group to another in a single transaction
(`test_aggregate_after_a_join_follows_a_customer_moving_country`).

A join *after* an aggregation is rejected at registration. The operators
would compute it correctly, but naming the joined columns of an aggregate
output is not designed yet.

## What the M4 differential test shows

See [milestones/M4.md](milestones/M4.md). Eight views, one or more per
aggregate, are compared with SQL after every commit. The workload runs:

- steady, then drain, refill, drain, refill;
- each drain runs until the orders table is empty and customers are down
  to 10;
- during the drains, extra transactions delete the row holding a group's
  current minimum or maximum, both numeric (an order's amount) and text (a
  customer's name).

## Not done (M4)

- **Other aggregates:** `avg`, `count(distinct …)`, `string_agg`,
  `array_agg`, percentiles. They are rejected at registration.
  `avg` = `sum / count` can be derived with a map after the aggregate, but
  matching Postgres's numeric scale for `avg` is not attempted.
- **Locale collations for text min/max.** Code-point order only (`COLLATE "C"`).
- **Float sums.** Refused, by design.
- **Top-k** (listed in the spec's architecture under operators). It is not
  part of M4's acceptance and is not implemented.
