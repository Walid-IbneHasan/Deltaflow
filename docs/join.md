# The incremental join, its state, and the memory budget

This is written for a reviewer who will push back. Every claim names the test
that checks it. The delta model and frontiers are in
[delta-algebra.md](delta-algebra.md); the choice of RocksDB is
[decision 0004](decisions/0004-state-store-rocksdb.md).

## 1. The update rule

`orders ⋈ customers` is bilinear in its inputs: it distributes over + on
each side separately. So when one transaction changes both sides,

    (L + ΔL) ⋈ (R + ΔR) − L ⋈ R  =  ΔL ⋈ R  +  (L + ΔL) ⋈ ΔR

Each side keeps an index of everything it has received so far, keyed by
join key. The engine runs a step's operators in topological order and a
join's inputs in port order, so within one transaction:

1. the left batch ΔL probes the right index, which still holds R, and emits
   ΔL ⋈ R; then ΔL is folded into the left index;
2. the right batch ΔR probes the left index, which now holds L + ΔL, and
   emits (L + ΔL) ⋈ ΔR; then ΔR is folded in.

That is exactly the right-hand side above. The ΔL ⋈ ΔR term is counted
once, in step 2. Folding before probing in step 1 would count it twice;
probing a stale index in step 2 would drop it.

An output delta's diff is `delta.diff × match multiplicity`. Both sides are
multisets, because a map before the join can make equal rows. Its time is
the delta's time.

Tests:

- `test_incremental_join_equals_join_of_accumulated_inputs`
  (property-based). Random insertions and retractions hit both sides in the
  same step, with NULL keys and duplicates. After every step the
  accumulated output equals a naive nested-loop join of the accumulated
  inputs.
- `test_join_view_matches_the_model_after_every_transaction` checks the
  same through the whole engine. Arrangements, filters on both sides, the
  join key itself changing, a customer deleted with orders still pointing
  at it, and unchanged TOAST in the right-hand rows.
- **Mutations:** ignoring the match multiplicity, or never folding the right
  side, make both fail.

## 1b. The left outer join (added after M7)

`.join(table, on=..., how="left")` is SQL's LEFT JOIN: the inner join, plus
every left row whose key matches no right row, with NULL right columns
(`pad`). A right join is the same with the tables swapped. Full outer
joins are not supported.

For one key k with left rows L_k and c_k right rows, the padded part of
the result is pad(L_k) when c_k = 0, and nothing otherwise. Within a step
the left batch runs first, then the right batch, as for the inner join:

1. **Left batch.** c_k has not changed yet, so the padded part changes by
   pad(ΔL_k) if c_k = 0. The probe of the right index that the inner part
   does anyway says whether c_k = 0: its entries never have multiplicity
   0. A left row with a NULL key matches nothing, so it is always padded.
2. **Right batch.** It moves c_k to c_k'. If c_k = 0 < c_k', the key
   gained a match: pad(L'_k) is retracted. If c_k > 0 = c_k', it lost its
   last match: pad(L'_k) is added. L'_k is the left index, which already
   includes ΔL; the inner part probes it anyway.

Summed with the inner part, that is the change of the left join:
pad(L'_k)·[c_k' = 0] − pad(L_k)·[c_k = 0]. c_k is stored per key
(`join:<view>.<step>:right_count`), so a right batch learns it without
scanning the right side; a step that changes one order of a customer with
thousands does not read them all.

**Padding needs the right table's columns.** A NULL-padded row has every
right column, as in SQL, so a map after the join can read
`r["customers.name"]` and get None. The columns come from the table's
schema. The snapshot loader installs tables one by one, in source order,
so for `orders ⟕ customers` the orders arrive while the customers index is
still empty: every order is padded, and loading customers retracts the
padding. `load_snapshot` therefore declares every table's schema before
loading any rows (`Engine.declare`). A resumed engine has them from its
checkpoint. If they were still missing, the join would raise rather than
make a row with columns missing
(`test_a_left_join_refuses_to_guess_the_right_columns`).

**Sharding.** A co-partitioned left join is exact shard by shard: a key's
left rows and all of its right rows live on one shard, so that shard knows
c_k. The partials are disjoint and merge by union.

**Tests.**

- `test_incremental_left_join_equals_left_join_of_accumulated_inputs`
  (property-based, as the inner join's test): random batches on both
  sides, NULL keys, duplicates. After every step the accumulated output
  equals a naive LEFT JOIN of the accumulated inputs. Two mutations make
  it fail:
  - not re-padding when a count crosses zero;
  - not padding NULL keys.
- The differential test compares two left-join views with Postgres's own
  LEFT JOIN after every one of about 8,000 commits:
  - `customers ⟕ orders`: padding comes and goes as a customer's first
    order arrives and its last one is deleted or moved;
  - `orders ⟕ customers`: padded during the bootstrap, as above.
- The sharded property test (`test_merged_shards_equal_the_unsharded_engine`)
  covers both directions, with customers deleted while their orders stay,
  and orders moving between shards.

**Not counted by `Count()`.** `Count()` is SQL's `count(*)`: it counts a
padded row too. SQL's `count(column)`, which skips NULLs, has no
counterpart yet.

## 2. Why the indices may forget time

The indices store accumulated multiplicities, with no timestamps. That is
sound only if every arriving delta is at or after every time already folded
in. Otherwise a probe would pair a delta with state from its own future.
Two facts make it hold:

- the engine applies transactions in commit-LSN order, one per step
  ([delta-algebra.md](delta-algebra.md) §3);
- the snapshot (time 0) is installed before any transaction.

`Join.process` checks it anyway: a delta older than the latest folded-in
time raises `FrontierViolationError` (`test_time_must_not_go_backwards`).

*What would change with independent inputs.* If the two sides came from
different replication streams (M7 sharding), one side could run ahead.
Then the join must hold back deltas whose time is beyond the other input's
frontier, and fold them in only as the meet of the input frontiers passes.
That is the general differential-dataflow rule. M3 does not need it: both
sides ride one stream and advance together, which the check enforces. The
join's output frontier is already the meet of its inputs' frontiers
(`Operator.frontier`), so sinks never publish past the lagging side.

## 3. Join-key equality

A probe finds matches by the *bytes* of the join key, so the key encoding
must make bytes equal exactly when values are equal (`state/codec.py`):

| Pair | Equal in SQL / Python | Naive encodings that differ |
|---|---|---|
| `Decimal("1.0")`, `Decimal("1.00")`, `1`, `1.0` | yes | `str`, `repr`, pickle |
| the same instant in UTC and in `+02:00` | yes | `isoformat`, pickle |
| `1` and `"1"` | no | — |

Numbers of any type are encoded as (sign, significant digits, exponent),
computed exactly, without `Decimal.normalize()`, whose 28-digit context
rounds. Aware datetimes are encoded as microseconds since the epoch in UTC.
NaN, which is not equal to itself, is refused.

Tests (property-based, `tests/unit/state/test_codec.py`):

- `(enc(a) == enc(b)) == (a == b)` over mixed types;
- the same instant in any zone;
- trailing zeros beyond 28 digits;
- prefix-freeness.

Changing the aware-datetime encoding to `isoformat` makes the zone property
fail.

NULL keys follow SQL: `NULL = NULL` is not true, so a row with a NULL in any
key column joins nothing. It is not even indexed (`test_null_keys_never_match`).

## 4. State layout

One RocksDB instance per engine. Keyspaces are length-prefixed names, so a
scan of one can never run into another.

| Keyspace | Key | Value |
|---|---|---|
| `arr:<table>` | canonical primary key | the row (pickle) |
| `join:<view>.<step>:left` / `:right` | canonical join key ‖ canonical row | signed 8-byte multiplicity ‖ the row |

A probe is a prefix scan over the canonical join key. Tuple encodings are
self-delimiting, so a key's encoding is never a prefix of a different key's,
and the scan returns exactly that key's rows.

**Cost.** The join's right index stores whole source rows, so a joined table
is stored twice (arrangement plus index). Pushing a projection below the
join, so the index keeps only the columns the view uses, is not done.

## 5. The memory budget

`StateStore(memory_budget=B)` creates one LRU cache of B bytes, used for:

- data blocks read back from SST files;
- index and filter blocks (`cache_index_and_filter_blocks`), not held
  per open file;
- memtables, charged to the same cache through a WriteBufferManager limited
  to B/4. When the charge reaches the limit, memtables flush to disk.

So RocksDB's resident memory is governed by B. State beyond B lives in SST
files and is read back on demand.

The budget does **not** cover the following, and nothing pretends it does:

| Outside the budget | Bounded by |
|---|---|
| the interpreter, psycopg, imports | measured as the baseline before the engine exists |
| one snapshot chunk in flight | `snapshot_chunk_rows` (5,000) / `snapshot_chunk_bytes` (2 MiB); the snapshot cursor fetches 200 rows at a time |
| one transaction's deltas and probe results | the transaction (M1 limitation: a huge transaction is held whole) |
| the in-memory sink's result | the view's size (M5 replaces it with the Postgres sink) |
| RocksDB iterator, compaction and allocator overhead | RocksDB, not configurable from rocksdict |

Before M3 the snapshot loader read a whole table into a Python list before
installing it. That would have made the budget meaningless; it now streams
through a server-side cursor in byte-bounded chunks.

## 6. Measured, not assumed

All numbers come from this machine (Windows 11, Docker Postgres 16, Python
3.12). The dataset has 10,000 customers, 60% of them with an incompressible
8–24 KB bio, and 100,000 orders. The view is
`orders(amount > 4500) ⋈ customers → (orders.id, country)`. Each run loads
the snapshot, applies 500 load transactions and compares the view with SQL.
Every run matched.

**First attempt: RocksDB's default background settings.** Resident growth
was 78–92 MiB whatever the budget (16 to 128 MiB). It did not scale with
state either: doubling the state added about 10 MiB. But a fixed ~65 MiB sat
outside the budget.

Two measurements located it:

- tracemalloc put the Python heap at only ~10 MiB above the baseline;
- a pure-RocksDB script writing the same volume grew to **3.3× a 16 MiB
  budget**. Flush and compaction buffers bypass the block cache, and
  compaction rewrote every 16 KB value repeatedly.

| Pure RocksDB, 16 MiB budget | Peak growth |
|---|---|
| defaults | 53.4 MiB |
| 1 background job, small readahead/file buffers | 32.5 MiB |
| values ≥ 4 KiB in blob files | 28.2 MiB |
| both (adopted) | **21.5 MiB** |

**With the adopted settings, the whole engine:**

| Budget | State on disk | Resident growth | Beyond the budget |
|---|---|---|---|
| 16 MiB | 398 MiB | 43.7 MiB | 27.7 MiB |
| 32 MiB | 400 MiB | 51.6 MiB | 19.6 MiB |
| 64 MiB | 408 MiB | 61.9 MiB | −2.1 MiB |
| 128 MiB | 394 MiB | 72.8 MiB | −55.2 MiB |
| 32 MiB | **787 MiB** (2× state) | 59.2 MiB | 27.2 MiB |

How to read this:

- **Memory does not follow state.** Doubling the state, from 400 to
  787 MiB, adds about 8 MiB, and that much is the in-memory sink holding a
  view result twice as large.
- **There is a fixed part outside the budget**, about 20–28 MiB. At a 32
  MiB budget it splits as follows:
  - the Python heap peaks 9.7 MiB above the baseline (tracemalloc);
  - the rest is RocksDB memory outside its cache (thread stacks, per-file
    and blob-file metadata, memtable arena slack, flush buffers) plus
    allocator slack.
- **Large budgets are not filled pointlessly.** The block cache only holds
  what was read.

So the accurate statement is: **resident memory ≈ budget + a fixed overhead
of a few tens of MiB, independent of state size.** `test_memory_budget.py`
asserts growth ≤ budget + 32 MiB with state at 13× a 32 MiB budget: 1.5×
the largest excess measured at that budget.

**It detects the bug it is meant to.** Restoring M2's snapshot loader,
which reads a whole table into a list, makes the test fail with 161 MiB of
growth.

The test measures the engine in a child process, so the OS peak-RSS counter
covers exactly the engine's lifetime. The test runner's own memory (seeding
the dataset, Hypothesis) is not counted.

## 7. Correctness with state on disk

The 100k-operation differential (`test_join_views.py`) runs with the
minimum budget, 1 MiB. State ends at about 14 MB (SST plus blob files), so it is flushed, compacted,
evicted and read back from disk for the whole run. That covers the store's
read paths (point gets, prefix scans across SST, blob and memtable data),
not only its in-memory fast path.

## Not done (M3)

- **Full outer joins, self-joins, joins of same-named tables.** (Left joins
  were added after M7, §1b.) Rejected at
  registration.
- **Projection pushdown.** Joined source rows are stored whole in the
  index.
- **Holding back deltas for independently advancing inputs.** Not needed
  while all sources share one stream, and enforced by a check (§2).
- **Durability.** The store is wiped at every start and the WAL is off;
  M5 makes state survive restarts atomically with the sink.
- **Spilling of a single huge transaction.** M1 limitation, unchanged.
