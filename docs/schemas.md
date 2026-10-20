# Real schemas: types, schema changes, partitions, large transactions

What Deltaflow does with the source tables a production database actually
has. The rule throughout: follow exactly what can be followed exactly, and
stop with a clear error where Postgres does not replicate enough to do so.
Nothing is approximated.

## Column types

| Type | Value in the engine | Notes |
|---|---|---|
| int2/4/8, numeric, float8, text, varchar, bool, date, timestamp[tz], uuid, bytea, interval, time | the Python value psycopg loads | as before |
| `json` | `Json`: its text, exactly as stored (whitespace, key order, duplicate keys) | Postgres has no equality for json; two `Json` are equal when their texts are |
| `jsonb` | `Jsonb`: its canonical text | equality is jsonb's: numbers by value, so `1.0` = `1`, as `GROUP BY` in Postgres |
| arrays (any element type, any number of dimensions) | `PgArray`: a tuple of elements, nested per dimension | an array whose lower bound is not 1 (`'[0:1]={1,2}'`) is **refused** when it arrives: its subscripts would differ from Postgres's |
| enums, domains, other types without a binary loader | the text Postgres prints | read as text by snapshots too, so snapshot and stream agree |

psycopg's defaults were not exact enough: jsonb numbers became floats
(`2.50` became `2.5`), duplicate json keys collapsed, array bounds were
dropped. They were also unhashable, so such a column used to stop the
pipeline. Snapshots now read json, jsonb, arrays and text-only types as
`::text`, through the stream's own decoder. A snapshot row and a streamed
row therefore hold identical values by construction. Sinks write `Json` and
`Jsonb` with their own types and arrays as arrays.

**Verified against Postgres** (`tests/integration/test_json_values.py`).
The test generates random JSON documents with duplicate keys, escapes,
surrogate pairs, every number form and random whitespace. It checks, on both
types: which texts Postgres accepts, jsonb's canonical text, and
`-> ->> #> #>>`. A 10,000-document soak found no mismatch. What it took to
agree:

- **jsonb's raw scalar.** jsonb stores a scalar at the top level as an
  array of one element, so `'"x"'::jsonb ->> 0` is `'x'`. `#>` paths do not
  step into it: `'"x"'::jsonb #>> '{0}'` is NULL.
- **json keeps lone surrogates.** json accepts `"\ud800"` on input. Its
  operators de-escape the whole document as they parse, so such a string,
  or `\u0000`, anywhere fails every operator on that document. jsonb
  refuses both on input.
- **Duplicate keys.** json `->` returns the last of them; jsonb keeps only
  the last.

### In SQL views

`-> ->> #> #>>` on json and jsonb, jsonb `?`, jsonb `=`/`<>`, casts between
text, json and jsonb, array subscripts `a[i]` and `a[i][j]`, `cardinality`,
`array_length`, and every comparison with `ANY (...)` and `ALL (...)`
(`tests/integration/test_sql_json_expressions.py`, against Postgres).
Ordering jsonb or arrays, `@>` and slices are refused at registration.

## Schema changes while views run

pgoutput announces a changed table with a new RELATION message. Each kind
of change is handled as follows:

| Change | What happens |
|---|---|
| `ADD COLUMN` | followed: rows keep the columns the views were planned with; the new column is left out (no view was planned with it) |
| `DROP COLUMN`, `RENAME COLUMN` of a column the table had | the runner stops: a view may read it. Re-register the views (`deltaflow reset`, then start) |
| `ALTER COLUMN ... TYPE`, including only its modifier (`numeric(12,2)` to `numeric(14,4)`) | the runner stops: the table was rewritten (every stored value changed), which Postgres does not replicate |

Column types and modifiers are recorded with the state checkpoint. A type
change made while the runner was down is therefore caught when it starts
again.

## Partitioned tables

A partitioned table can be a source. It is read through its root, and the
publication publishes via the root (`publish_via_partition_root = true`,
set by Deltaflow when it manages the publication, required otherwise).
Changes arrive under the root's name. A row moving between partitions
arrives as a DELETE and an INSERT. A partition itself is refused as a
source: its changes are published under the root's name.

Postgres does not replicate three things about partitions:

- **DETACH or DROP of a partition.** Its rows leave the table without a
  DELETE. Deltaflow checks the partitions at every checkpoint (and the
  checkpoint records them). When one has disappeared, the runner stops,
  and `serve()` starts it again, which bootstraps and reads the table as it
  now is. Exact, automatically, at the cost of a bootstrap. To avoid the
  bootstrap, `DELETE` the partition's rows before detaching it.
- **TRUNCATE of one partition** is not published at all. Deltaflow cannot
  see it, so the views keep those rows. Truncate the root, or `DELETE` from
  the partition.
- **ATTACH of a table that already has rows.** Those rows arrive without
  an INSERT. Attach empty tables (`CREATE TABLE ... PARTITION OF` is
  fine), or re-bootstrap after attaching a populated one.

## Large transactions

pgoutput without streaming sends a transaction only once it has committed.
That makes it sound to apply a large one before its end arrives:

- the stream hands it over in parts of `max_transaction_changes` changes
  (default 10,000) as they are decoded;
- the engine applies each part at the transaction's commit time. Nothing of
  the transaction is complete, so nothing is written, until its end;
- each Postgres sink keeps at most `sink_spill_after` deltas and keys in
  memory (default 100,000). Beyond that its buffers move into the state
  store, ordered by time and key, and are folded and written in chunks. All
  of it still commits with the watermark in one transaction;
- no checkpoint is taken inside a transaction. A crash or stop there
  replays the transaction whole from the last checkpoint.

So a ten-million-row `UPDATE`, or a view whose initial contents are a
hundred million rows, is never in memory at once
(`tests/integration/test_large_transactions.py`; the sink against the
in-memory sink, `tests/unit/sinks/test_sink_spill.py`).
