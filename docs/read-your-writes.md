# Read-your-writes

An application writes to its tables, then reads a view maintained from
them. The view lags the write by the pipeline's latency (milliseconds to a
second), so the read may miss the write. `_deltaflow.wait_for` closes that
gap: it returns once the views reflect everything committed up to a WAL
position.

```sql
-- after COMMIT, in any session of the source database:
SELECT _deltaflow.wait_for(pg_current_wal_lsn(), ARRAY['order_totals'], '5 seconds');
-- true: analytics.order_totals now reflects every committed write up to then
```

```python
from deltaflow.client import wait_for

conn.execute("INSERT INTO orders ...")
conn.commit()
if wait_for(conn, views=["order_totals"], timeout=5):  # default target: now
    rows = conn.execute("SELECT ... FROM analytics.order_totals").fetchall()
```

`deltaflow.client.await_for` is the same for an async connection. Any
language can call the SQL function.

## What it guarantees

`wait_for(target, views, timeout)`:

- **true** once the watermark of every listed view (all views when `views`
  is NULL) is at or past `target`. A view in a cluster has a watermark per
  shard; the least of them counts. A watermark W means the view's table
  holds exactly the result of every transaction committed before W, and
  commits together with the rows it covers (docs/exactly-once.md). So once
  `pg_current_wal_lsn()` taken after a commit is reached, that commit is in
  the view;
- **false** after `timeout`, for example when no runner is running;
- **an error** (`undefined_object`) for a view with no watermark at all: a
  misspelt name would otherwise wait for its whole timeout.

## Why it is prompt

A watermark moves when the runner flushes. While the views' tables are
busy, that is every `--flush-interval`. While they are idle, the WAL
position still moves (other tables, the database's own activity), and the
runner used to move its watermark there only at its next checkpoint, every
`--checkpoint-interval` (10 s by default). That is too slow to wait for.

So `wait_for` first emits a non-transactional logical message
(`pg_logical_emit_message`). The runner receives it in WAL order. Every
transaction that committed before it has already been delivered, which
makes the message a point of progress. The runner flushes anything
pending, moves its watermark to the message's position, and the waiter sees
it. The test measures a median under a second with a 30-second checkpoint
interval (`tests/integration/test_read_your_writes.py`).

**The message must be flushed.** A walsender sends only WAL that is on
disk, and a non-transactional message is not flushed when it is written.
At first `wait_for` did not flush it. It went out with the next commit,
usually the runner's own flush of the waiter's write. When that flush
committed just before the message, and nothing else committed after it,
the message waited for PostgreSQL's background writer. Every 15 s at
most, that writer logs the running transactions and asks for a flush. The
WAL writer flushes only completed pages, so nothing sooner moved the
message. `wait_for` then ran out its 10 s timeout, once in CI and 3 times
in 200 runs of the test on PostgreSQL 18.

Now it is flushed:

- **PostgreSQL 17 and later:** `pg_logical_emit_message(..., flush =>
  true)`. With nothing else committing, the wait took 8–12 ms, against
  4 s before. The test's median fell from 57 ms to 6 ms.
- **PostgreSQL 15 and 16,** which have no such option: a second message
  (prefix `deltaflow.flush`, ignored by runners) fills the rest of the WAL
  page, and the WAL writer flushes the completed page on its next round.
  That is `wal_writer_delay` (200 ms) after the call, or up to 5 s when
  the WAL writer has gone to sleep on an idle server. Measured: 0.2 s.
  The filler is at most one WAL page (8 KiB) per call.

`tests/integration/test_read_your_writes.py` waits with nothing else
committing, on every supported version.

## Notes

- **Isolation.** Call it in READ COMMITTED (the default): it polls the
  watermark table, and a REPEATABLE READ or SERIALIZABLE transaction would
  keep seeing the watermark as it was when the transaction began.
- **Privileges.** The function is `SECURITY DEFINER`, owned by the role the
  runner connects as, and executable by everyone: a reader needs no grant
  on `_deltaflow.watermarks`. Emitting the wake message needs what
  `pg_logical_emit_message` needs, which Deltaflow's own role has.
- **Cost.** A waiting session holds a connection and reads one small table
  every 5 ms until it returns.
