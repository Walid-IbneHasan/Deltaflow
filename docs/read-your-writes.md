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
