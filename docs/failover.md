# Failover: when the source's primary is replaced

Most production Postgres has a standby that is promoted when the primary
fails (Patroni, a cloud provider, an operator). A logical replication slot
lives on one server, so a runner has to find its slot again on the new
primary and know whether its own state is still valid there.

## Set up (Postgres 17 and later)

**Primary**
- `wal_level = logical`, as always.
- `synchronized_standby_slots = '<the standby's physical slot>'`. Logical
  walsenders then send nothing the standby has not confirmed. Without it, a
  runner can apply WAL that a failover then loses, and has to bootstrap
  afterwards (see "Divergence" below). The runner warns when it is unset.

**Standby**
- `sync_replication_slots = on`, `hot_standby_feedback = on`,
  `primary_slot_name` set, and `dbname` in `primary_conninfo`: it then keeps
  copies of the primary's failover slots;
- `wal_level = logical` and `max_wal_senders` / `max_replication_slots` at
  least the primary's, for it to serve logical replication once promoted.

**The runner** creates its slot as a failover slot on Postgres 17+ (or
makes an existing one one): `DurableRunner(failover=None)` is "where
supported". Give it every host, wanting the primary:

```sh
deltaflow run views.sql --slot shop \
  --dsn "host=db1,db2 port=5432 user=deltaflow dbname=shop target_session_attrs=read-write"
```

The replication connection tries the hosts in order, as libpq does, and
skips standbys (`in_hot_standby`). Every connection gets
`connect_timeout=10` unless the DSN or `PGCONNECT_TIMEOUT` sets one: libpq
otherwise waits for ever on a host that accepts TCP and then says nothing,
and would never reach the next one.

The server that answers moves to the front of the runner's host list.
Without that, every connection after a failover, each restart included,
would first wait `connect_timeout` on the old primary: a start opens
three. In the failover test the runner resumed 45 s after the primary
died without this, and 21 s with it. Docker Desktop holds a dead
container's port open, so each attempt there waits the full timeout.
Your application's own connections need a `connect_timeout` too, for the
same reason.

## What happens at a failover

The runner's connections break and `serve()` restarts it, retrying with
backoff until a host answers as primary. It then finds its slot there (the
standby's copy, now an ordinary slot) and **resumes from its checkpoint**.
The sinks drop what they already hold, as after any restart. No bootstrap.

Measured in `tests/failover/test_failover.py` (`make failover`: a Postgres
17 primary and standby in docker). The test kills the primary under load
and promotes the standby. The runner reconnects to the standby, resumes
(one restart, one resume, no bootstrap), and every view then equals its SQL
on the new primary.

## Divergence: when the state is no longer valid

Every checkpoint records the server's **system identifier** and
**timeline** (IDENTIFY_SYSTEM). On start:

- **Another cluster** (a different system identifier: a restore, or the DSN
  pointing elsewhere): the checkpoint's positions mean nothing there, so
  the runner bootstraps.
- **A new timeline.** The runner reads the new timeline's history
  (TIMELINE_HISTORY), which records where its own timeline ended. If the
  checkpoint is past that point, the runner applied WAL the new primary
  never had: views would keep rows that no longer exist. It bootstraps,
  saying why. `synchronized_standby_slots` prevents this case.
- **Sinks behind the checkpoint.** With the sinks in the source database,
  an asynchronous standby may have lost the last flushes. The usual check
  ("watermark behind the checkpoint") catches this, and the runner
  bootstraps.

Each check errs towards a bootstrap. A bootstrap is slow but always
correct, and in chunks it does not hold back VACUUM (decision 0009).

## Before Postgres 17

There are no failover slots: the new primary has no slot for the runner,
which then bootstraps by itself, as it would for any missing slot. Use
`--snapshot chunked` (the default) so the bootstrap does not hold back
VACUUM on a primary that has just taken over.
