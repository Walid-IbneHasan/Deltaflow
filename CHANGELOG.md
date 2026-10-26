# Changelog

## 0.1.0 — 2026-10-26

The first release. Deltaflow keeps views up to date from a PostgreSQL
replication slot: the work for each change is proportional to the change,
not to the tables, and the results are ordinary Postgres tables.

- **Views in SQL or Python.** `CREATE VIEW` statements, checked by
  Postgres's own parser, with filters, inner and left joins, `GROUP BY`
  with count, sum, min, max, avg and count distinct, and `DISTINCT`.
  Expressions return exactly what Postgres returns; what cannot be
  maintained is refused with the reason ([docs/sql.md](docs/sql.md)).
- **Exactly once.** Rows and their watermark commit in one transaction,
  and state is checkpointed to RocksDB, so a `kill -9` at any point loses
  and repeats nothing ([docs/exactly-once.md](docs/exactly-once.md)).
- **Real schemas.** json, jsonb, arrays, partitioned tables, large
  transactions and `ADD COLUMN` ([docs/schemas.md](docs/schemas.md)).
- **Loading and changes.** Initial loads run in chunks and resume after a
  stop; a changed view is rebuilt alone.
- **Failover.** PostgreSQL 17 failover slots, several hosts and timeline
  checks ([docs/failover.md](docs/failover.md)).
- **Scale-out.** Sharded views on several workers, with leases in
  Postgres and epoch fencing ([docs/scale-out.md](docs/scale-out.md)).
- **Read-your-writes.** `SELECT _deltaflow.wait_for(pg_current_wal_lsn())`
  ([docs/read-your-writes.md](docs/read-your-writes.md)).
- **Operations.** Prometheus metrics, JSON logs, `/healthz` and
  `/readyz`, alerting rules, a container image and a Helm chart
  ([docs/deploy.md](docs/deploy.md)).

Supports PostgreSQL 15 to 18 and Python 3.12 to 3.14. What is not done
yet is listed in
[docs/milestones/production.md](docs/milestones/production.md#not-done).
