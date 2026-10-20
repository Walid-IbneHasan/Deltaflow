#!/bin/bash
# A streaming standby of `primary` that keeps copies of its failover slots
# (sync_replication_slots, Postgres 17), then runs as Postgres does.
set -euo pipefail
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h primary -U deltaflow -d deltaflow_test; do sleep 1; done
  PGPASSWORD=deltaflow pg_basebackup -h primary -U deltaflow -D "$PGDATA" -X stream -R \
    -S standby_slot
  cat >> "$PGDATA/postgresql.auto.conf" <<'CONF'
primary_conninfo = 'host=primary user=deltaflow password=deltaflow dbname=deltaflow_test'
primary_slot_name = 'standby_slot'
hot_standby_feedback = on
sync_replication_slots = on
max_wal_senders = 16
max_replication_slots = 16
wal_level = logical
CONF
  chmod 700 "$PGDATA"
fi
exec postgres
