#!/bin/bash
# Runs once, when the primary's data directory is created.
set -euo pipefail
# The standby streams WAL from here with the cluster's own user.
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT pg_create_physical_replication_slot('standby_slot')"
