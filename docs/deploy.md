# Deploying Deltaflow

A worker is one process: `deltaflow run views.sql --slot <name>`. It needs
a connection to the source (replication and SQL), a writable state
directory that survives restarts, and, for its sinks, tables in the source
database (or another, `sink_dsn`). Its monitoring server answers on
`:9187`.

## As a package

```sh
pip install deltaflow           # or: uv tool install deltaflow
deltaflow --version
deltaflow run views.sql --slot shop --state-dir /var/lib/deltaflow/shop
```

The wheel is pure Python. Its three dependencies (psycopg, pglast,
rocksdict) ship binary wheels for Linux, macOS and Windows.

## As a container

```sh
docker build -t deltaflow .
docker run -d --name deltaflow \
  -e DELTAFLOW_DSN='host=db user=deltaflow dbname=shop sslmode=verify-full' \
  -v deltaflow-state:/var/lib/deltaflow \
  -v "$PWD/views.sql:/etc/deltaflow/views.sql:ro" \
  -p 9187:9187 \
  deltaflow run /etc/deltaflow/views.sql --slot shop \
    --state-dir /var/lib/deltaflow/shop --http-host 0.0.0.0
```

- The image installs dependencies exactly as `uv.lock` pins them, with
  hashes checked.
- It runs as uid/gid 10001, not root, and works with a read-only root
  filesystem (state goes to the volume, temporary files to `/tmp`).
- Its `HEALTHCHECK` asks `/healthz`.

## On Kubernetes (Helm)

```sh
kubectl create secret generic shop-db \
  --from-literal=dsn='host=db1,db2 user=deltaflow dbname=shop target_session_attrs=read-write'
helm install shop deploy/helm/deltaflow \
  --set database.existingSecret=shop-db \
  --set-file views.sql=views.sql \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.prometheusRule.enabled=true
```

What the chart creates:

- **A StatefulSet**, with the state on a PersistentVolumeClaim per pod
  (`persistence.size`, 10 Gi by default). Without persistent state, every
  restart bootstraps.
- **`mode: run`** (the default): one worker maintains every view.
- **`mode: cluster`**: `replicas` workers share the views' shards by
  leases. Set `cluster.shards` and `cluster.shardBy`, and see
  [scale-out.md](scale-out.md).
- **Probes.** Liveness asks `/healthz` (the process answers). Readiness
  asks `/readyz`: 200 once the views are live, 503 while starting, loading
  or restarting.
- **The connection string** only ever comes from a Secret.
- **A ServiceMonitor and a PrometheusRule**, each optional.

The views come from `views.sql`, or from `views.existingConfigMap`. The
pod template carries a checksum of them, so changing the views rolls the
pods. A changed view is rebuilt alone from the checkpoint's source rows,
without a bootstrap ([exactly-once.md](exactly-once.md)).

## Alerts

[`deploy/helm/deltaflow/files/deltaflow-alerts.yml`](../deploy/helm/deltaflow/files/deltaflow-alerts.yml)
is a Prometheus rule file. The chart turns it into a PrometheusRule, or
you can load it as is. `make alerts` checks it with promtool and runs its
unit tests.

| Alert | Fires when | Why it matters |
|---|---|---|
| DeltaflowDown | the worker is not scraped for 2 min | views stop moving; the slot holds WAL |
| DeltaflowViewsStale | p99 freshness above 5 s for 10 min | readers see old results |
| DeltaflowSlotRetainsWal (warning, critical) | the slot holds more than 1 GiB / 10 GiB | the source's disk fills |
| DeltaflowApplyBacklog | more than 256 MiB received but not applied | the engine is slower than the writes |
| DeltaflowNotCheckpointing | no checkpoint in 30 min | WAL accumulates; restarts replay more |
| DeltaflowRestartLoop | more than 3 restarts in 15 min | a recurring cause |
| DeltaflowBootstrapped | a bootstrap happened (info) | why is in the log |

## Sizing

- **Memory.** `--memory-budget` (MiB) bounds the state store's cache and
  memtables. The process needs that plus about 150 MiB. A large
  transaction is applied in parts, and sinks spill to the state store, so
  neither grows memory (docs/schemas.md).
- **Disk.** The state holds every source row the views read (the
  arrangements) plus each view's operator state, and two checkpoints of it
  (hard links, so not twice the space).
- **The source.** One replication slot per worker (per shard in a
  cluster), plus one more while loading.

## Privileges

The role Deltaflow connects as needs:

- `REPLICATION`;
- `SELECT` on the source tables;
- ownership of the publication, or the right to create one;
- `CREATE` on the sink schemas, if it creates the sink tables;
- `CREATE` on the database, for the `_deltaflow` schema (watermarks, the
  cluster's leases, `wait_for`).

Readers of the views need only `SELECT` on the sink tables. `wait_for` is
`SECURITY DEFINER` and executable by everyone.

## Upgrading Deltaflow

A new version resumes the checkpoints of the previous one. The checkpoint
metadata is versioned, and fields added since (column types, partitions,
the server's identity) are optional when read. If a change of the state's
format ever needs one, the version says so and the runner bootstraps,
saying why.
