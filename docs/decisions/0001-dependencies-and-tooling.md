# 0001 — Dependencies and tooling (M0)

Status: accepted, 2026-10-18

Every runtime and development dependency, and why it beats the standard
library or doing without.

## Runtime

| Dependency | Why |
|---|---|
| `rocksdict` (M3) | Operator state store with a memory budget. See [0004](0004-state-store-rocksdb.md). |
| `psycopg[binary]` 3 | Spec requirement. The stdlib has no PostgreSQL client. The `binary` extra ships the C implementation (`psycopg.pq.__impl__ == "binary"`) so no local libpq is needed on Windows or CI. It is used for ordinary SQL connections (seeding, load, and later the sink). The **replication connection does not go through psycopg's decoding**: from M1 the `pgoutput` stream is parsed by hand. |

## Development only

| Dependency | Why |
|---|---|
| `pytest` | Fixtures and parametrisation; `unittest` would need far more boilerplate for the differential suites. |
| `ruff` | Lint + format in one fast tool, replacing flake8/isort/black. |
| `mypy` | `--strict` is a project constraint. |
| `hypothesis` (M2) | Property tests for the delta algebra (group laws, consolidation, linearity of operators) and for the source arrangement against a model. Hand-rolled random testing would have to reinvent shrinking, which is what turns a failing 30-delta batch into the 3-delta counterexample worth reading. Brings `sortedcontainers`, pure Python. |
| `uv` (tool, not a package dependency) | Pins the interpreter (`.python-version` = 3.12, the supported floor) and the lockfile (`uv.lock`), so CI and local runs resolve identically. |
| GNU make | `make up/seed/load/check` is the spec'd interface. On Windows it was installed with `winget install ezwinports.make`; recipes run in Git Bash's `sh`. |

## Considered and rejected

- **`pytest-asyncio`.** psycopg's async mode cannot use Windows' default
  Proactor event loop, and pytest-asyncio's loop-policy hooks are being
  deprecated along with asyncio policies in 3.14. A three-line
  `deltaflow.aio.run()` that picks `SelectorEventLoop` on Windows serves both
  CLIs and tests with no extra dependency.

## Postgres version

The harness runs `postgres:16`, the oldest supported version. Developing
against the floor stops us from accidentally relying on newer `pgoutput`
protocol versions. A CI job against the newest major version is worth adding
once the decoder exists (M1).
