# 0002 — Replication connection speaks the wire protocol directly (M1)

Status: accepted, 2026-10-18

## Context

The replication connection must run `START_REPLICATION` and then stay in
CopyBoth mode, reading `XLogData` and keepalives while sending standby status
updates. psycopg 3 has no API for CopyBoth or replication connections. psycopg2
had one, but it is a C extension wrapping libpq, and the spec forbids hiding
the replication parser behind a library.

## Decision

`deltaflow/replication/wire.py` implements the parts of protocol v3 that a
logical replication client needs, using only the stdlib (`asyncio` streams,
`struct`, `hashlib`, `hmac`):

- startup with `replication=database`
- authentication: trust, cleartext, MD5, SCRAM-SHA-256
- simple query, used for `CREATE_REPLICATION_SLOT`, `DROP_REPLICATION_SLOT`
  and `IDENTIFY_SYSTEM`
- CopyBoth

Ordinary SQL (publications, catalog queries, seeding) still uses psycopg.

psycopg is reused in exactly one place on the replication path: its type
*loaders* turn a column's text form into a Python value, chosen by the column's
type OID (e.g. `"12.30"` → `Decimal("12.30")`). Rewriting date/numeric parsing
would add risk without showing anything about replication. The pgoutput
message decoding itself is entirely ours.

## Consequences and limits

- **No TLS.** Fine for localhost and a trusted network. Adding it means
  `SSLRequest` plus `loop.start_tls`, about 20 lines; it is left out until
  needed.
- **No SCRAM channel binding** (`-PLUS`), which follows from having no TLS.
- **No SASLprep.** Non-ASCII passwords are refused with a clear error rather
  than mis-hashed.
- Output-format GUCs (`DateStyle=ISO, YMD`, `TimeZone=UTC`,
  `IntervalStyle=postgres`, `extra_float_digits=3`) are pinned in the startup
  packet. The text pgoutput sends is therefore independent of server defaults.
