# 0002 — Replication connection speaks the wire protocol directly (M1)

Status: accepted, 2026-10-18; amended 2026-10-19 (TLS, below)

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

- ~~**No TLS.**~~ Superseded 2026-10-19, see "Amendment: TLS" below. The
  original estimate ("about 20 lines") was wrong: the hard part was not
  the handshake but resolving settings exactly as libpq does.
- **No SCRAM channel binding** (`-PLUS`). Its tls-server-end-point type
  hashes the server certificate with the hash of the certificate's own
  signature algorithm, which Python's ssl module does not expose.
  `channel_binding=require` is refused rather than pretended.
- **No SASLprep.** Non-ASCII passwords are refused with a clear error rather
  than mis-hashed.
- Output-format GUCs (`DateStyle=ISO, YMD`, `TimeZone=UTC`,
  `IntervalStyle=postgres`, `extra_float_digits=3`) are pinned in the startup
  packet. The text pgoutput sends is therefore independent of server defaults.

## Amendment: TLS (2026-10-19)

**The problem.** The replication connection ignored `sslmode`. A DSN with
`sslmode=require` connected in plaintext, and sent the password in the
clear if the server asked for one. Every psycopg connection in the same
process *was* encrypted, because libpq honoured the same DSN. Managed
Postgres (RDS, Cloud SQL, Azure, Supabase, Neon) requires TLS, so Deltaflow
could not stream from any of them. This broke the project's own rule
against silent approximation.

**Two options.**

(a) **Implement TLS and libpq's settings resolution in the replication
connection.** Settings come from libpq's own parser and defaults
(`PQconninfoParse`, `PQconndefaults` via `psycopg.pq`), so environment
variables apply to both kinds of connection.

(b) **Implement TLS for `sslmode` and `sslrootcert` only, and keep
`conninfo_to_dict`.** Less code. But environment variables (PGSSLMODE,
PGPASSWORD) and the password file would still apply to psycopg
connections and not to this one: the same split-brain in another form.

**Chosen: (a).** `replication/conninfo.py` resolves settings as libpq does
and *refuses* anything libpq would act on that this connection cannot
honour: several hosts, `hostaddr`, service files, CRLs,
`channel_binding=require`, GSS encryption, `target_session_attrs`, direct
TLS negotiation, and `options` that touch the pinned output formats.
Implemented:

- every `sslmode`, with libpq's order of attempts:
  - `prefer` continues in plaintext on the same connection after the
    server's "N", and retries in plaintext after a failed handshake;
  - `allow` tries plaintext first;
- `sslrootcert`, including `system`, which implies verify-full as in
  libpq 16;
- client certificates, with libpq's key permission check;
- `sslsni`, TLS version bounds, `sslkeylogfile`;
- the password file;
- `connect_timeout`, TCP keepalives (on by default, as in libpq);
- `require_auth`.

As in libpq's code (not only its docs), an existing root certificate file
is used to verify the server in every mode but `disable`.

**Two security properties, each with a test.**

- **Injected bytes are rejected (CVE-2021-23222).** TLS is negotiated on
  the raw socket, and exactly one byte of the answer to SSLRequest is
  read. Bytes a man in the middle appends after "S" therefore fail the
  handshake; they are never read as the server's messages.
- **The server must prove it knows the password.** A server that starts
  SCRAM and then sends AuthenticationOk without its final message is
  refused. Before this change it was accepted. That would have let an
  impostor pass as the server, and the gap existed with or without TLS.

**The harness now runs TLS.** The compose server uses the certificate its
image ships for "localhost", so the whole test suite streams over
encrypted connections (default `sslmode=prefer`).
`tests/integration/test_tls.py` checks:

- verify-full end to end;
- a name mismatch is refused;
- verify-ca accepts the chain only;
- the system roots refuse the self-signed certificate;
- psycopg and the replication connection agree.

Benchmark numbers recorded before this date were measured in plaintext.

**What TLS costs.**

- **Latency:** none measurable. Commit-to-receive on the replication
  stream was 1.2 ms median both with and without TLS.
- **Memory:** a one-off 6 to 10 MiB at the first connection of each kind,
  measured. That is two OpenSSL instances: libpq's, for psycopg, and
  Python's, for this connection. Later connections add nothing. The memory
  acceptance test (M3) failed once over its allowance until its baseline
  was taken after one throwaway connection of each kind: the budget
  bounds what grows with state, and library initialisation does not.
