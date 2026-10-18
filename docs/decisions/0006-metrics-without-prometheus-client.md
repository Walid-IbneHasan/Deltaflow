# 0006 — Prometheus metrics without `prometheus_client` (M6)

Status: accepted, 2026-10-19

## Context

M6 needs Prometheus metrics (end-to-end lag, per-operator throughput, state
size, slot lag) and a JSON API for the console, both served by the running
worker.

## Options

**(a) `prometheus_client`.** It is the standard library for this. But it
brings a process-global registry (one runner per registry is what tests and
M7's multi-shard workers need) and a thread-based HTTP server, next to an
asyncio process that would need its own server for the console anyway.
Most metrics here are *pulled* from state the engine already keeps, so the
library's push-style instrumentation would mostly sit idle.

**(b) Write the exposition format.** Text format 0.0.4 is `# HELP`,
`# TYPE` and one line per sample. Counters, gauges and fixed-bucket
histograms cover everything needed. About 200 lines, with tests of the
exact output.

## Decision

(b), served by `observability/http.py`: one asyncio server for `/metrics`,
`/api/stats` and `/healthz`, with one registry per runner.

## Consequences

- **No summaries, no exemplars, no OpenMetrics.** Quantiles come from
  histograms: Prometheus's `histogram_quantile` on the server side, or a
  coarse upper-bound estimate in `/api/stats`.
- **No authentication.** The server binds to 127.0.0.1 by default; anything
  else must sit behind something that authenticates.
