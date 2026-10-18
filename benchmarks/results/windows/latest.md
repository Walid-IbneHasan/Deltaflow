Measured 2026-10-18T20:02:36+00:00 on AMD Ryzen 5 5600X 6-Core Processor (12 logical CPUs, 15.9 GiB), Windows-11-10.0.26300-SP0, Python 3.12.14; PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu in Docker. Postgres-vs-host clock offset 1.2 ms.

| Capacity (backlog catch-up) | |
|---|---|
| row changes | 20814 in 5691 transactions |
| time | 22.27 s |
| **sustained changes/second** | **935** (255.6 transactions/s) |

| Load (share of capacity) | achieved changes/s | p50 | p90 | **p99 freshness** | max queued | peak RSS | checkpoints (mean, max) |
|---|---|---|---|---|---|---|---|
| 25% | 226 | ≤ 0.1 s | ≤ 0.2 s | **≤ 0.75 s** | 27 | 71.6 MiB | 7 (4.329 s, 27.038 s) |
| 50% | 449 | ≤ 30.0 s | ≤ 30.0 s | **≤ 30.0 s** | 32 | 77.7 MiB | 4 (7.094 s, 26.783 s) |
| 75% | 549 | ≤ 0.15 s | ≤ 0.3 s | **≤ 1.0 s** | 32 | 80.0 MiB | 7 (0.539 s, 0.872 s) |
