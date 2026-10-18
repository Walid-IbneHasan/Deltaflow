Measured 2026-10-18T19:58:44+00:00 on AMD Ryzen 5 5600X 6-Core Processor (12 logical CPUs, 7.7 GiB), Linux-6.18.54.1-microsoft-standard-WSL2-x86_64-with-glibc2.41, Python 3.12.15; PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu in Docker. Postgres-vs-host clock offset 0.0 ms.

| Capacity (backlog catch-up) | |
|---|---|
| row changes | 20814 in 5691 transactions |
| time | 12.98 s |
| **sustained changes/second** | **1603** (438.4 transactions/s) |

| Load (share of capacity) | achieved changes/s | p50 | p90 | **p99 freshness** | max queued | peak RSS | checkpoints (mean, max) |
|---|---|---|---|---|---|---|---|
| 25% | 386 | ≤ 0.075 s | ≤ 0.15 s | **≤ 0.15 s** | 0 | 85.6 MiB | 5 (0.05 s, 0.068 s) |
| 50% | 770 | ≤ 0.075 s | ≤ 0.15 s | **≤ 0.15 s** | 3 | 91.5 MiB | 5 (0.049 s, 0.097 s) |
| 75% | 1151 | ≤ 0.075 s | ≤ 0.15 s | **≤ 0.2 s** | 16 | 89.4 MiB | 5 (0.077 s, 0.118 s) |
