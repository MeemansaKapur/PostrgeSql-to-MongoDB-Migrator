**Q7 full customer profile (localhost)**

| Variant | p50 (ms) | p95 (ms) | MongoDB vs this (p50) |
|---|---|---|---|
| PostgreSQL A: one 10-table join | 0.27 | 2.46 | 103% slower |
| PostgreSQL B: 7 queries | 0.90 | 0.98 | 39% faster |
| PostgreSQL C: nested json_agg | 0.19 | 0.31 | 193% slower |
| MongoDB: 3 reads | 0.55 | 0.62 | n/a |

