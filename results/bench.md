| Query | PostgreSQL p50 / p95 (ms) | MongoDB p50 / p95 (ms) | p50 change |
|---|---|---|---|
| Q1 customer 360 | 0.26 / 0.37 | 0.36 / 0.41 | 40% slower in MongoDB |
| Q2 account statement | 0.13 / 0.15 | 0.21 / 0.25 | 62% slower in MongoDB |
| Q3 loan + schedule | 0.16 / 0.20 | 0.20 / 0.23 | 26% slower in MongoDB |
| Q4 deposits per branch | 1.72 / 1.84 | 4.63 / 4.92 | 169% slower in MongoDB |
| Q5 monthly volume by type | 8.16 / 8.49 | 11.90 / 14.68 | 46% slower in MongoDB |
| Q6 top 10 customers | 6.09 / 6.25 | 12.14 / 12.45 | 99% slower in MongoDB |
