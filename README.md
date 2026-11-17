# PostgreSQL → MongoDB Online Migrator

Migrates a live 12-table banking schema from PostgreSQL to MongoDB **while the application keeps writing**.
Capture first (Debezium CDC → Kafka), bulk copy second, replay third through an idempotent consumer, then verify.

## Results (laptop, Docker, PostgreSQL and MongoDB each capped at 2 vCPU / 2 GB)

| Metric | Value | Conditions |
|---|---|---|
| Source schema | 12 tables → 5 collections | `mapping.yaml` |
| Rows at seed | 322,732 (150K transactions) | `tools/seed.py`, seed 42 |
| Bulk copy | 210,447 documents built from ~374K rows in 5.4 s (~70K rows/s) | batch 2,000, load generator running at the same time |
| Replication lag (canary, steady state) | p50 750 ms, **p95 1.21 s**, max 1.5 s | 295 writes/s sustained for 60 s |
| Verification after consumer `kill -9` under load | **0 mismatches across 264,419 documents** | full chunked SHA-256 checksum, `results/verify_full_chaos_consumer_kill.json` |
| Bulk copier `kill -9` | resumed at txn_id 48,000, not from zero | checkpoint in `_migration_state` |

## Benchmark: SQL joins vs document model / aggregation pipelines

Median of 3 repetitions × 200 randomized runs (20 warm-up runs discarded), client-side timing including the driver round trip.
Answers were checked to be identical before timing (Q4, Q5, Q6).

| Query | PostgreSQL p50 / p95 (ms) | MongoDB p50 / p95 (ms) | p50 change |
|---|---|---|---|
| Q1 customer 360 | 0.26 / 0.37 | 0.36 / 0.41 | 40% slower in MongoDB |
| Q2 account statement | 0.13 / 0.15 | 0.21 / 0.25 | 62% slower in MongoDB |
| Q3 loan + schedule | 0.16 / 0.20 | 0.20 / 0.23 | 26% slower in MongoDB |
| Q4 deposits per branch | 1.72 / 1.84 | 4.63 / 4.92 | 169% slower in MongoDB |
| Q5 monthly volume by type | 8.16 / 8.49 | 11.90 / 14.68 | 46% slower in MongoDB |
| Q6 top 10 customers | 6.09 / 6.25 | 12.14 / 12.45 | 99% slower in MongoDB |

**PostgreSQL wins every query at this scale, and that's the honest result.** The whole dataset fits in memory,
so PostgreSQL's indexed joins cost microseconds. The document model removes joins, but the joins it removes
are cheap here. Q4/Q6 scan whole account documents (which carry embedded cards and holders) where PostgreSQL scans
narrow rows. Raw timings: `results/bench_raw.csv`.

## How it works

1. **Capture first.** Debezium (`snapshot.mode=no_data`) starts streaming every change into Kafka before anything is copied.
2. **Bulk copy.** Keyset pagination (`WHERE pk > last ORDER BY pk LIMIT n`), one `= ANY(ids)` query per child table per batch (no N+1),
   `ReplaceOne(upsert)`, checkpoint written *after* each batch.
3. **Replay.** The consumer reads from the earliest offset. For any row, all its events share a key → partition → order,
   and each carries the full `after` image, so the last event equals the current state. The copy converges.
4. **Verify.** Counts, sampled deep checks, and a full chunked checksum.

### Data model (12 tables → 5 collections)

| Relationship | Decision | Why |
|---|---|---|
| customers → addresses, beneficiaries | embed | 1:few, read with customer |
| accounts → cards | embed | 1:few, no children of its own |
| accounts → transactions | reference | unbounded (max 8,904 per account) |
| loans → loan_payments | embed (**manual override**) | bounded by tenure ≤ 360 → ~54 KB worst case |
| accounts ↔ customers | two-way reference | many-to-many |
| accounts → account_types, branches | extended reference | small lookups, rarely change; fan-out `update_many` on change |
| branches → employees | embed | ~12 per branch |

`migrator propose` derives this from the FK graph + fan-out stats (`mapping.proposed.yaml`); a human reviews it into `mapping.yaml`.

### Idempotency and ordering
- Manual Kafka commits after MongoDB confirms → at-least-once; every write sets absolute state → exactly-once *effects*.
- Embedded children use a single pipeline update (filter out old element + append new), with `$literal` so values like `"$500 refund"` aren't read as field paths.
- Cross-topic order isn't guaranteed: a child can arrive before its parent and create a stub; real rows carry `_has_row: true`, and the verifier alerts on stubs older than 60 s.
- Transient MongoDB errors retry with capped backoff (offsets not committed); permanent errors go to the `migrator.dlq` topic.

## Run it

```bash
docker compose up -d
python tools/seed.py --transactions 150000
migrator propose -o mapping.proposed.yaml    # review → mapping.yaml
migrator connector register                  # step 1: start capturing
python tools/loadgen.py --rate 300 &         # the "live app"
migrator bulk                                # step 2
migrator sync                                # step 3 (keep running)
migrator verify --watch                      # another terminal: lag + drift alerts
migrator verify --full                       # at cutover: full checksum
migrator bench
```

Postgres is exposed on **5433** (5432 is commonly taken). Unit tests: `pytest`. End-to-end: `pytest tests/integration -m integration` (needs the stack + a running `sync`).

## Limitations
- No reverse sync (MongoDB → PostgreSQL) for rollback.
- Dropped/renamed columns are detected and the table is paused (events go to the DLQ); not auto-handled.
- Stub reconciliation alerts but doesn't auto-delete orphans.
- Single Kafka broker, replication factor 1 (production: 3 brokers, RF 3, `min.insync.replicas=2`).
