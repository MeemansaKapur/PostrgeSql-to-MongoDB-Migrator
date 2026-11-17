"""Benchmark methodology (design doc 12.2):
- same machine, both DBs capped at 2 CPU / 2 GB, indexes on both sides
- answers checked equal before timing (a fast wrong answer is worthless)
- warm-up runs discarded; randomized parameters per run (not one cached row)
- client-side time.perf_counter() incl. driver round trip
- R repetitions, report the median repetition; raw timings saved to CSV
"""
from __future__ import annotations

import csv
import random
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from pymongo.database import Database

from migrator.bench import queries_mongo, queries_pg

SINCE = timedelta(days=365)


def make_params(pg: psycopg.Connection, rng: random.Random, n: int) -> list[dict]:
    max_c, max_a, max_l = pg.execute(
        "SELECT (SELECT MAX(customer_id) FROM customers), (SELECT MAX(account_id) FROM accounts), "
        "(SELECT MAX(loan_id) FROM loans)").fetchone()
    since = datetime.now(timezone.utc) - SINCE
    return [{"customer_id": rng.randint(1, max_c), "account_id": rng.randint(1, max_a),
             "loan_id": rng.randint(1, max_l), "branch_id": rng.randint(1, 50), "since": since}
            for _ in range(n)]


def _pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


def check_equivalence(pg, db) -> dict[str, str]:
    """Same answers from both engines on aggregate queries."""
    out = {}
    pg4 = {b: t for b, t in queries_pg.q4_branch_deposits(pg, {})}
    m4 = {d["_id"]: d["total"].to_decimal() for d in queries_mongo.q4_branch_deposits(db, {})}
    out["Q4"] = "equal" if pg4 == m4 else "DIFFERENT"
    pg6 = [(c, t) for c, t in queries_pg.q6_top_customers(pg, {})]
    m6 = [(d["_id"], d["total"].to_decimal()) for d in queries_mongo.q6_top_customers(db, {})]
    out["Q6"] = "equal" if pg6 == m6 else "DIFFERENT"
    p = {"branch_id": 7, "since": datetime.now(timezone.utc) - SINCE}
    pg5 = {(m.replace(tzinfo=timezone.utc), c): (n, v) for m, c, n, v in queries_pg.q5_monthly_volume_by_type(pg, p)}
    m5 = {(d["_id"]["month"].replace(tzinfo=timezone.utc), d["_id"]["code"]): (d["n"], d["volume"].to_decimal())
          for d in queries_mongo.q5_monthly_volume_by_type(db, p)}
    out["Q5"] = "equal" if pg5 == m5 else "DIFFERENT"
    return out


def run(pg: psycopg.Connection, db: Database, runs: int = 200, warmup: int = 20, reps: int = 3,
        out_dir: str = "results", seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    Path(out_dir).mkdir(exist_ok=True)
    raw_path = Path(out_dir) / "bench_raw.csv"
    raw = []
    rep_stats: dict[tuple, list[dict]] = {}
    for rep in range(1, reps + 1):
        params = make_params(pg, rng, runs + warmup)
        for name in queries_pg.QUERIES:
            for engine, fn, conn in (("postgres", queries_pg.QUERIES[name], pg),
                                     ("mongodb", queries_mongo.QUERIES[name], db)):
                times = []
                for i, p in enumerate(params):
                    t0 = time.perf_counter()
                    fn(conn, p)
                    ms = (time.perf_counter() - t0) * 1000
                    if i >= warmup:
                        times.append(ms)
                        raw.append((rep, name, engine, round(ms, 4)))
                rep_stats.setdefault((name, engine), []).append(
                    {"p50": statistics.median(times), "p95": _pct(times, 0.95), "mean": statistics.fmean(times)})
    with open(raw_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rep", "query", "engine", "ms"])
        w.writerows(raw)

    summary = []
    for name in queries_pg.QUERIES:
        row = {"query": name}
        for engine in ("postgres", "mongodb"):
            reps_ = sorted(rep_stats[(name, engine)], key=lambda s: s["p50"])
            med = reps_[len(reps_) // 2]               # median repetition
            row |= {f"{engine}_{k}": round(v, 3) for k, v in med.items()}
        row["p50_change_pct"] = round((row["postgres_p50"] - row["mongodb_p50"]) / row["postgres_p50"] * 100, 1)
        summary.append(row)
    with open(Path(out_dir) / "bench.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    return summary


def markdown(summary: list[dict]) -> str:
    lines = ["| Query | PostgreSQL p50 / p95 (ms) | MongoDB p50 / p95 (ms) | p50 change |",
             "|---|---|---|---|"]
    for r in summary:
        ch = r["p50_change_pct"]
        verdict = f"{ch:.0f}% faster in MongoDB" if ch > 0 else f"{-ch:.0f}% slower in MongoDB"
        lines.append(f"| {r['query']} | {r['postgres_p50']:.2f} / {r['postgres_p95']:.2f} | "
                     f"{r['mongodb_p50']:.2f} / {r['mongodb_p95']:.2f} | {verdict} |")
    return "\n".join(lines)
