"""Verifier loop: counts every 30 s, samples every 5 min, alerts per design doc 11.5."""
from __future__ import annotations

import logging
import time

import psycopg
from pymongo.database import Database

from migrator.transform.mapping import Mapping
from migrator.verify.alerts import Alerts
from migrator.verify.counts import count_checks, stubs
from migrator.verify.lag import consumer_lag, slot_lag, sync_status
from migrator.verify.sample import sample_check

log = logging.getLogger("migrator.verify")

SLOT_WARN_BYTES = 1 << 30   # 1 GiB of retained WAL (cap is 2 GB)


class Verifier:
    def __init__(self, pg: psycopg.Connection, db: Database, m: Mapping, bootstrap: str,
                 sample_size: int = 500):
        self.pg, self.db, self.m, self.bootstrap, self.sample_size = pg, db, m, bootstrap, sample_size
        self.alerts = Alerts()
        self.prev_behind: list[int] = []
        self.stub_seen: dict[tuple, float] = {}

    def lag_checks(self) -> dict:
        st = sync_status(self.db)
        cl = consumer_lag(self.bootstrap)
        slots = slot_lag(self.pg)
        p95 = st.get("canary_p95_ms")
        self.alerts.check("replication_lag_p95", p95 is not None and p95 > 5000,
                          f"canary p95 {p95} ms > 5000 ms", for_checks=2)
        self.prev_behind = (self.prev_behind + [cl["messages_behind"]])[-4:]
        growing = len(self.prev_behind) == 4 and all(
            b > a for a, b in zip(self.prev_behind, self.prev_behind[1:]))
        self.alerts.check("consumer_lag_growing", growing, f"messages behind: {self.prev_behind}")
        self.alerts.check("dlq_messages", st.get("dlq_count", 0) > 0, f"{st.get('dlq_count')} events in DLQ")
        self.alerts.check("schema_drift", bool(st.get("paused_tables")), f"paused: {st.get('paused_tables')}")
        for s in slots:
            self.alerts.check(f"slot_{s['slot']}", (s["retained_bytes"] or 0) > SLOT_WARN_BYTES
                              or s["wal_status"] == "lost",
                              f"retains {s['retained_bytes']} bytes, wal_status={s['wal_status']}")
        return {"sync": {k: st.get(k) for k in ("canary_p50_ms", "canary_p95_ms", "source_p95_ms",
                                                "applied", "dlq_count", "status_age_s")},
                "kafka": cl, "slots": slots}

    def count_round(self, caught_up: bool) -> list[dict]:
        counts = count_checks(self.pg, self.db, self.m)
        bad = [c for c in counts if c["diff"]]
        # During sync counts differ a little because of lag: only real drift if it persists
        # for 3 checks while the pipeline is caught up.
        self.alerts.check("count_mismatch", bool(bad) and caught_up,
                          "; ".join(f"{c['check']} {c['diff']:+d}" for c in bad), for_checks=3)
        now = time.time()
        current = {(c, i) for c, ids in stubs(self.db, self.m).items() for i in ids}
        self.stub_seen = {k: self.stub_seen.get(k, now) for k in current}
        old = [k for k, t in self.stub_seen.items() if now - t > 60]
        self.alerts.check("stale_stubs", bool(old), f"{len(old)} stubs older than 60 s: {old[:5]}")
        return counts

    def sample_round(self) -> list[dict]:
        res = sample_check(self.pg, self.db, self.m, self.sample_size)
        n = sum(len(r["mismatches"]) for r in res)
        self.alerts.check("sample_mismatch", n > 0,
                          f"{n} mismatches: " + "; ".join(f"{r['collection']}:{r['mismatches'][:2]}"
                                                          for r in res if r["mismatches"]), for_checks=2)
        return res

    def watch(self, count_every: float = 30, sample_every: float = 300) -> None:
        last_sample = 0.0
        while True:
            lag = self.lag_checks()
            caught_up = (lag["sync"].get("canary_p95_ms") or 0) < 2000 and lag["kafka"]["messages_behind"] < 1000
            counts = self.count_round(caught_up)
            log.info("lag p95=%sms behind=%d | count diffs: %s", lag["sync"].get("canary_p95_ms"),
                     lag["kafka"]["messages_behind"],
                     {c["check"]: c["diff"] for c in counts if c["diff"]} or "none")
            if time.time() - last_sample >= sample_every:
                res = self.sample_round()
                log.info("sample: %s", {r["collection"]: len(r["mismatches"]) for r in res})
                last_sample = time.time()
            time.sleep(count_every)
