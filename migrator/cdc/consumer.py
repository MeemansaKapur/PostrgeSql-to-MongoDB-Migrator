"""Kafka -> MongoDB sync loop.

At-least-once: offsets are committed manually, only after MongoDB confirms the
writes. Combined with idempotent handlers this gives exactly-once *effects*.
"""
from __future__ import annotations

import csv
import logging
import signal
import statistics
import time
from collections import OrderedDict, deque
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg import sql
from psycopg.rows import dict_row
from pymongo.database import Database
from pymongo.errors import BulkWriteError, ConnectionFailure, OperationFailure, PyMongoError

from migrator.cdc.dlq import DLQ_TOPIC, DeadLetterQueue
from migrator.cdc.handlers import CANARY, Event, Handlers, SchemaDrift
from migrator.transform.mapping import Mapping

log = logging.getLogger("migrator.sync")

TOPIC_PREFIX = "bank.public."
STATUS_ID = "sync:status"
# OperationFailure codes that are worth retrying (stepdowns, interrupted ops)
TRANSIENT_CODES = {6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436}


class PermanentError(Exception):
    pass


def is_transient(e: Exception) -> bool:
    if isinstance(e, ConnectionFailure):          # AutoReconnect, NetworkTimeout, ServerSelectionTimeout
        return True
    if isinstance(e, BulkWriteError):
        return False
    if isinstance(e, OperationFailure):
        return e.code in TRANSIENT_CODES or e.has_error_label("RetryableWriteError")
    return False


def ensure_topics(bootstrap: str, tables: list[str], partitions: int = 3) -> None:
    """Fix partition counts up front: adding partitions later re-maps keys and
    breaks per-row ordering during the change."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [TOPIC_PREFIX + t for t in tables] + [DLQ_TOPIC]
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in wanted if t not in existing]
    for t, fut in admin.create_topics(new).items() if new else []:
        try:
            fut.result()
            log.info("created topic %s", t)
        except Exception as e:  # created concurrently by Debezium: fine
            log.debug("topic %s: %s", t, e)


def load_lookups(pg_dsn: str, m: Mapping) -> dict[str, dict]:
    out = {}
    with psycopg.connect(pg_dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        for table in m.ext_refs:
            pk = m.pk(table)[0]
            cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
            out[table] = {r[pk]: r for r in cur.fetchall()}
    return out


class LagRecorder:
    """Canary lag = time from the app's write to the value being in MongoDB.
    Source lag  = time from PostgreSQL commit (source.ts_ms) to apply."""

    def __init__(self, csv_path: str | None, window: int = 3000):
        self.canary = deque(maxlen=window)    # (t_ms, lag_ms)
        self.source = deque(maxlen=window)
        self.csv = None
        if csv_path:
            Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
            new = not Path(csv_path).exists()
            self.csv_file = open(csv_path, "a", newline="")
            self.csv = csv.writer(self.csv_file)
            if new:
                self.csv.writerow(["applied_ms", "kind", "lag_ms"])

    def record(self, kind: str, lag_ms: int, now_ms: int) -> None:
        (self.canary if kind == "canary" else self.source).append((now_ms, lag_ms))
        if self.csv and kind == "canary":
            self.csv.writerow([now_ms, kind, lag_ms])

    @staticmethod
    def pct(samples, p: float, since_ms: int) -> float | None:
        vals = sorted(l for t, l in samples if t >= since_ms)
        if not vals:
            return None
        return vals[min(len(vals) - 1, int(round(p * (len(vals) - 1))))]

    def summary(self, window_ms: int = 60_000) -> dict:
        since = int(time.time() * 1000) - window_ms
        return {
            "canary_p50_ms": self.pct(self.canary, 0.50, since),
            "canary_p95_ms": self.pct(self.canary, 0.95, since),
            "source_p50_ms": self.pct(self.source, 0.50, since),
            "source_p95_ms": self.pct(self.source, 0.95, since),
            "canary_samples": sum(1 for t, _ in self.canary if t >= since),
        }

    def flush(self):
        if self.csv:
            self.csv_file.flush()


class SyncConsumer:
    def __init__(self, m: Mapping, db: Database, bootstrap: str, pg_dsn: str,
                 group_id: str = "migrator-sync", batch_size: int = 500,
                 lag_csv: str | None = "results/lag_samples.csv"):
        self.m, self.db, self.batch_size = m, db, batch_size
        self.handlers = Handlers(m, load_lookups(pg_dsn, m))
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,          # commit only after MongoDB confirms
            "auto.offset.reset": "earliest",      # first run: replay everything since capture began
            "max.poll.interval.ms": 300000,
            "topic.metadata.refresh.interval.ms": 5000,
        })
        self.dlq = DeadLetterQueue(bootstrap)
        self.lag = LagRecorder(lag_csv)
        self.paused_tables: dict[str, str] = {}
        self.running = True
        self.applied = 0
        self.transient_retries = 0
        self._last_status = 0.0
        tables = sorted(m.all_tables() | {CANARY})
        ensure_topics(bootstrap, tables)

    # -- writes -----------------------------------------------------------------
    def _write(self, ops: list[tuple[str, object]]) -> None:
        """Group by collection preserving arrival order; ordered bulk_write per collection
        (two ops in one batch may target the same document). Retries transient errors forever
        with capped backoff: offsets aren't committed, so nothing is lost while we wait."""
        by_coll: OrderedDict[str, list] = OrderedDict()
        for coll, op in ops:
            by_coll.setdefault(coll, []).append(op)
        for coll, coll_ops in by_coll.items():
            delay, attempt = 0.5, 0
            while True:
                try:
                    self.db[coll].bulk_write(coll_ops, ordered=True)
                    break
                except PyMongoError as e:
                    if not is_transient(e):
                        raise PermanentError(f"{type(e).__name__}: {getattr(e, 'details', None) or e}") from e
                    attempt += 1
                    self.transient_retries += 1
                    lvl = logging.ERROR if attempt >= 5 else logging.WARNING
                    log.log(lvl, "transient MongoDB error on %s (attempt %d, retry in %.1fs): %s",
                            coll, attempt, delay, e)
                    time.sleep(delay)
                    delay = min(delay * 2, 8.0)
                    if not self.running and attempt >= 3:
                        raise

    def _to_dlq(self, ev: Event, err) -> None:
        self.dlq.send(ev.topic, ev.partition, ev.offset, ev.raw_key, ev.raw_value, err)

    def apply_batch(self, events: list[Event]) -> None:
        live = []
        for ev in events:
            if ev.table in self.paused_tables:
                self._to_dlq(ev, f"table paused: {self.paused_tables[ev.table]}")
            else:
                live.append(ev)
        try:
            ops = [op for ev in live for op in self.handlers.ops_for(ev)]
            self._write(ops)
        except (PermanentError, SchemaDrift, Exception) as e:
            if isinstance(e, ConnectionFailure):
                raise
            log.warning("batch failed (%s); isolating the bad event one by one", e)
            for ev in live:
                self._apply_one(ev)
        now_ms = int(time.time() * 1000)
        for ev in live:
            if ev.table == CANARY and ev.after:
                self.lag.record("canary", now_ms - int(ev.after["written_ms"]), now_ms)
            elif ev.source_ts_ms:
                self.lag.record("source", now_ms - ev.source_ts_ms, now_ms)
        self.applied += len(live)

    def _apply_one(self, ev: Event) -> None:
        if ev.table in self.paused_tables:
            self._to_dlq(ev, f"table paused: {self.paused_tables[ev.table]}")
            return
        try:
            self._write(self.handlers.ops_for(ev))
        except SchemaDrift as e:
            self.paused_tables[ev.table] = str(e)
            log.error("ALERT schema drift, pausing table %s: %s", ev.table, e)
            self._to_dlq(ev, e)
        except ConnectionFailure:
            raise
        except Exception as e:   # PermanentError, conversion errors, bugs
            self._to_dlq(ev, e)

    # -- status -------------------------------------------------------------------
    def _publish_status(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_status < 5:
            return
        self._last_status = now
        s = self.lag.summary()
        self.lag.flush()
        try:
            self.db["_migration_state"].update_one({"_id": STATUS_ID}, {"$set": {
                **s, "applied": self.applied, "dlq_count": self.dlq.count,
                "paused_tables": self.paused_tables, "transient_retries": self.transient_retries,
                "updated_ms": int(now * 1000)}}, upsert=True)
        except PyMongoError:
            pass
        log.info("applied=%d dlq=%d canary_p95=%sms source_p95=%sms",
                 self.applied, self.dlq.count, s["canary_p95_ms"], s["source_p95_ms"])

    # -- loop -----------------------------------------------------------------------
    def stop(self, *_):
        log.info("shutdown requested: finishing current batch")
        self.running = False

    def run(self, max_idle_s: float | None = None) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.consumer.subscribe([r"^bank\.public\..*"])   # leading ^ = regex subscription
        idle_since = time.time()
        try:
            while self.running:
                msgs = self.consumer.consume(num_messages=self.batch_size, timeout=1.0)
                self._publish_status()
                if not msgs:
                    if max_idle_s is not None and time.time() - idle_since > max_idle_s:
                        break
                    continue
                idle_since = time.time()
                events = []
                for msg in msgs:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            log.warning("kafka error: %s", msg.error())
                        continue
                    if msg.value() is None:       # tombstone (disabled in connector, but be safe)
                        continue
                    try:
                        ev = Event.parse(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value())
                        if ev.op not in ("c", "u", "d", "r"):
                            log.warning("skipping op=%s on %s", ev.op, ev.table)
                            continue
                        events.append(ev)
                    except Exception as e:
                        self.dlq.send(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value(), e)
                self.apply_batch(events)
                self.consumer.commit(asynchronous=False)   # only after MongoDB confirmed
        finally:
            self._publish_status(force=True)
            self.consumer.close()   # prompt rebalance instead of waiting for session timeout
            self.dlq.producer.flush(10)
            log.info("sync stopped cleanly (applied=%d, dlq=%d)", self.applied, self.dlq.count)
