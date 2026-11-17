"""Lag from three angles:
  - replication lag: canary / source.ts_ms percentiles published by the sync consumer
  - Kafka consumer lag: high watermark - committed offset, per partition
  - replication slot lag: WAL PostgreSQL is retaining for Debezium (disk-fill risk)
"""
from __future__ import annotations

import time

import psycopg
from confluent_kafka import Consumer, TopicPartition
from pymongo.database import Database

from migrator.cdc.consumer import STATUS_ID


def sync_status(db: Database) -> dict:
    s = db["_migration_state"].find_one({"_id": STATUS_ID}) or {}
    if s.get("updated_ms"):
        s["status_age_s"] = round(time.time() - s["updated_ms"] / 1000, 1)
    return s


def consumer_lag(bootstrap: str, group_id: str = "migrator-sync", prefix: str = "bank.public.") -> dict:
    """Querying committed offsets with the group id does not join the group."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id, "enable.auto.commit": False})
    try:
        md = c.list_topics(timeout=10)
        parts = [TopicPartition(t, p) for t, tm in md.topics.items() if t.startswith(prefix)
                 for p in tm.partitions]
        committed = c.committed(parts, timeout=10)
        total, per_topic = 0, {}
        for tp in committed:
            lo, hi = c.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)
            pos = tp.offset if tp.offset >= 0 else lo     # nothing committed yet -> from earliest
            behind = max(0, hi - pos)
            total += behind
            if behind:
                per_topic[tp.topic] = per_topic.get(tp.topic, 0) + behind
        return {"messages_behind": total, "by_topic": per_topic}
    finally:
        c.close()


def slot_lag(pg: psycopg.Connection) -> list[dict]:
    rows = pg.execute(
        "SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, "
        "wal_status FROM pg_replication_slots").fetchall()
    return [{"slot": r[0], "active": r[1], "retained_bytes": r[2], "wal_status": r[3]} for r in rows]
