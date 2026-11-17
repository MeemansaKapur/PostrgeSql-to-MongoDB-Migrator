"""Resumable progress, stored in the target (survives the copier's machine/container)."""
from __future__ import annotations

from datetime import datetime, timezone

from pymongo.database import Database

STATE = "_migration_state"


class Checkpoint:
    def __init__(self, db: Database, key: str):
        self.coll = db[STATE]
        self.key = key

    def load(self) -> dict:
        return self.coll.find_one({"_id": self.key}) or {
            "last_pk": None, "rows_done": 0, "child_rows": 0, "done": False}

    def save(self, last_pk, rows_done: int, child_rows: int, done: bool = False) -> None:
        # Written AFTER the batch lands: a crash in between redoes one (idempotent) batch.
        self.coll.update_one(
            {"_id": self.key},
            {"$set": {"last_pk": last_pk, "rows_done": rows_done, "child_rows": child_rows,
                      "done": done, "updated_at": datetime.now(timezone.utc)}},
            upsert=True)

    def reset(self) -> None:
        self.coll.delete_one({"_id": self.key})
