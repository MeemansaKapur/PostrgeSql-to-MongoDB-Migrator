"""Copy every root table into MongoDB as fully built documents.

- keyset pagination (WHERE pk > last ORDER BY pk LIMIT n): every page costs the same
- one query per child table per batch (= ANY(ids)), never one per parent (N+1)
- ReplaceOne(upsert) + checkpoint after the write: re-running a batch is harmless
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from pymongo import ReplaceOne
from pymongo.database import Database

from migrator.bulk.checkpoint import Checkpoint
from migrator.transform.documents import build_document
from migrator.transform.mapping import Mapping

log = logging.getLogger("migrator.bulk")

# readable progress: small first, largest last
DEFAULT_ORDER = ["branches", "customers", "accounts", "loans", "transactions"]


class BulkCopier:
    def __init__(self, pg: psycopg.Connection, db: Database, mapping: Mapping, batch_size: int = 2000):
        self.pg, self.db, self.m, self.batch_size = pg, db, mapping, batch_size
        self.lookups = self._load_lookups()

    def _fetch(self, query, params=()) -> list[dict]:
        with self.pg.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def _load_lookups(self) -> dict[str, dict]:
        """Lookup tables are tiny: load once, attach from memory."""
        out = {}
        for table in self.m.ext_refs:
            pk = self.m.pk(table)[0]
            rows = self._fetch(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
            out[table] = {r[pk]: r for r in rows}
        return out

    def _children_by_parent(self, table: str, fk: str, ids: list) -> dict:
        grouped = defaultdict(list)
        q = sql.SQL("SELECT * FROM {} WHERE {} = ANY(%s)").format(sql.Identifier(table), sql.Identifier(fk))
        for r in self._fetch(q, (ids,)):
            grouped[r[fk]].append(r)
        return grouped

    def build_batch(self, collection: str, rows: list[dict]) -> tuple[list[dict], int]:
        """Rows of the root table -> documents. Also used by the verifier."""
        cfg = self.m.root_cfg[collection]
        ids = [r[cfg["id"]] for r in rows]
        child_rows = 0
        children = {}
        for e in self.m.embeds_of(collection):
            children[e.as_] = self._children_by_parent(e.table, e.fk, ids)
            child_rows += sum(len(v) for v in children[e.as_].values())
        joins = {}
        for s in self.m.m2m_of(collection):
            joins[s.as_] = self._children_by_parent(s.via, s.local_fk, ids)
            child_rows += sum(len(v) for v in joins[s.as_].values())
        docs = []
        for r in rows:
            pid = r[cfg["id"]]
            docs.append(build_document(
                self.m, collection, r, self.lookups,
                {k: v.get(pid, []) for k, v in children.items()},
                {k: v.get(pid, []) for k, v in joins.items()}))
        return docs, child_rows

    def copy_collection(self, collection: str) -> dict:
        cfg = self.m.root_cfg[collection]
        table, pk = cfg["from_table"], cfg["id"]
        ckpt = Checkpoint(self.db, f"bulk:{collection}")
        state = ckpt.load()
        last_pk, rows_done, child_rows = state["last_pk"], state["rows_done"], state["child_rows"]
        if state.get("done"):
            log.info("%-13s already done (%s rows), skipping", collection, f"{rows_done:,}")
            return {"collection": collection, "rows": rows_done, "child_rows": child_rows, "seconds": 0.0}
        if last_pk is not None:
            log.info("%-13s resuming after %s=%s (%s rows already copied)", collection, pk, last_pk, f"{rows_done:,}")

        first = sql.SQL("SELECT * FROM {t} ORDER BY {pk} LIMIT %s").format(
            t=sql.Identifier(table), pk=sql.Identifier(pk))
        page = sql.SQL("SELECT * FROM {t} WHERE {pk} > %s ORDER BY {pk} LIMIT %s").format(
            t=sql.Identifier(table), pk=sql.Identifier(pk))
        coll = self.db[collection]
        t0 = time.perf_counter()
        copied_now = 0
        while True:
            rows = (self._fetch(first, (self.batch_size,)) if last_pk is None
                    else self._fetch(page, (last_pk, self.batch_size)))
            if not rows:
                break
            docs, n_child = self.build_batch(collection, rows)
            coll.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
            last_pk = rows[-1][pk]
            rows_done += len(rows)
            child_rows += n_child
            copied_now += len(rows) + n_child
            ckpt.save(last_pk, rows_done, child_rows)
        secs = time.perf_counter() - t0
        ckpt.save(last_pk, rows_done, child_rows, done=True)
        rate = copied_now / secs if secs > 0 else 0
        log.info("%-13s %9s docs  %9s child rows  %6.1fs  %8s rows/s",
                 collection, f"{rows_done:,}", f"{child_rows:,}", secs, f"{rate:,.0f}")
        return {"collection": collection, "rows": rows_done, "child_rows": child_rows,
                "seconds": secs, "rows_per_s": rate}

    def run(self, collections: list[str] | None = None) -> list[dict]:
        names = collections or [c for c in DEFAULT_ORDER if c in self.m.root_cfg] + \
            [c for c in self.m.root_cfg if c not in DEFAULT_ORDER]
        return [self.copy_collection(c) for c in names]


def reset_checkpoints(db: Database) -> None:
    db["_migration_state"].delete_many({"_id": {"$regex": "^bulk:"}})
