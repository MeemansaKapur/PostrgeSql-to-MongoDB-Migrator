"""Full checksum: walk each collection's primary-key range in chunks; hash every
canonical document on both sides; compare chunk hashes; drill down only into the
rare chunk that differs (the pt-table-checksum idea)."""
from __future__ import annotations

import hashlib
import logging
import time

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from pymongo.database import Database

from migrator.bulk.copier import BulkCopier
from migrator.transform.mapping import Mapping
from migrator.verify.normalize import canonical, diff_fields, doc_hash

log = logging.getLogger("migrator.checksum")


def _chunk_hash(docs: dict) -> str:
    h = hashlib.sha256()
    for _id in sorted(docs):
        h.update(f"{_id}:{docs[_id]}\n".encode())
    return h.hexdigest()


def full_checksum(pg: psycopg.Connection, db: Database, m: Mapping, chunk: int = 10_000,
                  collections: list[str] | None = None) -> list[dict]:
    builder = BulkCopier(pg, db, m)
    report = []
    for coll in collections or list(m.root_cfg):
        cfg = m.root_cfg[coll]
        table, pk = cfg["from_table"], cfg["id"]
        t0 = time.perf_counter()
        lo, hi = pg.execute(sql.SQL("SELECT MIN({pk}), MAX({pk}) FROM {t}").format(
            pk=sql.Identifier(pk), t=sql.Identifier(table))).fetchone()
        # MongoDB may hold ids outside PostgreSQL's range (rows deleted, stubs): include them
        mlo = db[coll].find_one({}, {"_id": 1}, sort=[("_id", 1)])
        mhi = db[coll].find_one({}, {"_id": 1}, sort=[("_id", -1)])
        bounds = [x for x in (lo, hi, mlo and mlo["_id"], mhi and mhi["_id"]) if x is not None]
        docs_checked, chunks, bad_chunks, mismatches = 0, 0, 0, []
        if bounds:
            start, end = min(bounds), max(bounds)
            q = sql.SQL("SELECT * FROM {t} WHERE {pk} >= %s AND {pk} < %s ORDER BY {pk}").format(
                t=sql.Identifier(table), pk=sql.Identifier(pk))
            for c_lo in range(start, end + 1, chunk):
                c_hi = c_lo + chunk
                with pg.cursor(row_factory=dict_row) as cur:
                    cur.execute(q, (c_lo, c_hi))
                    rows = cur.fetchall()
                exp_docs = builder.build_batch(coll, rows)[0] if rows else []
                exp = {d["_id"]: canonical(m, coll, d) for d in exp_docs}
                act = {d["_id"]: canonical(m, coll, d)
                       for d in db[coll].find({"_id": {"$gte": c_lo, "$lt": c_hi}})}
                eh = _chunk_hash({k: doc_hash(v) for k, v in exp.items()})
                ah = _chunk_hash({k: doc_hash(v) for k, v in act.items()})
                chunks += 1
                docs_checked += max(len(exp), len(act))
                if eh != ah:   # drill down
                    bad_chunks += 1
                    for _id in sorted(set(exp) | set(act)):
                        if exp.get(_id) != act.get(_id):
                            mismatches.append({"_id": _id, "fields": diff_fields(exp.get(_id), act.get(_id))})
        secs = time.perf_counter() - t0
        log.info("%-13s %8s docs  %4d chunks  %d bad  %d mismatches  %.1fs",
                 coll, f"{docs_checked:,}", chunks, bad_chunks, len(mismatches), secs)
        report.append({"collection": coll, "documents": docs_checked, "chunks": chunks,
                       "bad_chunks": bad_chunks, "mismatches": mismatches, "seconds": secs})
    return report
