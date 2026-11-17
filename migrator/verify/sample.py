"""Sampled deep checks: rebuild the expected document from PostgreSQL with the same
builder the copier uses, compare with what's in MongoDB, report differing fields.

Random ids are drawn between MIN and MAX (never ORDER BY random(), which sorts the
whole table). Shared-transformer bugs are caught by the golden tests, not here."""
from __future__ import annotations

import random

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from pymongo.database import Database

from migrator.bulk.copier import BulkCopier
from migrator.transform.mapping import Mapping
from migrator.verify.normalize import canonical, diff_fields


def sample_check(pg: psycopg.Connection, db: Database, m: Mapping, n: int = 500,
                 seed: int | None = None) -> list[dict]:
    rng = random.Random(seed)
    builder = BulkCopier(pg, db, m)
    results = []
    for coll, cfg in m.root_cfg.items():
        table, pk = cfg["from_table"], cfg["id"]
        lo, hi = pg.execute(sql.SQL("SELECT MIN({pk}), MAX({pk}) FROM {t}").format(
            pk=sql.Identifier(pk), t=sql.Identifier(table))).fetchone()
        if lo is None:
            continue
        ids = sorted({rng.randint(lo, hi) for _ in range(n)})
        with pg.cursor(row_factory=dict_row) as cur:
            cur.execute(sql.SQL("SELECT * FROM {t} WHERE {pk} = ANY(%s) ORDER BY {pk}").format(
                t=sql.Identifier(table), pk=sql.Identifier(pk)), (ids,))
            rows = cur.fetchall()
        expected_docs, _ = builder.build_batch(coll, rows) if rows else ([], 0)
        expected = {d["_id"]: canonical(m, coll, d) for d in expected_docs}
        actual = {d["_id"]: canonical(m, coll, d) for d in db[coll].find({"_id": {"$in": ids}})}
        mismatches = []
        for i in ids:   # ids absent from PG must also be absent from MongoDB (deleted rows)
            e, a = expected.get(i), actual.get(i)
            if e != a:
                mismatches.append({"_id": i, "fields": diff_fields(e, a)})
        results.append({"collection": coll, "sampled_ids": len(ids), "existing_rows": len(rows),
                        "mismatches": mismatches})
    return results
