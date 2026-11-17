"""Cheap count checks: root rows vs real documents, child rows vs array sizes, stubs."""
from __future__ import annotations

import psycopg
from psycopg import sql
from pymongo.database import Database

from migrator.transform.mapping import Mapping


def _pg_count(pg: psycopg.Connection, table: str) -> int:
    return pg.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))).fetchone()[0]


def _array_total(db: Database, collection: str, field: str) -> int:
    res = list(db[collection].aggregate([
        {"$match": {"_has_row": True}},
        {"$group": {"_id": None, "total": {"$sum": {"$size": {"$ifNull": [f"${field}", []]}}}}}]))
    return res[0]["total"] if res else 0


def count_checks(pg: psycopg.Connection, db: Database, m: Mapping) -> list[dict]:
    out = []
    for coll, cfg in m.root_cfg.items():
        out.append({"check": f"{cfg['from_table']} -> {coll}",
                    "pg": _pg_count(pg, cfg["from_table"]),
                    "mongo": db[coll].count_documents({"_has_row": True})})
    for e in m.embeds.values():
        out.append({"check": f"{e.table} -> {e.collection}.{e.as_}[]",
                    "pg": _pg_count(pg, e.table), "mongo": _array_total(db, e.collection, e.as_)})
    for sides in m.m2m.values():
        for s in sides:
            out.append({"check": f"{s.via} -> {s.collection}.{s.as_}[]",
                        "pg": _pg_count(pg, s.via), "mongo": _array_total(db, s.collection, s.as_)})
    for r in out:
        r["diff"] = r["mongo"] - r["pg"]
    return out


def stubs(db: Database, m: Mapping) -> dict[str, list]:
    """Documents created by a child event whose parent row never arrived (no _has_row)."""
    out = {}
    for coll in m.root_cfg:
        ids = [d["_id"] for d in db[coll].find({"_has_row": {"$exists": False}}, {"_id": 1}).limit(1000)]
        if ids:
            out[coll] = ids
    return out
