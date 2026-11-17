"""Canonical form of a document, so the same data always hashes the same way:
Decimal128 -> string, dates -> epoch ms, arrays sorted by their id, internal fields removed."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from bson.decimal128 import Decimal128

from migrator.transform.mapping import Mapping

INTERNAL = {"_has_row"}


def _scalar(v):
    if isinstance(v, Decimal128):
        return {"$dec": str(v.to_decimal())}
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return {"$ms": int(v.timestamp() * 1000)}
    if isinstance(v, dict):
        return {k: _scalar(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_scalar(x) for x in v]
    return v   # int/Int64 -> int via json, str, bool, None


def _sort_key(x):
    return json.dumps(x, sort_keys=True)


def canonical(m: Mapping, collection: str, doc: dict | None) -> dict | None:
    if doc is None:
        return None
    out = {k: _scalar(v) for k, v in doc.items() if k not in INTERNAL}
    sort_by = {e.as_: e.id for e in m.embeds_of(collection)}
    sort_by |= {s.as_: s.remote_fk for s in m.m2m_of(collection)}
    for field, v in out.items():
        if isinstance(v, list):
            key = sort_by.get(field)
            out[field] = sorted(v, key=(lambda x, k=key: (x.get(k) if isinstance(x, dict) else x))
                                if key else _sort_key)
    return out


def dumps(doc) -> str:
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), default=str)


def doc_hash(doc) -> str:
    return hashlib.sha256(dumps(doc).encode()).hexdigest()


def diff_fields(expected: dict | None, actual: dict | None) -> list[str]:
    if expected is None:
        return ["<extra document in MongoDB>"]
    if actual is None:
        return ["<missing in MongoDB>"]
    keys = set(expected) | set(actual)
    return sorted(k for k in keys if expected.get(k, "<absent>") != actual.get(k, "<absent>"))
