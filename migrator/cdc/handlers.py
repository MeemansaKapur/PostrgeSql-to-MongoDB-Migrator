"""Debezium change event -> list of (collection, PyMongo write op). Pure: no I/O.

Every op sets ABSOLUTE state (never increments), so applying an event twice
gives the same result as once. That's what makes at-least-once delivery safe.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from pymongo import DeleteOne, UpdateMany, UpdateOne

from migrator.transform.documents import embed_element, ext_ref_object, m2m_element, root_fields
from migrator.transform.mapping import Mapping
from migrator.transform.types import convert_row

CANARY = "canary"


class SchemaDrift(Exception):
    """An event is missing columns the mapping expects (dropped/renamed column)."""


@dataclass
class Event:
    topic: str
    partition: int
    offset: int
    table: str
    op: str                     # c u d r
    before: dict | None
    after: dict | None
    source_ts_ms: int
    lsn: int | None = None
    raw_key: bytes | None = None
    raw_value: bytes | None = None

    @classmethod
    def parse(cls, topic: str, partition: int, offset: int, key: bytes | None, value: bytes) -> "Event":
        v = json.loads(value)
        src = v.get("source") or {}
        return cls(topic, partition, offset, src.get("table") or topic.rsplit(".", 1)[-1], v["op"],
                   v.get("before"), v.get("after"), src.get("ts_ms", 0), src.get("lsn"), key, value)


# -- primitive ops -------------------------------------------------------------

def upsert_child(parent_id, array: str, id_field: str, elem: dict) -> UpdateOne:
    """Replace-if-present, append-if-absent, in ONE atomic update (pipeline).
    $literal keeps user strings like "$500 refund" from being read as field paths."""
    return UpdateOne(
        {"_id": parent_id},
        [{"$set": {array: {"$concatArrays": [
            {"$filter": {"input": {"$ifNull": [f"${array}", []]},
                         "cond": {"$ne": [f"$$this.{id_field}", {"$literal": elem[id_field]}]}}},
            [{"$literal": elem}],
        ]}}}],
        upsert=True)   # parent missing -> creates a stub (no _has_row); see reconciliation


def delete_child(parent_id, array: str, id_field: str, child_id) -> UpdateOne:
    return UpdateOne({"_id": parent_id}, {"$pull": {array: {id_field: child_id}}})


def delete_child_anywhere(array: str, id_field: str, child_id) -> UpdateOne:
    """Parent unknown (no REPLICA IDENTITY FULL): find it through the multikey path."""
    return UpdateOne({f"{array}.{id_field}": child_id}, {"$pull": {array: {id_field: child_id}}})


# -- handlers ------------------------------------------------------------------

@dataclass
class Handlers:
    m: Mapping
    lookups: dict[str, dict] = field(default_factory=dict)   # lookup table -> {pk: raw row}

    def check_columns(self, ev: Event) -> None:
        row = ev.after or ev.before
        expected = self.m.columns(ev.table)
        if not row or not expected:
            return
        # Deletes on default replica identity carry only the PK: nothing to compare.
        if ev.op == "d" and set(row) <= set(self.m.pk(ev.table)):
            return
        missing = set(expected) - set(row)
        if missing:
            raise SchemaDrift(f"{ev.table}: columns {sorted(missing)} missing from change event "
                              f"(dropped or renamed?)")

    def ops_for(self, ev: Event) -> list[tuple[str, object]]:
        t = ev.table
        if t == CANARY or t in self.m.ignore_tables:
            return []
        self.check_columns(ev)
        ops: list[tuple[str, object]] = []
        if t in self.m.ext_refs:
            ops += self._lookup(ev)
        if t in self.m.roots:
            ops += self._root(ev)
        if t in self.m.embeds:
            ops += self._embedded(ev)
        if t in self.m.m2m:
            ops += self._m2m(ev)
        return ops

    def _root(self, ev: Event):
        coll = self.m.roots[ev.table]
        pk = self.m.root_cfg[coll]["id"]
        if ev.op == "d":
            return [(coll, DeleteOne({"_id": ev.before[pk]}))]
        fields = root_fields(self.m, coll, ev.after, self.lookups)
        _id = fields.pop("_id")
        fields["_has_row"] = True
        # $set, not replace: embedded arrays are maintained by other topics.
        # Pipeline form so the same atomic op also creates any missing array as []
        # (matches the bulk copy: a new account with no cards has cards: [], not no field).
        stage = {k: {"$literal": v} for k, v in fields.items()}
        for arr in self._arrays(coll):
            stage[arr] = {"$ifNull": [f"${arr}", []]}
        return [(coll, UpdateOne({"_id": _id}, [{"$set": stage}], upsert=True))]

    def _arrays(self, coll: str) -> list[str]:
        return [e.as_ for e in self.m.embeds_of(coll)] + [s.as_ for s in self.m.m2m_of(coll)]

    def _lookup(self, ev: Event):
        """Extended-reference source changed: refresh cache, fan out to every copy."""
        pk = self.m.pk(ev.table)[0]
        if ev.op == "d":
            self.lookups.get(ev.table, {}).pop(ev.before[pk], None)
            return []
        self.lookups.setdefault(ev.table, {})[ev.after[pk]] = ev.after
        ops = []
        for r in self.m.ext_refs[ev.table]:
            obj = ext_ref_object(self.m, r, ev.after)
            ops.append((r.collection, UpdateMany(
                {f"{r.as_}.{pk}": obj[pk]},
                {"$set": {f"{r.as_}.{f}": v for f, v in obj.items()}})))
        return ops

    def _embedded(self, ev: Event):
        e = self.m.embeds[ev.table]
        ops = []
        if ev.op == "d":
            child_id = convert_row(ev.before, self.m.columns(e.table))[e.id]
            parent = ev.before.get(e.fk)
            op = (delete_child(parent, e.as_, e.id, child_id) if parent is not None
                  else delete_child_anywhere(e.as_, e.id, child_id))
            return [(e.collection, op)]
        elem = embed_element(self.m, e, ev.after)
        new_parent = ev.after[e.fk]
        old_parent = (ev.before or {}).get(e.fk)
        if old_parent is not None and old_parent != new_parent:   # child moved to another parent
            ops.append((e.collection, delete_child(old_parent, e.as_, e.id, elem[e.id])))
        ops.append((e.collection, upsert_child(new_parent, e.as_, e.id, elem)))
        return ops

    def _m2m(self, ev: Event):
        ops = []
        for s in self.m.m2m[ev.table]:
            def remove(row):
                local = row[s.local_fk]
                remote = convert_row(row, self.m.columns(s.via))[s.remote_fk]
                pull = {s.as_: {s.remote_fk: remote}} if s.keep_fields else {s.as_: remote}
                return (s.collection, UpdateOne({"_id": local}, {"$pull": pull}))

            if ev.op == "d":
                ops.append(remove(ev.before))
                continue
            b = ev.before or {}
            if b and (b.get(s.local_fk), b.get(s.remote_fk)) != (ev.after[s.local_fk], ev.after[s.remote_fk]):
                ops.append(remove(b))   # key columns changed: drop the old link
            local = ev.after[s.local_fk]
            elem = m2m_element(self.m, s, ev.after)
            if s.keep_fields:
                ops.append((s.collection, upsert_child(local, s.as_, s.remote_fk, elem)))
            else:
                ops.append((s.collection, UpdateOne({"_id": local}, {"$addToSet": {s.as_: elem}}, upsert=True)))
        return ops
