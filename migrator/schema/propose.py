"""Classify each table over the foreign-key graph and propose a MongoDB mapping.

Rules, applied in order:
  1. join table   : exactly 2 FKs and the PK is made of them   -> many-to-many on both parents
  2. lookup table : no FKs of its own, referenced, < LOOKUP_MAX -> extended reference
  3. embeddable   : exactly 1 FK, nothing references it, max fan-out <= EMBED_MAX -> embed
  4. otherwise    : root collection with plain id references

The output is a proposal. A human reviews it (query patterns are invisible to the
catalog) and saves the approved version as mapping.yaml.
"""
from __future__ import annotations

from dataclasses import dataclass

from migrator.schema.introspect import Catalog

EMBED_MAX = 100
LOOKUP_MAX = 1000
EXT_REF_FIELDS = 4


@dataclass
class Classification:
    kind: str     # join | lookup | embed | root
    reason: str


def classify(cat: Catalog, ignore: set[str], embed_max: int = EMBED_MAX,
             lookup_max: int = LOOKUP_MAX) -> dict[str, Classification]:
    out: dict[str, Classification] = {}
    lookups = {n for n, t in cat.tables.items()
               if n not in ignore and not cat.fks_of(n) and cat.inbound(n) and t.rows < lookup_max}
    for name, t in cat.tables.items():
        if name in ignore:
            continue
        fks = cat.fks_of(name)
        inbound = cat.inbound(name)
        fk_cols = {f.col for f in fks}
        if len(fks) == 2 and set(t.pk) == fk_cols:
            out[name] = Classification(
                "join", f"join table: composite PK {t.pk} = 2 FKs ({fks[0].parent}, {fks[1].parent})")
        elif name in lookups:
            out[name] = Classification(
                "lookup", f"lookup table ({t.rows} rows < {lookup_max}), no FKs of its own, "
                          f"referenced by {sorted({f.child for f in inbound})}")
        elif len(fks) == 1 and not inbound and fks[0].max_fanout <= embed_max:
            f = fks[0]
            out[name] = Classification(
                "embed", f"1:few into {f.parent} (max {f.max_fanout}, avg {f.avg_fanout:.1f} "
                         f"<= {embed_max}), no inbound FKs")
        else:
            why = []
            if inbound:
                why.append(f"referenced by {sorted({f.child for f in inbound})}")
            owners = [f for f in fks if f.parent not in lookups]  # lookups become extended refs
            big = [f for f in owners if f.max_fanout > embed_max]
            for f in big:
                why.append(f"fan-out per {f.parent} unbounded/large (max {f.max_fanout} > {embed_max})")
            if len(owners) > 1 and not big:
                why.append(f"{len(owners)} parents, so no single owner to embed into")
            if not why:
                why.append("independent entity")
            out[name] = Classification("root", "; ".join(why))
    return out


def _singular(name: str) -> str:
    if name.endswith(("ches", "shes", "xes", "sses")):
        return name[:-2]
    return name[:-1] if name.endswith("s") else name


def _ext_ref_fields(cat: Catalog, table: str) -> list[str]:
    t = cat.tables[table]
    fields = list(t.pk) + [c for c in t.unique if c not in t.pk]
    for c in t.columns:
        if len(fields) >= EXT_REF_FIELDS:
            break
        if c not in fields:
            fields.append(c)
    return fields[:EXT_REF_FIELDS]


def propose(cat: Catalog, ignore: set[str], embed_max: int = EMBED_MAX,
            lookup_max: int = LOOKUP_MAX, database: str = "bank") -> dict:
    kinds = classify(cat, ignore, embed_max, lookup_max)

    embeds: dict[str, list[str]] = {}
    for name, k in kinds.items():
        if k.kind == "embed":
            embeds.setdefault(cat.fks_of(name)[0].parent, []).append(name)

    # A lookup table becomes its own collection only if something embeds into it
    collection_tables = [n for n, k in kinds.items()
                         if k.kind == "root" or (k.kind == "lookup" and n in embeds)]

    collections: dict[str, dict] = {}
    for name in collection_tables:
        t = cat.tables[name]
        coll: dict = {"from_table": name, "id": t.pk[0], "reason": kinds[name].reason}

        for child in embeds.get(name, []):
            f = cat.fks_of(child)[0]
            coll.setdefault("embed", []).append({
                "table": child, "as": child, "fk": f.col, "id": cat.tables[child].pk[0],
                "reason": kinds[child].reason})

        for f in cat.fks_of(name):
            pk = kinds.get(f.parent)
            if pk and pk.kind == "lookup":
                coll.setdefault("extended_refs", []).append({
                    "table": f.parent, "as": _singular(f.parent),
                    "fk": f.col, "fields": _ext_ref_fields(cat, f.parent),
                    "reason": f"{pk.reason}; copy a few fields to avoid a lookup on every read"})
            else:
                coll.setdefault("references", []).append({
                    "field": f.col, "collection": f.parent,
                    "reason": "plain id reference"})

        for jname, k in kinds.items():
            if k.kind != "join":
                continue
            jfks = cat.fks_of(jname)
            mine = [f for f in jfks if f.parent == name]
            if not mine:
                continue
            local = mine[0]
            remote = next(f for f in jfks if f is not local)
            extra = [c for c in cat.tables[jname].columns if c not in (local.col, remote.col)]
            coll.setdefault("many_to_many", []).append({
                "via": jname, "as": remote.parent, "local_fk": local.col,
                "remote_fk": remote.col, "keep_fields": [remote.col, *extra],
                "reason": k.reason})
        collections[name] = coll

    migrated = [n for n in kinds]
    return {
        "source": {"database": database, "schema": "public"},
        "embed_max": embed_max,
        "lookup_max": lookup_max,
        "collections": collections,
        "ignore_tables": sorted(ignore),
        "tables": {n: {"pk": cat.tables[n].pk, "columns": cat.tables[n].columns}
                   for n in migrated},
    }
