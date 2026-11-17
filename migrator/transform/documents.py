"""Row(s) -> MongoDB document. Shared by the bulk copier, CDC handlers and verifier,
so all three agree exactly on the shape of every document."""
from __future__ import annotations

from migrator.transform.mapping import Embed, ExtRef, ManyToMany, Mapping
from migrator.transform.types import convert_row


def root_fields(m: Mapping, collection: str, row: dict, lookups: dict[str, dict]) -> dict:
    """The parent's own columns (+ extended-reference sub-documents), without arrays.

    The pk becomes _id; FK columns replaced by an extended reference are dropped
    (the id lives inside the sub-document instead)."""
    cfg = m.root_cfg[collection]
    table = cfg["from_table"]
    conv = convert_row(row, m.columns(table))
    refs = m.ext_refs_of(collection)
    replaced = {r.fk for r in refs}
    doc = {"_id": conv[cfg["id"]]}
    for k, v in conv.items():
        if k != cfg["id"] and k not in replaced:
            doc[k] = v
    for r in refs:
        doc[r.as_] = ext_ref_object(m, r, lookups.get(r.table, {}).get(row.get(r.fk)))
    return doc


def ext_ref_object(m: Mapping, r: ExtRef, lookup_row: dict | None) -> dict | None:
    if lookup_row is None:
        return None
    conv = convert_row(lookup_row, m.columns(r.table))
    return {f: conv.get(f) for f in r.fields}


def embed_element(m: Mapping, e: Embed, row: dict) -> dict:
    """A child row as an array element: every column except the FK to the parent."""
    conv = convert_row(row, m.columns(e.table))
    return {k: v for k, v in conv.items() if k != e.fk}


def m2m_element(m: Mapping, s: ManyToMany, row: dict):
    conv = convert_row(row, m.columns(s.via))
    if s.keep_fields is None:
        return conv[s.remote_fk]
    return {f: conv.get(f) for f in s.keep_fields}


def build_document(m: Mapping, collection: str, row: dict, lookups: dict[str, dict],
                   children: dict[str, list[dict]], joins: dict[str, list[dict]]) -> dict:
    """children: array name -> child rows; joins: array name -> join-table rows."""
    doc = root_fields(m, collection, row, lookups)
    for e in m.embeds_of(collection):
        elems = [embed_element(m, e, r) for r in children.get(e.as_, [])]
        doc[e.as_] = sorted(elems, key=lambda x: x[e.id])
    for s in m.m2m_of(collection):
        elems = [m2m_element(m, s, r) for r in joins.get(s.as_, [])]
        doc[s.as_] = sorted(elems, key=lambda x: x if s.keep_fields is None else x[s.remote_fk])
    doc["_has_row"] = True
    return doc
