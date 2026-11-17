"""Indexes mapping.yaml by source table, so bulk/CDC/verify can ask
"where does a row of table X go?" without re-walking the YAML."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Embed:
    collection: str      # parent collection
    parent_table: str
    table: str
    as_: str             # array field name
    fk: str              # column in child pointing at parent
    id: str              # child's own pk (element identity in the array)


@dataclass(frozen=True)
class ExtRef:
    collection: str
    table: str           # lookup table
    as_: str             # sub-document field name
    fk: str              # column on the root row pointing at the lookup
    fields: tuple[str, ...]


@dataclass(frozen=True)
class ManyToMany:
    collection: str
    via: str             # join table
    as_: str
    local_fk: str        # join column pointing at this collection's row
    remote_fk: str       # join column pointing at the other side
    keep_fields: tuple[str, ...] | None   # None -> array of remote ids


@dataclass
class Mapping:
    raw: dict
    roots: dict[str, str] = field(default_factory=dict)          # table -> collection
    root_cfg: dict[str, dict] = field(default_factory=dict)      # collection -> cfg
    embeds: dict[str, Embed] = field(default_factory=dict)       # child table -> Embed
    ext_refs: dict[str, list[ExtRef]] = field(default_factory=dict)   # lookup table -> refs
    m2m: dict[str, list[ManyToMany]] = field(default_factory=dict)    # join table -> sides

    @classmethod
    def from_dict(cls, raw: dict) -> "Mapping":
        m = cls(raw)
        for name, c in raw["collections"].items():
            m.roots[c["from_table"]] = name
            m.root_cfg[name] = c
            for e in c.get("embed", []):
                m.embeds[e["table"]] = Embed(name, c["from_table"], e["table"], e["as"], e["fk"], e["id"])
            for r in c.get("extended_refs", []):
                m.ext_refs.setdefault(r["table"], []).append(
                    ExtRef(name, r["table"], r["as"], r["fk"], tuple(r["fields"])))
            for mm in c.get("many_to_many", []):
                kf = mm.get("keep_fields")
                m.m2m.setdefault(mm["via"], []).append(
                    ManyToMany(name, mm["via"], mm["as"], mm["local_fk"], mm["remote_fk"],
                               tuple(kf) if kf else None))
        return m

    # -- column / key metadata ------------------------------------------------
    def columns(self, table: str) -> dict[str, str]:
        return self.raw.get("tables", {}).get(table, {}).get("columns", {})

    def pk(self, table: str) -> list[str]:
        return self.raw.get("tables", {}).get(table, {}).get("pk", [])

    @property
    def ignore_tables(self) -> set[str]:
        return set(self.raw.get("ignore_tables", []))

    def embeds_of(self, collection: str) -> list[Embed]:
        return [e for e in self.embeds.values() if e.collection == collection]

    def ext_refs_of(self, collection: str) -> list[ExtRef]:
        return [r for refs in self.ext_refs.values() for r in refs if r.collection == collection]

    def m2m_of(self, collection: str) -> list[ManyToMany]:
        return [s for sides in self.m2m.values() for s in sides if s.collection == collection]

    def all_tables(self) -> set[str]:
        return set(self.roots) | set(self.embeds) | set(self.ext_refs) | set(self.m2m)
