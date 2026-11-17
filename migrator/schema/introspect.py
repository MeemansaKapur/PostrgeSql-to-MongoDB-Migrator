"""Read PostgreSQL's catalog: tables, columns, keys, foreign keys, fan-out stats."""
from __future__ import annotations

from dataclasses import dataclass, field

import psycopg
from psycopg import sql

COLUMNS_SQL = """
SELECT c.table_name, c.column_name, c.data_type, c.ordinal_position
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_name = c.table_name AND t.table_schema = c.table_schema
WHERE c.table_schema = %s AND t.table_type = 'BASE TABLE'
ORDER BY c.table_name, c.ordinal_position
"""

# constraint_type in ('PRIMARY KEY', 'UNIQUE')
KEYS_SQL = """
SELECT tc.table_name, tc.constraint_type, tc.constraint_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = %s AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY tc.table_name, kcu.ordinal_position
"""

FK_SQL = """
SELECT tc.table_name, kcu.column_name, ccu.table_name, ccu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s
ORDER BY tc.table_name, kcu.column_name
"""


@dataclass
class ForeignKey:
    child: str
    col: str
    parent: str
    parent_col: str
    max_fanout: int = 0
    avg_fanout: float = 0.0


@dataclass
class Table:
    name: str
    columns: dict[str, str] = field(default_factory=dict)   # name -> pg data_type
    pk: list[str] = field(default_factory=list)
    unique: list[str] = field(default_factory=list)          # single-column UNIQUEs
    rows: int = 0


@dataclass
class Catalog:
    tables: dict[str, Table]
    fks: list[ForeignKey]

    def fks_of(self, table: str) -> list[ForeignKey]:
        return [f for f in self.fks if f.child == table]

    def inbound(self, table: str) -> list[ForeignKey]:
        return [f for f in self.fks if f.parent == table]


def row_count(conn: psycopg.Connection, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        return cur.fetchone()[0]


def fanout(conn: psycopg.Connection, child: str, col: str) -> tuple[int, float]:
    q = sql.SQL("SELECT COALESCE(MAX(c), 0), COALESCE(AVG(c), 0) "
                "FROM (SELECT COUNT(*) AS c FROM {} GROUP BY {}) t"
                ).format(sql.Identifier(child), sql.Identifier(col))
    with conn.cursor() as cur:
        cur.execute(q)
        mx, avg = cur.fetchone()
        return int(mx), float(avg)


def introspect(conn: psycopg.Connection, schema: str = "public") -> Catalog:
    tables: dict[str, Table] = {}
    with conn.cursor() as cur:
        cur.execute(COLUMNS_SQL, (schema,))
        for tname, col, dtype, _ in cur.fetchall():
            tables.setdefault(tname, Table(tname)).columns[col] = dtype

        cur.execute(KEYS_SQL, (schema,))
        uniques: dict[str, list[str]] = {}
        for tname, ctype, cname, col in cur.fetchall():
            if ctype == "PRIMARY KEY":
                tables[tname].pk.append(col)
            else:
                uniques.setdefault(cname, []).append(col)
                tables[tname].unique.append(col)
        # keep only single-column unique constraints
        for cols in uniques.values():
            if len(cols) > 1:
                for t in tables.values():
                    t.unique = [c for c in t.unique if c not in cols]

        cur.execute(FK_SQL, (schema,))
        fks = [ForeignKey(*r) for r in cur.fetchall()]

    for t in tables.values():
        t.rows = row_count(conn, t.name)
    for f in fks:
        f.max_fanout, f.avg_fanout = fanout(conn, f.child, f.col)
    return Catalog(tables, fks)
