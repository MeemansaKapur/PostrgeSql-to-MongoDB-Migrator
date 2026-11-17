"""Q7: full customer profile (customer + addresses + beneficiaries + accounts with
type/branch/cards + loans with payment schedules). 10 tables in SQL.

Every variant returns the SAME shape, and assembling it is part of the timed work
(the application needs this shape, whichever database it comes from):
    {"customer", "addresses", "beneficiaries", "accounts": [{..., type, branch, cards}],
     "loans": [{..., payments}]}

PostgreSQL is measured three ways, so the comparison can't be cherry-picked:
  A  one big LEFT JOIN, de-duplicated in the app (what ORM eager loading does)
  B  one query per table (7 round trips)
  C  nested json_agg subqueries (PostgreSQL's best tool: 1 round trip, no duplicate rows)
"""
from __future__ import annotations

import csv
import json
import random
import statistics
import time
from collections import defaultdict
from decimal import Decimal
from functools import partial
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import set_json_loads
from pymongo.database import Database

ADDR = ("address_id", "type", "line1", "city", "state", "pincode")
BEN = ("beneficiary_id", "name", "account_no", "ifsc", "added_at")
ACC = ("account_id", "account_no", "balance", "status", "opened_at")
TYPE = ("type_id", "code", "name", "interest_rate")
BR = ("branch_id", "code", "name", "ifsc")
CARD = ("card_id", "last4", "network", "expires_on", "status")
LOAN = ("loan_id", "branch_id", "principal", "rate", "tenure_months", "status", "disbursed_on")
PAY = ("payment_id", "due_date", "paid_on", "amount", "status")


# -- A: one big join ------------------------------------------------------------
def _cols(alias, cols, prefix):
    return ", ".join(f"{alias}.{c} AS {prefix}_{c}" for c in cols)


SQL_A = f"""
SELECT c.customer_id, c.name, c.email, c.phone, c.dob, c.kyc_status, c.created_at,
       {_cols('a', ADDR, 'a')}, {_cols('b', BEN, 'b')}, h.role AS h_role,
       {_cols('ac', ACC, 'ac')}, {_cols('at', TYPE, 'at')}, {_cols('br', BR, 'br')},
       {_cols('cd', CARD, 'cd')}, {_cols('l', LOAN, 'l')}, {_cols('lp', PAY, 'lp')}
FROM customers c
LEFT JOIN addresses a       ON a.customer_id = c.customer_id
LEFT JOIN beneficiaries b   ON b.customer_id = c.customer_id
LEFT JOIN account_holders h ON h.customer_id = c.customer_id
LEFT JOIN accounts ac       ON ac.account_id = h.account_id
LEFT JOIN account_types at  ON at.type_id = ac.type_id
LEFT JOIN branches br       ON br.branch_id = ac.branch_id
LEFT JOIN cards cd          ON cd.account_id = ac.account_id
LEFT JOIN loans l           ON l.customer_id = c.customer_id
LEFT JOIN loan_payments lp  ON lp.loan_id = l.loan_id
WHERE c.customer_id = %s
"""


def _pick(row, prefix, cols):
    return {c: row[f"{prefix}_{c}"] for c in cols}


def pg_a(pg: psycopg.Connection, cid: int) -> dict:
    with pg.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(SQL_A, (cid,)).fetchall()
    if not rows:
        return None
    r0 = rows[0]
    customer = {k: r0[k] for k in ("customer_id", "name", "email", "phone", "dob", "kyc_status", "created_at")}
    addrs, bens, accs, cards, loans, pays = {}, {}, {}, defaultdict(dict), {}, defaultdict(dict)
    for r in rows:   # de-duplicate the cartesian product
        if r["a_address_id"] is not None:
            addrs.setdefault(r["a_address_id"], _pick(r, "a", ADDR))
        if r["b_beneficiary_id"] is not None:
            bens.setdefault(r["b_beneficiary_id"], _pick(r, "b", BEN))
        if r["ac_account_id"] is not None:
            aid = r["ac_account_id"]
            if aid not in accs:
                accs[aid] = {**_pick(r, "ac", ACC), "role": r["h_role"],
                             "type": _pick(r, "at", TYPE), "branch": _pick(r, "br", BR)}
            if r["cd_card_id"] is not None:
                cards[aid].setdefault(r["cd_card_id"], _pick(r, "cd", CARD))
        if r["l_loan_id"] is not None:
            lid = r["l_loan_id"]
            loans.setdefault(lid, _pick(r, "l", LOAN))
            if r["lp_payment_id"] is not None:
                pays[lid].setdefault(r["lp_payment_id"], _pick(r, "lp", PAY))
    return {"customer": customer, "addresses": list(addrs.values()), "beneficiaries": list(bens.values()),
            "accounts": [{**a, "cards": list(cards[i].values())} for i, a in accs.items()],
            "loans": [{**l, "payments": list(pays[i].values())} for i, l in loans.items()]}


# -- B: one query per table -------------------------------------------------------
def pg_b(pg: psycopg.Connection, cid: int) -> dict:
    with pg.cursor(row_factory=dict_row) as cur:
        customer = cur.execute("SELECT * FROM customers WHERE customer_id = %s", (cid,)).fetchone()
        if customer is None:
            return None
        addrs = cur.execute(f"SELECT {', '.join(ADDR)} FROM addresses WHERE customer_id = %s", (cid,)).fetchall()
        bens = cur.execute(f"SELECT {', '.join(BEN)} FROM beneficiaries WHERE customer_id = %s", (cid,)).fetchall()
        accs = cur.execute(
            f"SELECT {_cols('ac', ACC, 'ac')}, h.role AS h_role, {_cols('at', TYPE, 'at')}, {_cols('br', BR, 'br')} "
            "FROM account_holders h JOIN accounts ac ON ac.account_id = h.account_id "
            "JOIN account_types at ON at.type_id = ac.type_id JOIN branches br ON br.branch_id = ac.branch_id "
            "WHERE h.customer_id = %s", (cid,)).fetchall()
        ids = [a["ac_account_id"] for a in accs]
        cards = cur.execute(f"SELECT account_id, {', '.join(CARD)} FROM cards WHERE account_id = ANY(%s)",
                            (ids,)).fetchall()
        loans = cur.execute(f"SELECT {', '.join(LOAN)} FROM loans WHERE customer_id = %s", (cid,)).fetchall()
        pays = cur.execute(f"SELECT loan_id, {', '.join(PAY)} FROM loan_payments WHERE loan_id = ANY(%s)",
                           ([l["loan_id"] for l in loans],)).fetchall()
    cards_by, pays_by = defaultdict(list), defaultdict(list)
    for c in cards:
        cards_by[c.pop("account_id")].append(c)
    for p in pays:
        pays_by[p.pop("loan_id")].append(p)
    return {"customer": customer, "addresses": addrs, "beneficiaries": bens,
            "accounts": [{**_pick(a, "ac", ACC), "role": a["h_role"], "type": _pick(a, "at", TYPE),
                          "branch": _pick(a, "br", BR), "cards": cards_by[a["ac_account_id"]]} for a in accs],
            "loans": [{**l, "payments": pays_by[l["loan_id"]]} for l in loans]}


# -- C: nested json_agg -------------------------------------------------------------
def _obj(alias, cols):
    return "json_build_object(" + ", ".join(f"'{c}', {alias}.{c}" for c in cols) + ")"


SQL_C = f"""
SELECT json_build_object(
  'customer', to_json(c),
  'addresses', (SELECT COALESCE(json_agg({_obj('a', ADDR)}), '[]') FROM addresses a WHERE a.customer_id = c.customer_id),
  'beneficiaries', (SELECT COALESCE(json_agg({_obj('b', BEN)}), '[]') FROM beneficiaries b WHERE b.customer_id = c.customer_id),
  'accounts', (SELECT COALESCE(json_agg(
      ({_obj('ac', ACC)}::jsonb || jsonb_build_object(
          'role', h.role, 'type', {_obj('at', TYPE)}, 'branch', {_obj('br', BR)},
          'cards', (SELECT COALESCE(json_agg({_obj('cd', CARD)}), '[]') FROM cards cd WHERE cd.account_id = ac.account_id)))
      ), '[]')
    FROM account_holders h JOIN accounts ac ON ac.account_id = h.account_id
    JOIN account_types at ON at.type_id = ac.type_id JOIN branches br ON br.branch_id = ac.branch_id
    WHERE h.customer_id = c.customer_id),
  'loans', (SELECT COALESCE(json_agg(
      ({_obj('l', LOAN)}::jsonb || jsonb_build_object(
          'payments', (SELECT COALESCE(json_agg({_obj('lp', PAY)}), '[]') FROM loan_payments lp WHERE lp.loan_id = l.loan_id)))
      ), '[]') FROM loans l WHERE l.customer_id = c.customer_id))
FROM customers c WHERE c.customer_id = %s
"""


def pg_c(pg: psycopg.Connection, cid: int) -> dict:
    row = pg.execute(SQL_C, (cid,)).fetchone()
    return row[0] if row else None


# -- MongoDB: 3 indexed reads, no joins -------------------------------------------------
def mongo(db: Database, cid: int) -> dict:
    c = db.customers.find_one({"_id": cid})
    if c is None:
        return None
    accounts = list(db.accounts.find({"holders.customer_id": cid}))
    loans = list(db.loans.find({"customer_id": cid}))
    return {"customer": c, "addresses": c.get("addresses", []), "beneficiaries": c.get("beneficiaries", []),
            "accounts": accounts, "loans": loans}


VARIANTS = {
    "PostgreSQL A: one 10-table join": ("pg", pg_a),
    "PostgreSQL B: 7 queries": ("pg", pg_b),
    "PostgreSQL C: nested json_agg": ("pg", pg_c),
    "MongoDB: 3 reads": ("mongo", mongo),
}


# -- equivalence ---------------------------------------------------------------------------
def _dec(v) -> str:
    if hasattr(v, "to_decimal"):
        v = v.to_decimal()
    return str(Decimal(str(v)).quantize(Decimal("0.01")))


def summary(p: dict | None):
    """Comparable fingerprint: every id, plus balances / codes / payment amounts."""
    if p is None:
        return None
    acc_id = lambda a: a.get("account_id", a.get("_id"))
    loan_id = lambda l: l.get("loan_id", l.get("_id"))
    return {
        "addresses": sorted(a["address_id"] for a in p["addresses"]),
        "beneficiaries": sorted(b["beneficiary_id"] for b in p["beneficiaries"]),
        "accounts": sorted((acc_id(a), _dec(a["balance"]), a["type"]["code"], a["branch"]["code"],
                            sorted(c["card_id"] for c in a["cards"])) for a in p["accounts"]),
        "loans": sorted((loan_id(l), _dec(l["principal"]),
                         sorted((x["payment_id"], _dec(x["amount"])) for x in l["payments"])) for l in p["loans"]),
    }


def check_equivalence(pg, db, ids: list[int]) -> list[int]:
    bad = []
    for cid in ids:
        sums = [summary(fn(pg if kind == "pg" else db, cid)) for kind, fn in VARIANTS.values()]
        if any(s != sums[0] for s in sums[1:]):
            bad.append(cid)
    return bad


# -- runner ------------------------------------------------------------------------------------
def _pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


def run(pg: psycopg.Connection, db: Database, runs=200, warmup=20, reps=3, out_dir="results",
        seed=11, label="localhost") -> list[dict]:
    # money must stay exact through JSON too (default json parsing would give floats)
    set_json_loads(partial(json.loads, parse_float=Decimal), pg)
    max_c = pg.execute("SELECT MAX(customer_id) FROM customers").fetchone()[0]
    rng = random.Random(seed)
    per_rep: dict[str, list[dict]] = defaultdict(list)
    raw = []
    for rep in range(1, reps + 1):
        ids = [rng.randint(1, max_c) for _ in range(runs + warmup)]
        for name, (kind, fn) in VARIANTS.items():
            conn = pg if kind == "pg" else db
            times = []
            for i, cid in enumerate(ids):
                t0 = time.perf_counter()
                fn(conn, cid)
                ms = (time.perf_counter() - t0) * 1000
                if i >= warmup:
                    times.append(ms)
                    raw.append((label, rep, name, cid, round(ms, 4)))
            per_rep[name].append({"p50": statistics.median(times), "p95": _pct(times, 0.95),
                                  "mean": statistics.fmean(times)})
    out = []
    for name, reps_ in per_rep.items():
        med = sorted(reps_, key=lambda s: s["p50"])[len(reps_) // 2]
        out.append({"label": label, "variant": name, **{k: round(v, 3) for k, v in med.items()}})
    mongo_p50 = next(r["p50"] for r in out if r["variant"].startswith("MongoDB"))
    for r in out:
        r["mongo_vs_this_p50_pct"] = None if r["variant"].startswith("MongoDB") else \
            round((r["p50"] - mongo_p50) / r["p50"] * 100, 1)
    Path(out_dir).mkdir(exist_ok=True)
    raw_path = Path(out_dir) / "bench_profile_raw.csv"
    new = not raw_path.exists()
    with open(raw_path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["label", "rep", "variant", "customer_id", "ms"])
        w.writerows(raw)
    return out


def markdown(rows: list[dict]) -> str:
    lines = [f"**Q7 full customer profile ({rows[0]['label']})**", "",
             "| Variant | p50 (ms) | p95 (ms) | MongoDB vs this (p50) |", "|---|---|---|---|"]
    for r in rows:
        ch = r["mongo_vs_this_p50_pct"]
        vs = "n/a" if ch is None else (f"{ch:.0f}% faster" if ch > 0 else f"{-ch:.0f}% slower")
        lines.append(f"| {r['variant']} | {r['p50']:.2f} | {r['p95']:.2f} | {vs} |")
    return "\n".join(lines)
