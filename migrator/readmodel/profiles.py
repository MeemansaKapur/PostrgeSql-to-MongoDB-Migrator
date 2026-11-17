"""customer_profiles: one document per customer holding everything the profile
screen shows, gathered from all 12 source tables:

  customers + addresses + beneficiaries                       (customer)
  account_holders -> accounts + account_types + branches      (accounts, with role/type/branch)
    + employees (branch manager) + cards
    + transactions (SUBSET pattern: last 10 per account; the full history is unbounded)
  loans + loan_payments                                        (loans with schedule)

Maintenance strategy: RECOMPUTE, not patch. After each CDC batch is applied to the
normalized collections, the consumer works out which customers were affected and
rebuilds just those profiles from the (already synced) MongoDB collections.
  + idempotent and self-healing: a rebuild always converges to the current state
  + no extra load on the source PostgreSQL (reads the target only)
  - write amplification: one transfer rewrites the profiles of every holder of both accounts
"""
from __future__ import annotations

import logging
import time

from pymongo import DeleteMany, ReplaceOne
from pymongo.database import Database

log = logging.getLogger("migrator.profiles")

PROFILES = "customer_profiles"
RECENT_TXNS = 10
CUSTOMER_FIELDS = ("name", "email", "phone", "dob", "kyc_status", "created_at")


def _by(items, key):
    return sorted(items or [], key=lambda x: x[key])


def build_profiles(db: Database, customer_ids: list) -> dict:
    """customer_id -> profile document, for every id that has a real customer row."""
    ids = list(customer_ids)
    custs = {c["_id"]: c for c in db.customers.find({"_id": {"$in": ids}, "_has_row": True})}
    if not custs:
        return {}
    ids = list(custs)
    accounts = list(db.accounts.aggregate([
        {"$match": {"holders.customer_id": {"$in": ids}, "_has_row": True}},
        {"$lookup": {"from": "transactions", "localField": "_id", "foreignField": "account_id",
                     "pipeline": [{"$sort": {"created_at": -1, "_id": -1}}, {"$limit": RECENT_TXNS}],
                     "as": "recent"}},
    ]))
    branch_ids = list({a["branch"]["branch_id"] for a in accounts if a.get("branch")})
    managers = {}
    for b in db.branches.find({"_id": {"$in": branch_ids}}, {"employees": 1}):
        mgrs = sorted((e for e in b.get("employees", []) if e.get("role") == "manager"),
                      key=lambda e: e["employee_id"])
        managers[b["_id"]] = ({"employee_id": mgrs[0]["employee_id"], "name": mgrs[0]["name"],
                               "email": mgrs[0]["email"]} if mgrs else None)
    loans_by = {}
    for l in db.loans.find({"customer_id": {"$in": ids}, "_has_row": True}):
        loans_by.setdefault(l["customer_id"], []).append(l)

    accounts_by = {}
    for a in accounts:
        branch = dict(a["branch"]) if a.get("branch") else None
        if branch is not None:
            branch["manager"] = managers.get(branch["branch_id"])
        base = {
            "account_id": a["_id"], "account_no": a.get("account_no"), "balance": a.get("balance"),
            "status": a.get("status"), "opened_at": a.get("opened_at"), "type": a.get("type"),
            "branch": branch, "cards": _by(a.get("cards"), "card_id"),
            "recent_txns": [{"txn_id": t["_id"], "type": t.get("type"), "amount": t.get("amount"),
                             "balance_after": t.get("balance_after"), "description": t.get("description"),
                             "counterparty": t.get("counterparty"), "created_at": t.get("created_at")}
                            for t in a["recent"]],
        }
        for h in a.get("holders", []):
            if h["customer_id"] in custs:
                accounts_by.setdefault(h["customer_id"], []).append({**base, "role": h["role"]})

    out = {}
    for cid, c in custs.items():
        out[cid] = {
            "_id": cid, **{f: c.get(f) for f in CUSTOMER_FIELDS},
            "addresses": _by(c.get("addresses"), "address_id"),
            "beneficiaries": _by(c.get("beneficiaries"), "beneficiary_id"),
            "accounts": _by(accounts_by.get(cid), "account_id"),
            "loans": [{"loan_id": l["_id"], **{k: l.get(k) for k in (
                "branch_id", "principal", "rate", "tenure_months", "status", "disbursed_on")},
                       "payments": _by(l.get("payments"), "payment_id")}
                      for l in _by(loans_by.get(cid), "_id")],
            "_has_row": True,
        }
    return out


def refresh(db: Database, customer_ids, chunk: int = 1000) -> int:
    """Rebuild these profiles; delete profiles whose customer no longer exists."""
    ids = list(customer_ids)
    n = 0
    for i in range(0, len(ids), chunk):
        part = ids[i:i + chunk]
        docs = build_profiles(db, part)
        ops = [ReplaceOne({"_id": cid}, d, upsert=True) for cid, d in docs.items()]
        gone = [cid for cid in part if cid not in docs]
        if gone:
            ops.append(DeleteMany({"_id": {"$in": gone}}))
        if ops:
            db[PROFILES].bulk_write(ops, ordered=False)
        n += len(docs)
    return n


def rebuild_all(db: Database, chunk: int = 1000) -> int:
    t0 = time.perf_counter()
    ids = [c["_id"] for c in db.customers.find({"_has_row": True}, {"_id": 1}).sort("_id", 1)]
    n = refresh(db, ids, chunk)
    # profiles of customers that no longer exist
    db[PROFILES].delete_many({"_id": {"$nin": ids}})
    log.info("customer_profiles: %s documents in %.1fs", f"{n:,}", time.perf_counter() - t0)
    return n


def affected_customers(db: Database, events) -> set:
    """Which profiles does this batch of change events touch?"""
    cust, accts, loans, branches, types = set(), set(), set(), set(), set()
    for ev in events:
        rows = [r for r in (ev.after, ev.before) if r]
        t = ev.table
        for r in rows:
            if t in ("customers", "addresses", "beneficiaries", "account_holders"):
                cust.add(r.get("customer_id"))
            elif t in ("accounts", "cards", "transactions"):
                accts.add(r.get("account_id"))
            elif t == "loans":
                cust.add(r.get("customer_id"))
            elif t == "loan_payments":
                loans.add(r.get("loan_id"))
            elif t in ("branches", "employees"):
                branches.add(r.get("branch_id"))
            elif t == "account_types":
                types.add(r.get("type_id"))
    accts.discard(None), loans.discard(None), branches.discard(None), types.discard(None)
    if accts:
        for a in db.accounts.find({"_id": {"$in": list(accts)}}, {"holders.customer_id": 1}):
            cust.update(h["customer_id"] for h in a.get("holders", []))
        cust.update(p["_id"] for p in db[PROFILES].find({"accounts.account_id": {"$in": list(accts)}}, {"_id": 1}))
    if loans:
        cust.update(l["customer_id"] for l in db.loans.find({"_id": {"$in": list(loans)}}, {"customer_id": 1})
                    if l.get("customer_id") is not None)
        cust.update(p["_id"] for p in db[PROFILES].find({"loans.loan_id": {"$in": list(loans)}}, {"_id": 1}))
    if branches:
        cust.update(p["_id"] for p in db[PROFILES].find(
            {"accounts.branch.branch_id": {"$in": list(branches)}}, {"_id": 1}))
    if types:
        cust.update(p["_id"] for p in db[PROFILES].find({"accounts.type.type_id": {"$in": list(types)}}, {"_id": 1}))
    cust.discard(None)
    return cust


def verify_profiles(db: Database, customer_ids=None, chunk: int = 1000) -> dict:
    """Stored profile vs a fresh rebuild from the normalized collections (which are
    themselves verified against PostgreSQL)."""
    if customer_ids is None:
        ids = sorted({c["_id"] for c in db.customers.find({"_has_row": True}, {"_id": 1})}
                     | {p["_id"] for p in db[PROFILES].find({}, {"_id": 1})})
    else:
        ids = list(customer_ids)
    mismatches, checked = [], 0
    for i in range(0, len(ids), chunk):
        part = ids[i:i + chunk]
        expected = build_profiles(db, part)
        actual = {p["_id"]: p for p in db[PROFILES].find({"_id": {"$in": part}})}
        for cid in part:
            e, a = expected.get(cid), actual.get(cid)
            if e is None and a is None:
                continue
            checked += 1
            if e != a:
                fields = (["<missing>"] if a is None else ["<extra>"] if e is None
                          else sorted(k for k in set(e) | set(a) if e.get(k) != a.get(k)))
                mismatches.append({"_id": cid, "fields": fields})
    return {"checked": checked, "mismatches": mismatches}
