"""Target indexes, each tied to a query (design doc 7.7).

Unique indexes go in BEFORE the sync consumer starts (a bad event can't silently
create duplicates); the rest AFTER the bulk copy (one build beats 270K updates).
Unique indexes are partial on real rows, so CDC stubs (no _has_row) can't collide on null.
"""
from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.database import Database

REAL = {"_has_row": True}

UNIQUE = {
    "customers": [IndexModel([("email", ASCENDING)], unique=True, partialFilterExpression=REAL, name="email_u")],
    "accounts": [IndexModel([("account_no", ASCENDING)], unique=True, partialFilterExpression=REAL,
                            name="account_no_u")],
    "branches": [IndexModel([("employees.email", ASCENDING)], unique=True, name="employees_email_u",
                            partialFilterExpression={"employees.email": {"$exists": True}})],
}

SECONDARY = {
    "customers": [IndexModel([("account_ids", ASCENDING)], name="account_ids")],
    "accounts": [
        IndexModel([("holders.customer_id", ASCENDING)], name="holders_customer"),    # Q1
        IndexModel([("branch.branch_id", ASCENDING)], name="branch_id"),              # Q4 + fan-out
        IndexModel([("type.type_id", ASCENDING)], name="type_id"),                    # fan-out
        IndexModel([("cards.card_id", ASCENDING)], name="card_id"),                   # card lookups
    ],
    # Q2 (ESR) and the profile's "last 10 per account"; _id breaks created_at ties deterministically
    "transactions": [IndexModel([("account_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
                                name="account_created_id")],
    # which profiles does a change touch? (CDC read-model maintenance)
    "customer_profiles": [
        IndexModel([("accounts.account_id", ASCENDING)], name="account_id"),
        IndexModel([("loans.loan_id", ASCENDING)], name="loan_id"),
        IndexModel([("accounts.branch.branch_id", ASCENDING)], name="branch_id"),
        IndexModel([("accounts.type.type_id", ASCENDING)], name="type_id"),
    ],
    "loans": [IndexModel([("customer_id", ASCENDING)], name="customer_id"),
              IndexModel([("payments.payment_id", ASCENDING)], name="payment_id")],
}


def ensure_indexes(db: Database, unique: bool = True, secondary: bool = True) -> list[str]:
    created = []
    if secondary and "account_created" in db.transactions.index_information():
        db.transactions.drop_index("account_created")   # superseded by account_created_id
    for group, enabled in ((UNIQUE, unique), (SECONDARY, secondary)):
        if not enabled:
            continue
        for coll, models in group.items():
            if models:
                created += [f"{coll}.{n}" for n in db[coll].create_indexes(models)]
    return created
