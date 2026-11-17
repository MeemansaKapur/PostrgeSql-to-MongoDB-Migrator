"""The same six questions, MongoDB side (documents + aggregation pipelines)."""
from __future__ import annotations

from pymongo.database import Database


def q1_customer_360(db: Database, p: dict):
    """Addresses are embedded; accounts carry cards: 2 round trips, no joins."""
    # same data as the SQL side: profile + addresses (no beneficiaries)
    customer = db.customers.find_one({"_id": p["customer_id"]}, {"beneficiaries": 0})
    accounts = list(db.accounts.find({"holders.customer_id": p["customer_id"]}))
    return customer, accounts


def q2_statement(db, p):
    return list(db.transactions.find({"account_id": p["account_id"], "created_at": {"$gte": p["since"]}})
                .sort("created_at", -1).limit(50))


def q3_loan_schedule(db, p):
    return db.loans.find_one({"_id": p["loan_id"]})


def q4_branch_deposits(db, p):
    return list(db.accounts.aggregate([
        {"$match": {"status": "active"}},
        {"$group": {"_id": "$branch.branch_id", "total": {"$sum": "$balance"}}},
        {"$sort": {"_id": 1}}]))


def q5_monthly_volume_by_type(db, p):
    """Join-shaped: $lookup per account (uses the account_id/created_at index)."""
    return list(db.accounts.aggregate([
        {"$match": {"branch.branch_id": p["branch_id"]}},
        {"$lookup": {"from": "transactions", "let": {"aid": "$_id"}, "as": "t",
                     "pipeline": [{"$match": {"$expr": {"$and": [
                         {"$eq": ["$account_id", "$$aid"]}, {"$gte": ["$created_at", p["since"]]}]}}},
                         {"$project": {"amount": 1, "created_at": 1}}]}},
        {"$unwind": "$t"},
        {"$group": {"_id": {"month": {"$dateTrunc": {"date": "$t.created_at", "unit": "month"}},
                            "code": "$type.code"},
                    "n": {"$sum": 1}, "volume": {"$sum": "$t.amount"}}},
        {"$sort": {"_id.month": 1, "_id.code": 1}}]))


def q6_top_customers(db, p):
    return list(db.accounts.aggregate([
        {"$unwind": "$holders"},
        {"$group": {"_id": "$holders.customer_id", "total": {"$sum": "$balance"}}},
        {"$sort": {"total": -1, "_id": 1}},
        {"$limit": 10}]))


QUERIES = {
    "Q1 customer 360": q1_customer_360,
    "Q2 account statement": q2_statement,
    "Q3 loan + schedule": q3_loan_schedule,
    "Q4 deposits per branch": q4_branch_deposits,
    "Q5 monthly volume by type": q5_monthly_volume_by_type,
    "Q6 top 10 customers": q6_top_customers,
}
