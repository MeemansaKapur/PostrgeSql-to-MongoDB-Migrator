"""The six business questions, PostgreSQL side (joins). Each takes a param dict and
returns rows the way an application would consume them."""
from __future__ import annotations

import psycopg


def q1_customer_360(pg: psycopg.Connection, p: dict):
    """Profile + addresses, then accounts (via holders) + cards: 2 round trips."""
    profile = pg.execute(
        "SELECT c.*, a.address_id, a.type, a.line1, a.city, a.state, a.pincode "
        "FROM customers c LEFT JOIN addresses a ON a.customer_id = c.customer_id "
        "WHERE c.customer_id = %s", (p["customer_id"],)).fetchall()
    accounts = pg.execute(
        "SELECT ac.*, h.role, cd.card_id, cd.last4, cd.network, cd.expires_on, cd.status AS card_status "
        "FROM account_holders h JOIN accounts ac ON ac.account_id = h.account_id "
        "LEFT JOIN cards cd ON cd.account_id = ac.account_id "
        "WHERE h.customer_id = %s", (p["customer_id"],)).fetchall()
    return profile, accounts


def q2_statement(pg, p):
    return pg.execute(
        "SELECT * FROM transactions WHERE account_id = %s AND created_at >= %s "
        "ORDER BY created_at DESC LIMIT 50", (p["account_id"], p["since"])).fetchall()


def q3_loan_schedule(pg, p):
    return pg.execute(
        "SELECT l.*, lp.payment_id, lp.due_date, lp.paid_on, lp.amount, lp.status AS payment_status "
        "FROM loans l LEFT JOIN loan_payments lp ON lp.loan_id = l.loan_id "
        "WHERE l.loan_id = %s ORDER BY lp.due_date", (p["loan_id"],)).fetchall()


def q4_branch_deposits(pg, p):
    return pg.execute(
        "SELECT branch_id, SUM(balance) AS total FROM accounts WHERE status = 'active' "
        "GROUP BY branch_id ORDER BY branch_id").fetchall()


def q5_monthly_volume_by_type(pg, p):
    return pg.execute(
        "SELECT date_trunc('month', t.created_at AT TIME ZONE 'UTC') AS month, at.code, "
        "       COUNT(*) AS n, SUM(t.amount) AS volume "
        "FROM accounts a JOIN account_types at ON at.type_id = a.type_id "
        "JOIN transactions t ON t.account_id = a.account_id "
        "WHERE a.branch_id = %s AND t.created_at >= %s "
        "GROUP BY 1, 2 ORDER BY 1, 2", (p["branch_id"], p["since"])).fetchall()


def q6_top_customers(pg, p):
    return pg.execute(
        "SELECT h.customer_id, SUM(a.balance) AS total FROM account_holders h "
        "JOIN accounts a ON a.account_id = h.account_id "
        "GROUP BY h.customer_id ORDER BY total DESC, h.customer_id LIMIT 10").fetchall()


QUERIES = {
    "Q1 customer 360": q1_customer_360,
    "Q2 account statement": q2_statement,
    "Q3 loan + schedule": q3_loan_schedule,
    "Q4 deposits per branch": q4_branch_deposits,
    "Q5 monthly volume by type": q5_monthly_volume_by_type,
    "Q6 top 10 customers": q6_top_customers,
}
