"""End-to-end: PostgreSQL change -> Debezium -> Kafka -> sync -> MongoDB.

Needs `docker compose up -d`, a registered connector and `migrator sync` running.
Run with:  pytest tests/integration -m integration
"""
import time

import psycopg
import pytest
from pymongo import MongoClient

from migrator.config import get_settings

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def env():
    s = get_settings()
    try:
        pg = psycopg.connect(s.pg_dsn, autocommit=True, connect_timeout=3)
        db = MongoClient(s.mongo_uri, serverSelectionTimeoutMS=2000)[s.mongo_db]
        db.command("ping")
    except Exception as e:
        pytest.skip(f"docker stack not reachable: {e}")
    yield pg, db
    pg.close()


def wait_for(fn, timeout=15.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(0.2)
    raise AssertionError(f"condition not met within {timeout}s (last={last!r})")


def one(pg, q, *p):
    with pg.cursor() as cur:
        cur.execute(q, p)
        return cur.fetchone()[0]


def test_insert_update_delete_move(env):
    pg, db = env
    tag = str(int(time.time() * 1000))[-9:]
    cid = one(pg, "INSERT INTO customers (name,email,phone,dob,kyc_status) VALUES "
                  "('E2E User', %s, '+919999999999', '2000-01-01', 'verified') RETURNING customer_id",
              f"e2e.{tag}@example.com")
    addr = one(pg, "INSERT INTO addresses (customer_id,type,line1,city,state,pincode) VALUES "
                   "(%s,'home','$500 Main St','Pune','Maharashtra','411001') RETURNING address_id", cid)
    acc1 = one(pg, "INSERT INTO accounts (account_no,type_id,branch_id,balance) VALUES (%s,1,7,100.50) "
                   "RETURNING account_id", f"9{tag}00"[:12].ljust(12, "0"))
    acc2 = one(pg, "INSERT INTO accounts (account_no,type_id,branch_id,balance) VALUES (%s,2,7,0) "
                   "RETURNING account_id", f"8{tag}00"[:12].ljust(12, "0"))
    pg.execute("INSERT INTO account_holders VALUES (%s,%s,'primary')", (acc1, cid))
    card = one(pg, "INSERT INTO cards (account_id,last4,network,expires_on) VALUES (%s,'1234','visa','2030-01-01') "
                   "RETURNING card_id", acc1)

    # insert: embedded document is assembled from 4 topics
    c = wait_for(lambda: db.customers.find_one({"_id": cid, "_has_row": True, "addresses.0": {"$exists": True},
                                                "account_ids": acc1}))
    assert c["addresses"][0]["line1"] == "$500 Main St"        # $literal protected it
    a = wait_for(lambda: db.accounts.find_one({"_id": acc1, "cards.card_id": card, "holders.customer_id": cid}))
    assert a["type"]["code"] == "SAV" and str(a["balance"]) == "100.50"

    # update the address
    pg.execute("UPDATE addresses SET city='Mumbai' WHERE address_id=%s", (addr,))
    wait_for(lambda: db.customers.find_one({"_id": cid, "addresses": {"$elemMatch": {"address_id": addr, "city": "Mumbai"}}}))
    assert len(db.customers.find_one({"_id": cid})["addresses"]) == 1   # replaced, not duplicated

    # move the card to another account
    pg.execute("UPDATE cards SET account_id=%s WHERE card_id=%s", (acc2, card))
    wait_for(lambda: db.accounts.find_one({"_id": acc2, "cards.card_id": card}))
    wait_for(lambda: db.accounts.find_one({"_id": acc1, "cards.card_id": {"$ne": card}}))

    # delete the card
    pg.execute("DELETE FROM cards WHERE card_id=%s", (card,))
    wait_for(lambda: db.accounts.find_one({"_id": acc2, "cards.card_id": {"$ne": card}}))

    # delete the customer (cascades address + holder rows)
    pg.execute("DELETE FROM customers WHERE customer_id=%s", (cid,))
    wait_for(lambda: db.customers.find_one({"_id": cid}) is None or None)
    wait_for(lambda: db.accounts.find_one({"_id": acc1, "holders.customer_id": {"$ne": cid}}))

    # cleanup
    pg.execute("DELETE FROM accounts WHERE account_id = ANY(%s)", ([acc1, acc2],))
