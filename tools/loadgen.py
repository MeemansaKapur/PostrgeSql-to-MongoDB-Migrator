"""The "live application": keeps writing to PostgreSQL during the migration.

Mix: 80% transfers, 8% card status, 5% new beneficiary, 4% address update,
2% new customer+account, 1% beneficiary delete. Plus a canary write every 100 ms.

    python tools/loadgen.py --rate 300 --seconds 300
"""
from __future__ import annotations

import random
import signal
import threading
import time
from decimal import Decimal

import psycopg
import typer

from migrator.config import get_settings

stop = threading.Event()


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ops = 0
        self.skipped = 0   # no-op by design: insufficient balance, account without a card...
        self.errors = 0    # database errors
        self.by_kind: dict[str, int] = {}

    def add(self, kind: str, ok: bool = True, error: bool = False):
        with self.lock:
            if error:
                self.errors += 1
            elif not ok:
                self.skipped += 1
            else:
                self.ops += 1
                self.by_kind[kind] = self.by_kind.get(kind, 0) + 1


def transfer(cur, rng: random.Random, max_acc: int):
    a, b = rng.sample(range(1, max_acc + 1), 2)
    amount = Decimal(rng.randint(100, 500000)) / 100
    # Lock both rows in ascending id order: A->B and B->A can't deadlock.
    cur.execute("SELECT account_id, balance, account_no FROM accounts WHERE account_id = ANY(%s) "
                "ORDER BY account_id FOR UPDATE", ([a, b],))
    rows = {r[0]: r for r in cur.fetchall()}
    if len(rows) < 2 or rows[a][1] < amount:
        return False
    src_bal, dst_bal = rows[a][1] - amount, rows[b][1] + amount
    cur.execute("UPDATE accounts SET balance = %s WHERE account_id = %s", (src_bal, a))
    cur.execute("UPDATE accounts SET balance = %s WHERE account_id = %s", (dst_bal, b))
    cur.execute("INSERT INTO transactions (account_id,type,amount,balance_after,description,counterparty) "
                "VALUES (%s,'debit',%s,%s,'UPI transfer',%s), (%s,'credit',%s,%s,'UPI transfer',%s)",
                (a, amount, src_bal, rows[b][2], b, amount, dst_bal, rows[a][2]))
    return True


def card_status(cur, rng, max_acc):
    cur.execute("UPDATE cards SET status = CASE status WHEN 'active' THEN 'blocked' ELSE 'active' END "
                "WHERE card_id = (SELECT card_id FROM cards WHERE account_id = %s LIMIT 1)",
                (rng.randint(1, max_acc),))
    return cur.rowcount > 0


def new_beneficiary(cur, rng, max_cust):
    cur.execute("INSERT INTO beneficiaries (customer_id,name,account_no,ifsc) VALUES (%s,%s,%s,%s)",
                (rng.randint(1, max_cust), f"Payee {rng.randint(1, 99999)}",
                 f"{rng.randint(10**11, 10**12 - 1)}", f"BANK0{rng.randint(1, 999999):06d}"))
    return True


def address_update(cur, rng, max_cust):
    cur.execute("UPDATE addresses SET line1 = %s WHERE address_id = "
                "(SELECT address_id FROM addresses WHERE customer_id = %s LIMIT 1)",
                (f"{rng.randint(1, 999)} New Road", rng.randint(1, max_cust)))
    return cur.rowcount > 0


def new_customer(cur, rng, tag):
    cur.execute("INSERT INTO customers (name,email,phone,dob) VALUES (%s,%s,%s,'1995-05-05') RETURNING customer_id",
                (f"New Customer {tag}", f"new.{tag}@example.com", f"+91{rng.randint(6000000000, 9999999999)}"))
    cid = cur.fetchone()[0]
    cur.execute("INSERT INTO addresses (customer_id,type,line1,city,state,pincode) "
                "VALUES (%s,'home','1 Fresh Lane','Pune','Maharashtra','411001')", (cid,))
    cur.execute("INSERT INTO accounts (account_no,type_id,branch_id,balance) VALUES "
                "(%s,1,%s,1000) RETURNING account_id", (f"5{tag:011d}"[-12:], rng.randint(1, 50)))
    aid = cur.fetchone()[0]
    cur.execute("INSERT INTO account_holders VALUES (%s,%s,'primary')", (aid, cid))
    return True


def delete_beneficiary(cur, rng, max_cust):
    cur.execute("DELETE FROM beneficiaries WHERE beneficiary_id = "
                "(SELECT beneficiary_id FROM beneficiaries WHERE customer_id = %s LIMIT 1)",
                (rng.randint(1, max_cust),))
    return cur.rowcount > 0


KINDS = ["transfer", "card", "beneficiary", "address", "customer", "beneficiary_delete"]
WEIGHTS = [80, 8, 5, 4, 2, 1]


def worker(i: int, dsn: str, rate: float, stats: Stats, max_acc: int, max_cust: int):
    rng = random.Random(1000 + i)
    interval = 1.0 / rate
    next_tick = time.perf_counter()
    seq = 0
    with psycopg.connect(dsn) as conn:
        while not stop.is_set():
            kind = rng.choices(KINDS, WEIGHTS)[0]
            try:
                with conn.transaction(), conn.cursor() as cur:
                    if kind == "transfer":
                        ok = transfer(cur, rng, max_acc)
                    elif kind == "card":
                        ok = card_status(cur, rng, max_acc)
                    elif kind == "beneficiary":
                        ok = new_beneficiary(cur, rng, max_cust)
                    elif kind == "address":
                        ok = address_update(cur, rng, max_cust)
                    elif kind == "customer":
                        seq += 1
                        ok = new_customer(cur, rng, int(time.time() * 1000) % 10**8 * 100 + i * 10 + seq % 10)
                    else:
                        ok = delete_beneficiary(cur, rng, max_cust)
                stats.add(kind, ok)
            except psycopg.Error:
                stats.add(kind, False, error=True)
            # steady rate, not bursty: sleep until the next tick
            next_tick += interval
            time.sleep(max(0.0, next_tick - time.perf_counter()))


def canary(dsn: str, every_s: float = 0.1):
    with psycopg.connect(dsn, autocommit=True) as conn:
        while not stop.is_set():
            conn.execute("UPDATE canary SET written_ms = %s WHERE id = 1", (int(time.time() * 1000),))
            time.sleep(every_s)


def main(rate: float = typer.Option(300, help="target operations per second"),
         seconds: float = typer.Option(0, help="run time, 0 = until Ctrl-C"),
         workers: int = typer.Option(4, help="concurrent connections")):
    dsn = get_settings().pg_dsn
    with psycopg.connect(dsn) as conn:
        max_acc = conn.execute("SELECT MAX(account_id) FROM accounts").fetchone()[0]
        max_cust = conn.execute("SELECT MAX(customer_id) FROM customers").fetchone()[0]
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    stats = Stats()
    threads = [threading.Thread(target=canary, args=(dsn,), daemon=True)]
    threads += [threading.Thread(target=worker, args=(i, dsn, rate / workers, stats, max_acc, max_cust), daemon=True)
                for i in range(workers)]
    for t in threads:
        t.start()
    t0 = time.perf_counter()
    last_ops, last_t = 0, t0
    while not stop.is_set():
        time.sleep(1)
        now = time.perf_counter()
        if now - last_t >= 5:
            print(f"[loadgen] {stats.ops - last_ops:>5} ops in {now - last_t:.1f}s "
                  f"= {(stats.ops - last_ops) / (now - last_t):,.0f} ops/s  "
                  f"(skipped {stats.skipped}, errors {stats.errors})", flush=True)
            last_ops, last_t = stats.ops, now
        if seconds and now - t0 >= seconds:
            stop.set()
    for t in threads:
        t.join(timeout=5)
    el = time.perf_counter() - t0
    print(f"[loadgen] done: {stats.ops:,} ops in {el:.1f}s = {stats.ops / el:,.0f} ops/s achieved "
          f"(target {rate:g}); skipped {stats.skipped}; errors {stats.errors}; {stats.by_kind}", flush=True)


if __name__ == "__main__":
    typer.run(main)
