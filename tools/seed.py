"""Seed the bank schema with reproducible fake data (fixed seed, en_IN locale).

Loads with COPY into staging temp tables, then INSERT ... OVERRIDING SYSTEM VALUE
so we control primary keys (needed because ids are GENERATED ALWAYS) and can
build children that point at known parent ids.

Run BEFORE registering the Debezium connector, or every seeded row becomes a
Kafka event.

    python tools/seed.py --transactions 150000
"""
from __future__ import annotations

import random
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import psycopg
import typer
from faker import Faker
from psycopg import sql

from migrator.config import get_settings

SEED = 42
NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)  # fixed "now" so reruns are identical

# Insert order respects foreign keys
TABLES = [
    ("branches", "branch_id"),
    ("employees", "employee_id"),
    ("customers", "customer_id"),
    ("addresses", "address_id"),
    ("account_types", "type_id"),
    ("accounts", "account_id"),
    ("account_holders", None),
    ("cards", "card_id"),
    ("transactions", "txn_id"),
    ("beneficiaries", "beneficiary_id"),
    ("loans", "loan_id"),
    ("loan_payments", "payment_id"),
]

CITIES = [
    ("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"), ("Delhi", "Delhi"),
    ("Bengaluru", "Karnataka"), ("Chennai", "Tamil Nadu"), ("Hyderabad", "Telangana"),
    ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"), ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"),
]
TXN_DESCRIPTIONS = ["UPI transfer", "ATM withdrawal", "Salary", "NEFT transfer",
                    "Card payment", "Bill payment", "IMPS transfer", "Interest credit",
                    "$500 refund", None]


def money(x: float) -> Decimal:
    return Decimal(str(round(x, 2))).quantize(Decimal("0.01"))


def rand_ts(rng: random.Random, start: datetime, end: datetime) -> datetime:
    span = (end - start).total_seconds()
    # keep microseconds on purpose: exercises the ms truncation in the transformer
    return start + timedelta(seconds=rng.random() * span)


def generate(n_customers: int, n_txns: int) -> dict[str, list[tuple]]:
    rng = random.Random(SEED)
    Faker.seed(SEED)
    fake = Faker("en_IN")
    data: dict[str, list[tuple]] = {t: [] for t, _ in TABLES}

    # branches (50)
    for b in range(1, 51):
        city, _ = CITIES[(b - 1) % len(CITIES)]
        data["branches"].append((
            b, f"{city[:3].upper()}{b:02d}", f"{city} {fake.street_name()}", city,
            f"BANK0{b:06d}", date(2000, 1, 1) + timedelta(days=rng.randint(0, 8000))))

    # employees (~12 per branch)
    eid = 0
    for b in range(1, 51):
        for i in range(rng.randint(8, 16)):
            eid += 1
            role = "manager" if i == 0 else rng.choice(["officer", "teller", "teller"])
            data["employees"].append((
                eid, b, fake.name(), role, f"emp{eid}@bank.example",
                date(2010, 1, 1) + timedelta(days=rng.randint(0, 5000))))

    # customers, addresses, beneficiaries
    aid = bid = 0
    for c in range(1, n_customers + 1):
        name = fake.name()
        slug = "".join(ch for ch in name.lower().replace(" ", ".") if ch.isalnum() or ch == ".")
        data["customers"].append((
            c, name, f"{slug}.{c}@example.com", f"+91{rng.randint(6000000000, 9999999999)}",
            date(1950, 1, 1) + timedelta(days=rng.randint(0, 20000)),
            rng.choices(["verified", "pending", "rejected"], [85, 12, 3])[0],
            rand_ts(rng, datetime(2018, 1, 1, tzinfo=timezone.utc), NOW)))
        for t in (["home"] if rng.random() < 0.5 else ["home", "work"]):
            aid += 1
            city, state = rng.choice(CITIES)
            data["addresses"].append((
                aid, c, t, fake.street_address()[:200], city, state, f"{rng.randint(110000, 859999)}"))
        for _ in range(rng.choice([0, 1, 1, 2, 2, 3, 3, 4, 5, 8])):
            bid += 1
            data["beneficiaries"].append((
                bid, c, fake.name(), f"{rng.randint(10**11, 10**12 - 1)}",
                f"BANK0{rng.randint(1, 999999):06d}",
                rand_ts(rng, datetime(2020, 1, 1, tzinfo=timezone.utc), NOW)))

    # account types (lookup)
    data["account_types"] = [
        (1, "SAV", "Savings", Decimal("3.50"), Decimal("1000.00")),
        (2, "CUR", "Current", Decimal("0.00"), Decimal("10000.00")),
        (3, "FD", "Fixed Deposit", Decimal("7.10"), Decimal("5000.00")),
        (4, "SAL", "Salary", Decimal("3.00"), Decimal("0.00")),
    ]

    # accounts (~1.4 per customer), holders (~15% joint), cards
    acc_id = card_id = 0
    opening: dict[int, tuple[Decimal, datetime]] = {}
    for c in range(1, n_customers + 1):
        n_acc = rng.choices([1, 2, 3], [65, 30, 5])[0]
        for _ in range(n_acc):
            acc_id += 1
            opened = rand_ts(rng, datetime(2019, 1, 1, tzinfo=timezone.utc), datetime(2025, 6, 1, tzinfo=timezone.utc))
            start_bal = money(rng.uniform(1000, 200000))
            opening[acc_id] = (start_bal, opened)
            data["accounts"].append([
                acc_id, f"{100000000000 + acc_id}", rng.choices([1, 2, 3, 4], [60, 15, 10, 15])[0],
                rng.randint(1, 50), start_bal,
                rng.choices(["active", "frozen", "closed"], [94, 3, 3])[0], opened])
            data["account_holders"].append((acc_id, c, "primary"))
            if rng.random() < 0.15:
                other = rng.randint(1, n_customers)
                if other != c:
                    data["account_holders"].append((acc_id, other, "joint"))
            for _ in range(rng.choices([0, 1, 2, 3], [10, 60, 25, 5])[0]):
                card_id += 1
                data["cards"].append((
                    card_id, acc_id, f"{rng.randint(0, 9999):04d}",
                    rng.choice(["visa", "mastercard", "rupay"]),
                    date(2026, 1, 1) + timedelta(days=rng.randint(0, 1800)),
                    rng.choices(["active", "blocked"], [92, 8])[0]))

    # transactions: power-law distribution over accounts, consistent balance_after
    n_accounts = acc_id
    weights = [rng.paretovariate(1.2) for _ in range(n_accounts)]
    picks = rng.choices(range(1, n_accounts + 1), weights=weights, k=n_txns)
    per_acc: dict[int, int] = {}
    for a in picks:
        per_acc[a] = per_acc.get(a, 0) + 1
    txn_id = 0
    acc_rows = {row[0]: row for row in data["accounts"]}
    txns_unsorted = []
    for a, n in per_acc.items():
        bal, opened = opening[a]
        times = sorted(rand_ts(rng, opened, NOW) for _ in range(n))
        for ts in times:
            amt = money(rng.uniform(50, 25000) if rng.random() < 0.9 else rng.uniform(25000, 150000))
            if rng.random() < 0.55 and bal - amt >= 0:
                kind, bal = "debit", bal - amt
            else:
                kind, bal = "credit", bal + amt
            txns_unsorted.append((ts, a, kind, amt, bal, rng.choice(TXN_DESCRIPTIONS),
                                  f"{rng.randint(10**11, 10**12 - 1)}" if rng.random() < 0.7 else None))
        acc_rows[a][4] = bal  # account balance = last balance_after
    txns_unsorted.sort(key=lambda t: t[0])  # ids increase with time, like a real system
    for ts, a, kind, amt, bal, desc, cp in txns_unsorted:
        txn_id += 1
        data["transactions"].append((txn_id, a, kind, amt, bal, desc, cp, ts))
    data["accounts"] = [tuple(r) for r in data["accounts"]]

    # loans + payment schedules (bounded by tenure)
    pay_id = 0
    for l in range(1, 3001):
        tenure = rng.choice([12, 24, 36, 60, 120, 240, 360])
        principal = money(rng.uniform(50000, 5000000))
        rate = Decimal(str(round(rng.uniform(7.5, 14.0), 2)))
        disbursed = date(2023, 1, 1) + timedelta(days=rng.randint(0, 1300))
        months_elapsed = max(1, (NOW.date() - disbursed).days // 30)
        n_pay = min(tenure, months_elapsed + 1)
        status = rng.choices(["active", "closed", "defaulted"], [85, 10, 5])[0]
        data["loans"].append((l, rng.randint(1, n_customers), rng.randint(1, 50),
                              principal, rate, tenure, status, disbursed))
        r = rate / Decimal(1200)
        emi = money(float(principal * r * (1 + r) ** tenure / ((1 + r) ** tenure - 1)))
        for k in range(1, n_pay + 1):
            pay_id += 1
            due = disbursed + timedelta(days=30 * k)
            if due > NOW.date():
                paid_on, st = None, "due"
            elif rng.random() < 0.9:
                paid_on, st = due - timedelta(days=rng.randint(0, 5)), "paid"
            else:
                paid_on, st = due + timedelta(days=rng.randint(1, 40)), "late"
            data["loan_payments"].append((pay_id, l, due, paid_on, emi, st))

    return data


def load(conn: psycopg.Connection, data: dict[str, list[tuple]]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("TRUNCATE " + ", ".join(t for t, _ in TABLES) + " RESTART IDENTITY CASCADE")
        for table, pk in TABLES:
            rows = data[table]
            stage = f"stage_{table}"
            cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP")
                        .format(sql.Identifier(stage), sql.Identifier(table)))
            t0 = time.perf_counter()
            with cur.copy(sql.SQL("COPY {} FROM STDIN").format(sql.Identifier(stage))) as cp:
                for row in rows:
                    cp.write_row(row)
            override = sql.SQL("OVERRIDING SYSTEM VALUE ") if pk else sql.SQL("")
            cur.execute(sql.SQL("INSERT INTO {} {}SELECT * FROM {}")
                        .format(sql.Identifier(table), override, sql.Identifier(stage)))
            if pk:  # move the identity sequence past the ids we inserted
                cur.execute(sql.SQL(
                    "SELECT setval(pg_get_serial_sequence({}, {}), (SELECT COALESCE(MAX({}), 0) + 1 FROM {}), false)"
                ).format(sql.Literal(table), sql.Literal(pk), sql.Identifier(pk), sql.Identifier(table)))
            print(f"  {table:<16} {len(rows):>8,} rows  ({time.perf_counter() - t0:.1f}s)")
    with conn.cursor() as cur:
        cur.execute("ANALYZE")


def main(customers: int = typer.Option(10_000, help="number of customers"),
         transactions: int = typer.Option(150_000, help="number of transactions")):
    t0 = time.perf_counter()
    data = generate(customers, transactions)
    print(f"generated in {time.perf_counter() - t0:.1f}s; loading...")
    with psycopg.connect(get_settings().pg_dsn) as conn:
        load(conn, data)
    total = sum(len(v) for v in data.values())
    print(f"done: {total:,} rows in {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    typer.run(main)
