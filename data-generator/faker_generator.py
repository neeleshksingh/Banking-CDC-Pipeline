from __future__ import annotations

import argparse
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

# -----------------------------
# Project configuration (safe to hardcode here)
# -----------------------------
CURRENCY = "USD"
ACCOUNTS_PER_CUSTOMER = 2
MAX_TXN_AMOUNT = Decimal("5000.00")

# Non-zero initial balances
INITIAL_BALANCE_MIN = Decimal("100.00")
INITIAL_BALANCE_MAX = Decimal("5000.00")

# Transaction mix (weights)
TXN_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
TXN_WEIGHTS = [0.40, 0.35, 0.25]

# Probability that an iteration also updates an existing customer's email
# (exercises CDC UPDATE events and the SCD2 customer snapshot)
CUSTOMER_UPDATE_PROBABILITY = 0.5

# -----------------------------
# CLI
# -----------------------------
parser = argparse.ArgumentParser(description="Run fake banking data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
parser.add_argument("--iterations", type=int, default=0, help="Stop after N iterations (0 = run forever)")
parser.add_argument("--sleep", type=float, default=2.0, help="Seconds to sleep between iterations")
parser.add_argument("--customers", type=int, default=3, help="New customers per iteration")
parser.add_argument("--transactions", type=int, default=25, help="Transactions per iteration")
parser.add_argument(
    "--backfill-days",
    type=int,
    default=0,
    help="Backfill mode: spread iterations chronologically over the last N days "
    "(uses explicit created_at, no sleep, no customer updates). Requires --iterations.",
)
args = parser.parse_args()

if args.once:
    args.iterations = 1
if args.backfill_days and not args.iterations:
    parser.error("--backfill-days requires --iterations (e.g. --iterations 200)")

# -----------------------------
# Helpers
# -----------------------------
fake = Faker()


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def random_txn_amount() -> Decimal:
    # Right-skewed: many small payments, few large ones
    val = Decimal(str(min(random.lognormvariate(4.5, 1.0), float(MAX_TXN_AMOUNT))))
    return max(val.quantize(Decimal("0.01"), rounding=ROUND_DOWN), Decimal("1.00"))


def unique_email(first_name: str, last_name: str) -> str:
    # UUID suffix avoids UNIQUE violations across generator restarts
    return f"{first_name}.{last_name}.{uuid.uuid4().hex[:8]}@{fake.free_email_domain()}".lower()


# -----------------------------
# Connect to Postgres
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
# Explicit transactions: each business event commits atomically,
# so Debezium sees the transaction row and the balance update together.
conn.autocommit = False
cur = conn.cursor()


# -----------------------------
# Entity creation
# -----------------------------
def create_customers(n: int, ts: datetime | None) -> list[int]:
    customer_ids = []
    for _ in range(n):
        first_name = fake.first_name()
        last_name = fake.last_name()
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email, created_at, updated_at) "
            "VALUES (%s, %s, %s, COALESCE(%s, now()), COALESCE(%s, now())) RETURNING id",
            (first_name, last_name, unique_email(first_name, last_name), ts, ts),
        )
        customer_ids.append(cur.fetchone()[0])
    conn.commit()
    return customer_ids


def create_accounts(customer_ids: list[int], ts: datetime | None) -> list[int]:
    account_ids = []
    for customer_id in customer_ids:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, COALESCE(%s, now()), COALESCE(%s, now())) RETURNING id",
                (
                    customer_id,
                    random.choice(["SAVINGS", "CHECKING"]),
                    random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX),
                    CURRENCY,
                    ts,
                    ts,
                ),
            )
            account_ids.append(cur.fetchone()[0])
    conn.commit()
    return account_ids


# -----------------------------
# Transactions (balance-aware)
# -----------------------------
def post_transaction(account_id: int, txn_type: str, amount: Decimal, related_account: int | None,
                     ts: datetime | None) -> str:
    """Insert one transaction and apply it to balances in a single DB transaction.

    Debits that would overdraw the account are recorded as FAILED and do not
    move money (accounts.balance has CHECK (balance >= 0)).
    """
    # Lock involved accounts in id order to avoid deadlocks
    locked = sorted({a for a in (account_id, related_account) if a is not None})
    cur.execute("SELECT id, balance FROM accounts WHERE id = ANY(%s) ORDER BY id FOR UPDATE", (locked,))
    balances = dict(cur.fetchall())

    status = "COMPLETED"
    if txn_type in ("WITHDRAWAL", "TRANSFER") and balances.get(account_id, Decimal(0)) < amount:
        status = "FAILED"

    cur.execute(
        "INSERT INTO transactions (account_id, transaction_type, amount, related_account_id, status, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, COALESCE(%s, now()), COALESCE(%s, now()))",
        (account_id, txn_type, amount, related_account, status, ts, ts),
    )

    if status == "COMPLETED":
        if txn_type == "DEPOSIT":
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, account_id))
        elif txn_type == "WITHDRAWAL":
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, account_id))
        else:  # TRANSFER
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, account_id))
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, related_account))

    conn.commit()
    return status


def update_random_customer():
    """Change one existing customer's email -> CDC UPDATE event -> new SCD2 version."""
    cur.execute("SELECT id, first_name, last_name FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return None
    customer_id, first_name, last_name = row
    cur.execute("UPDATE customers SET email = %s WHERE id = %s", (unique_email(first_name, last_name), customer_id))
    conn.commit()
    return customer_id


# -----------------------------
# Core generation logic (one iteration)
# -----------------------------
def load_existing_accounts() -> list[int]:
    cur.execute("SELECT id FROM accounts")
    ids = [r[0] for r in cur.fetchall()]
    conn.commit()
    return ids


def run_iteration(account_pool: list[int], base_ts: datetime | None):
    def ts(offset_seconds: float) -> datetime | None:
        return base_ts + timedelta(seconds=offset_seconds) if base_ts else None

    customers = create_customers(args.customers, ts(0))
    new_accounts = create_accounts(customers, ts(1))
    account_pool.extend(new_accounts)

    stats = {"COMPLETED": 0, "FAILED": 0}
    if len(account_pool) >= 2:
        for i in range(args.transactions):
            account_id = random.choice(account_pool)
            txn_type = random.choices(TXN_TYPES, weights=TXN_WEIGHTS)[0]
            related_account = None
            if txn_type == "TRANSFER":
                related_account = random.choice([a for a in account_pool if a != account_id])
            status = post_transaction(account_id, txn_type, random_txn_amount(), related_account,
                                      ts(2 + i * random.uniform(5, 120)))
            stats[status] += 1

    updated = None
    if not base_ts and random.random() < CUSTOMER_UPDATE_PROBABILITY:
        updated = update_random_customer()

    print(
        f"✅ {len(customers)} customers, {len(new_accounts)} accounts, "
        f"{stats['COMPLETED']} completed / {stats['FAILED']} failed transactions"
        + (f", updated customer {updated}" if updated else "")
        + (f" @ {base_ts:%Y-%m-%d %H:%M}" if base_ts else "")
    )


# -----------------------------
# Main loop
# -----------------------------
try:
    if args.backfill_days:
        # Chronological spread so accounts always exist before their transactions.
        # Only accounts created by this backfill are used (older ones have later created_at).
        account_pool: list[int] = []
        start = datetime.now(timezone.utc) - timedelta(days=args.backfill_days)
        step = timedelta(days=args.backfill_days) / args.iterations
        for n in range(args.iterations):
            base = start + step * n + timedelta(seconds=random.uniform(0, step.total_seconds() * 0.5))
            run_iteration(account_pool, base)
    else:
        account_pool = load_existing_accounts()
        iteration = 0
        while True:
            iteration += 1
            print(f"\n--- Iteration {iteration} started ---")
            run_iteration(account_pool, None)
            print(f"--- Iteration {iteration} finished ---")
            if args.iterations and iteration >= args.iterations:
                break
            time.sleep(args.sleep)

except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    conn.rollback()
    cur.close()
    conn.close()
    sys.exit(0)
