import argparse
import os
import time

import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
# Kafka Connect stores source offsets per connector name: registering under a
# new name forces a fresh initial snapshot of all tables (full resync).
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "banking-cdc-connector")
SLOT_NAME = "banking_slot"

parser = argparse.ArgumentParser(description="Create or update the Debezium Postgres connector")
parser.add_argument(
    "--replace",
    action="store_true",
    help="Delete any OTHER connector that uses the same replication slot first "
    "(two connectors cannot share one slot).",
)
args = parser.parse_args()

# -----------------------------
# Connector config
# -----------------------------
connector_config = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": os.getenv("POSTGRES_HOST"),
    "database.port": os.getenv("POSTGRES_PORT"),
    "database.user": os.getenv("POSTGRES_USER"),
    "database.password": os.getenv("POSTGRES_PASSWORD"),
    "database.dbname": os.getenv("POSTGRES_DB"),
    "topic.prefix": "banking_server",
    "table.include.list": "public.customers,public.accounts,public.transactions",
    "plugin.name": "pgoutput",
    "slot.name": SLOT_NAME,
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "filtered",
    "tombstones.on.delete": "false",
    # Money as exact decimal strings (e.g. "123.45"), cast to NUMBER(18,2) in dbt.
    # The default ("precise") sends base64-encoded bytes in JSON, which
    # Snowflake cannot cast; "double" would introduce floating-point rounding.
    "decimal.handling.mode": "string",
}

# -----------------------------
# Guard: one replication slot = one connector
# -----------------------------
for name in requests.get(f"{CONNECT_URL}/connectors", timeout=30).json():
    if name == CONNECTOR_NAME:
        continue
    other = requests.get(f"{CONNECT_URL}/connectors/{name}/config", timeout=30).json()
    if other.get("slot.name") != SLOT_NAME:
        continue
    if not args.replace:
        print(
            f"❌ Connector '{name}' already uses replication slot '{SLOT_NAME}'.\n"
            f"   Re-run with --replace to delete it and register '{CONNECTOR_NAME}' "
            "(this triggers a full re-snapshot of all tables)."
        )
        raise SystemExit(1)
    requests.delete(f"{CONNECT_URL}/connectors/{name}", timeout=30).raise_for_status()
    print(f"🗑️  Deleted old connector '{name}' (replication slot kept).")
    time.sleep(5)  # let the old task release the slot

# -----------------------------
# Create or update (idempotent)
# -----------------------------
# PUT /connectors/{name}/config creates the connector if missing,
# otherwise updates its config in place (keeps stored offsets).
url = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config"
response = requests.put(url, json=connector_config, timeout=30)

if response.status_code == 201:
    print(f"✅ Connector '{CONNECTOR_NAME}' created successfully!")
elif response.status_code == 200:
    print(f"✅ Connector '{CONNECTOR_NAME}' config updated.")
else:
    print(f"❌ Failed to create/update connector ({response.status_code}): {response.text}")
    raise SystemExit(1)

# -----------------------------
# Wait for RUNNING
# -----------------------------
for _ in range(20):
    status = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=30).json()
    tasks = status.get("tasks", [])
    states = [status.get("connector", {}).get("state")] + [t.get("state") for t in tasks]
    if tasks and all(s == "RUNNING" for s in states):
        print(f"✅ Connector and task are RUNNING: {status}")
        break
    if "FAILED" in states:
        print(f"❌ Connector FAILED: {status}")
        raise SystemExit(1)
    time.sleep(3)
else:
    print(f"⚠️  Not RUNNING yet, check again: curl {CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
