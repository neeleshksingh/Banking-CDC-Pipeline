# Runbook: run and test the pipeline by hand

```text
Postgres ──WAL──▶ Debezium ──▶ Kafka ──▶ consumer (host) ──▶ MinIO (Parquet)
                                                               │  every 1 min
                                                               ▼
                         Airflow: minio_to_snowflake_banking ──▶ Snowflake BANKING.RAW
                                                               │  Dataset trigger
                                                               ▼
                         Airflow: dbt_banking_build ──▶ STAGING (views) ──▶ ANALYTICS (snapshots, dims, facts, monitoring)
```

Expected end-to-end latency for a new Postgres row: roughly **2–4 minutes**
(10 s consumer flush + ≤1 min Airflow schedule + ~1–2 min `dbt build`).

All commands run from the repository root unless stated otherwise.

---

## 0. Prerequisites (once)

| What | Check |
|---|---|
| Docker Desktop running | `docker info` |
| Python venv with dependencies | `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt` |
| Env files present | `.env`, `data-generator/.env`, `kafka-debezium/.env`, `consumer/.env`, `docker/dags/.env` |
| dbt profile | `banking_dbt/.dbt/profiles.yml` (reads `SNOWFLAKE_*` env vars) |

Optional consumer settings in `consumer/.env`: `BATCH_SIZE` (default 500) and `FLUSH_SECONDS` (default 10).
Optional Airflow admin for a fresh setup in `.env`: `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` (default `admin`/`admin`).

---

## 1. Start the stack

```bash
docker compose up -d --build
docker compose ps -a
```

Wait until `postgres`, `kafka` and `connect` show **healthy**, and `airflow-init` shows **exited (0)**.
Kafka may restart once or twice with `NodeExists` right after a restart; the restart policy recovers it
within ~30 s.

UIs: Airflow http://localhost:8080 · MinIO console http://localhost:9001 · Kafka Connect http://localhost:8083

---

## 2. Apply the Postgres schema (existing database)

A fresh (empty) data directory gets the schema automatically. For your **existing** database, apply it once.
It is idempotent and safe to re-run:

```bash
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < postgres/schema.sql
```

Check:

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d transactions"'
```

You should see the `updated_at` column, the `transactions_*_check` constraints and the new indexes.

---

## 3. Register (or update) the Debezium connector

```bash
cd kafka-debezium && ../.venv/bin/python register_connector.py && cd ..
```

Expect `Connector 'banking-cdc-connector' created` (or `config updated`) and `RUNNING`.

If it stops with `Connector '...' already uses replication slot 'banking_slot'`, an older connector
(e.g. `banking-postgres-connector`) owns the slot. Replace it:

```bash
cd kafka-debezium && ../.venv/bin/python register_connector.py --replace && cd ..
```

This deletes the old connector (the slot is kept) and registers the new one, which re-snapshots every table.
dbt removes the resulting duplicates. Topics:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep banking_server
```

---

## 4. The Kafka → MinIO consumer

**In Docker (default).** `docker compose up -d --build` starts the `consumer` service once Kafka and MinIO
are healthy. It uses the in-network endpoints (`kafka:9092`, `http://minio:9000`) and takes credentials,
`KAFKA_GROUP`, `BATCH_SIZE` and `FLUSH_SECONDS` from `consumer/.env`.

```bash
docker compose logs -f consumer
```

Expect `MinIO upload successful` and `Kafka offset committed` lines every ~10 s while events flow.
Per-event lines are logged at DEBUG: set `CONSUMER_LOG_LEVEL=DEBUG` in `.env` and recreate the service
to see them. `docker compose stop consumer` sends SIGTERM: the consumer flushes and commits what it has
buffered before exiting.

**On the host (terminal 1).** Stop the container first, so the two don't share the consumer group:

```bash
docker compose stop consumer
LOG_LEVEL=INFO .venv/bin/python consumer/kafka_to_minio.py
```

It reads `consumer/.env` (`localhost:29092`, `http://localhost:9000`). `Ctrl+C` flushes and exits.

> The previous consumer never committed offsets (a kafka-python API mismatch), so on its first start this
> version re-reads everything still retained in Kafka (7 days). That's expected: duplicates are removed in
> dbt staging.

---

## 5. One-time Snowflake cleanup and first dbt build

1. In Snowsight, run [`snowflake/cleanup_legacy_objects.sql`](../snowflake/cleanup_legacy_objects.sql).
   It drops only derived objects left by older versions (marts built in `RAW`, and the old-layout
   `ANALYTICS.FACT_TRANSACTIONS`). RAW tables and SCD2 snapshots are **not** touched.
2. Run the first build with the same dbt version Airflow uses:

```bash
docker exec airflow-scheduler bash -c "cd /opt/airflow/banking_dbt && dbt build --full-refresh \
  --profiles-dir /home/airflow/.dbt --target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
```

Expect `Completed successfully`. `WARN` results on relationship tests are acceptable: they flag
late-arriving rows that resolve on the next build.

Result in Snowflake:

| Schema | Objects |
|---|---|
| `BANKING.RAW` | `CUSTOMERS`, `ACCOUNTS`, `TRANSACTIONS` (append-only CDC events + `_FILE_NAME`, `_LOADED_AT`) |
| `BANKING.STAGING` | `STG_CUSTOMERS`, `STG_ACCOUNTS`, `STG_TRANSACTIONS` (current state, CDC applied) |
| `BANKING.ANALYTICS` | `DIM_CUSTOMERS`, `DIM_ACCOUNTS`, `DIM_DATE`, `FACT_TRANSACTIONS`, `DIM_*_HISTORY`, `FCT_ACCOUNT_BALANCE_CHANGES`, `FCT_CDC_EVENTS`, `MON_PIPELINE_FRESHNESS`, `*_SNAPSHOT` |

---

## 6. Enable the Airflow DAGs

```bash
docker exec airflow-scheduler airflow dags unpause minio_to_snowflake_banking
docker exec airflow-scheduler airflow dags unpause dbt_banking_build
```

- `minio_to_snowflake_banking` runs every minute. Without new files it **skips** (grey) and does not
  connect to Snowflake, so the warehouse can auto-suspend.
- `dbt_banking_build` has no clock schedule: it runs whenever the load task lands new data (Airflow Dataset).

---

## 7. Generate data (terminal 2)

```bash
cd data-generator
../.venv/bin/python faker_generator.py --iterations 5          # 5 bursts, then stop
# or run continuously:  ../.venv/bin/python faker_generator.py
```

Each iteration inserts 3 customers, 6 accounts and 25 transactions. It updates balances in the same DB
transaction, marks overdrawing debits as `FAILED`, and sometimes changes a customer's email (a CDC UPDATE).

**History for time-series charts** (MoM / rolling windows): backfill spreads timestamps over past days:

```bash
../.venv/bin/python faker_generator.py --backfill-days 90 --iterations 300
```

---

## 8. Watch it flow

| Hop | Where to look | What you should see |
|---|---|---|
| Kafka → MinIO | terminal 1 | uploads + committed offsets |
| MinIO | http://localhost:9001 → bucket `raw` | `customers/ accounts/ transactions/` → `date=YYYY-MM-DD/*.parquet` |
| MinIO → Snowflake | Airflow → `minio_to_snowflake_banking` → `load_snowflake` log | `COPY ...: LOADED` rows |
| dbt | Airflow → `dbt_banking_build` → `dbt_build` log | `Completed successfully` |

---

## 9. Verify

Postgres (source of truth):

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select (select count(*) from customers) customers, (select count(*) from accounts) accounts, (select count(*) from transactions) transactions"'
```

Snowflake: run [`snowflake/verify_pipeline.sql`](../snowflake/verify_pipeline.sql).

- Mart counts should equal Postgres counts once the pipeline is idle (wait ~4 min after the generator stops).
- Query 2 must show `duplicate_ids = 0` and `null_types = 0`.
- `MON_PIPELINE_FRESHNESS` shows event counts per operation and the ingest latency.

---

## 10. Hands-on CDC tests

Open psql:

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Run each test, wait ~3–4 minutes, then check Snowflake.

| # | Postgres | Check in Snowflake |
|---|---|---|
| A. Insert | `INSERT INTO transactions (account_id, transaction_type, amount) VALUES ((SELECT min(id) FROM accounts), 'DEPOSIT', 123.45) RETURNING id;` | `select * from ANALYTICS.FACT_TRANSACTIONS where transaction_id = <id>;` |
| B. Customer update (SCD2) | `UPDATE customers SET email = 'scd2.test@example.com' WHERE id = 1;` | `select * from ANALYTICS.DIM_CUSTOMERS_HISTORY where customer_id = 1 order by valid_from;` → 2 versions, old one closed; `DIM_CUSTOMERS` shows the new email |
| C. Account attribute change (SCD2) | `UPDATE accounts SET account_type = CASE account_type WHEN 'SAVINGS' THEN 'CHECKING' ELSE 'SAVINGS' END WHERE id = 1;` | `select * from ANALYTICS.DIM_ACCOUNTS_HISTORY where account_id = 1 order by valid_from;` |
| D. Balance change (not SCD2) | `UPDATE accounts SET balance = balance + 10 WHERE id = 1;` | `DIM_ACCOUNTS.balance` updated, **no** new history version; new row in `FCT_ACCOUNT_BALANCE_CHANGES` |
| E. Delete | `DELETE FROM transactions WHERE id = <id from A>;` | row gone from `FACT_TRANSACTIONS`; `select * from ANALYTICS.FCT_CDC_EVENTS where source_table = 'transactions' and cdc_operation = 'd';` |

### Failure test: nothing is lost when MinIO is down

1. Run the generator continuously (step 7).
2. `docker stop minio` → terminal 1 shows `❌ Flush failed ... Rewound ... retrying`.
3. `docker start minio` → uploads resume from the rewound offsets.
4. Stop the generator, wait ~4 min, and compare counts (step 9): they match.

### Idempotency test: re-loading does not duplicate

```bash
docker exec airflow-scheduler airflow variables delete minio_to_snowflake_watermarks
docker exec airflow-scheduler airflow dags trigger minio_to_snowflake_banking
```

The `load_snowflake` log shows `LOAD_SKIPPED ... File was loaded before`, and RAW row counts don't change.

---

## 11. Full resync (if Snowflake is missing rows that Postgres has)

This forces Debezium to re-snapshot every table; dbt de-duplicates the resulting events.

```bash
cd kafka-debezium && CONNECTOR_NAME=banking-cdc-connector-v2 ../.venv/bin/python register_connector.py --replace && cd ..
```

Keep using the new name (`CONNECTOR_NAME=banking-cdc-connector-v2`) for later runs of that script.

---

## 12. Stop

1. `Ctrl+C` the generator, then the consumer (it flushes buffered events before exiting).
2. `docker compose stop` keeps containers, Kafka topics and connector offsets.
   Avoid `docker compose down` unless you want a clean Kafka; the next start will then re-snapshot.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `kafka` keeps restarting with `NodeExists` | Previous broker session still in ZooKeeper; it clears within ~20 s. If it persists: `docker compose restart zookeeper kafka` |
| Connector task `FAILED` | `curl localhost:8083/connectors/banking-cdc-connector/status` for the trace; then re-run step 3 |
| `fact_transactions was built by an older version of this model` | Run step 5 (cleanup + `--full-refresh`) |
| `load_snowflake` fails with `SignatureDoesNotMatch` | MinIO keys in `docker/dags/.env` don't match the MinIO root user |
| `Could not connect to Snowflake backend` | Network/DNS from Docker; the task retries once. Re-trigger the DAG |
| dbt test failure in `dbt_build` | Open the task log. Downstream models are skipped, so bad data never reaches the marts |
