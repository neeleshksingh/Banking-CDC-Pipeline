# Runbook: run and test the pipeline by hand

```text
Postgres ──WAL──▶ Debezium ──▶ Kafka ──▶ consumer (Docker) ──▶ MinIO (Parquet)
                                                               │  every 1 min
                                                               ▼
                         Airflow: minio_to_snowflake_banking ──▶ Snowflake BANKING.RAW
                                                               │  Dataset trigger
                                                               ▼
                         Airflow: dbt_banking_build ──▶ STAGING (views) ──▶ ANALYTICS (snapshots, dims, facts, monitoring)
```

Expected end-to-end latency for a new Postgres row: about **1 minute**
(10 s consumer flush + 0–60 s wait for the next Airflow load + ~6 s `COPY INTO` + ~17 s `dbt build`);
a large backfill can take ~3 minutes.

All commands run from the repository root unless stated otherwise.

---

## 0. Prerequisites (once)

| What | Check |
|---|---|
| Docker Desktop running | `docker info` |
| Python 3.11 venv with dependencies (same version as CI and the consumer image; dbt 1.12 needs ≥ 3.10) | `python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt` (macOS: `brew install python@3.11`) |
| Env files present | `.env`, `data-generator/.env`, `kafka-debezium/.env`, `consumer/.env`, `docker/dags/.env` (copy each `.env.example` and fill it in) |
| Snowflake key pair | `keys/snowflake_rsa_key.p8` attached to `BANKING_PIPELINE_USER` (section 0b) |
| dbt profile | `banking_dbt/.dbt/profiles.yml`, key-pair version from [`banking_dbt/README.md`](../banking_dbt/README.md) |

Optional consumer settings in `consumer/.env`: `BATCH_SIZE` (default 500) and `FLUSH_SECONDS` (default 10).
Optional Airflow admin for a fresh setup in `.env`: `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` (default `admin`/`admin`).

Unit tests (no Docker or Snowflake needed): `.venv/bin/pip install -r requirements-dev.txt && .venv/bin/pytest`.

### 0b. Snowflake key-pair auth and least-privilege role (once)

Airflow and dbt log in as the service user `BANKING_PIPELINE_USER` with a private key and the role
`BANKING_PIPELINE_ROLE`. No password, no `ACCOUNTADMIN`.

1. In Snowsight, as `ACCOUNTADMIN`, run [`snowflake/security_setup.sql`](../snowflake/security_setup.sql)
   ("Run All"). It is idempotent. It creates the roles, the two service users and the grants.
2. Generate the key pair (unencrypted PKCS#8, written to the git-ignored `keys/`):

   ```bash
   make snowflake-keys
   ```

   Run the printed `ALTER USER BANKING_PIPELINE_USER SET RSA_PUBLIC_KEY = '...';` in Snowsight, then check
   that `describe user BANKING_PIPELINE_USER` shows an `RSA_PUBLIC_KEY_FP`.
3. **Only if `BANKING` already has tables or views** created by another role (e.g. `ACCOUNTADMIN`): review
   and run the commented `grant ownership ... copy current grants` block (section 7) in
   `security_setup.sql`. Without it, `PUT` into `@%table`, `ALTER TABLE` and dbt's `create or replace` fail
   with "Insufficient privileges". A fresh database doesn't need it.
4. In `docker/dags/.env`: set `SNOWFLAKE_USER=BANKING_PIPELINE_USER`, `SNOWFLAKE_ROLE=BANKING_PIPELINE_ROLE`,
   `SNOWFLAKE_PRIVATE_KEY_PATH=/opt/airflow/keys/snowflake_rsa_key.p8` and an empty
   `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=` (see `docker/dags/.env.example`).
5. Create `banking_dbt/.dbt/profiles.yml` from the key-pair profile in
   [`banking_dbt/README.md`](../banking_dbt/README.md).
6. Recreate the Airflow containers so they pick up the env file and the `./keys` mount (read-only at
   `/opt/airflow/keys`): `docker compose up -d --force-recreate airflow-webserver airflow-scheduler`.

Check: `docker exec airflow-scheduler ls -l /opt/airflow/keys` lists the key, and a manual run of
`minio_to_snowflake_banking` succeeds. On Linux hosts the key file must be readable by the container's
`airflow` user (uid 50000); Docker Desktop on macOS maps permissions for you.

**GitHub Actions (CD).** Create a second key for the CI user and add repository secrets:

```bash
make snowflake-keys KEY_NAME=snowflake_ci_key SF_USER=BANKING_CI_USER   # run the printed ALTER USER
```

| Secret | Value |
|---|---|
| `SNOWFLAKE_ACCOUNT` | account identifier |
| `SNOWFLAKE_CI_USER` | `BANKING_CI_USER` |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` |
| `SNOWFLAKE_PRIVATE_KEY` | full contents of `keys/snowflake_ci_key.p8`, including the `BEGIN` / `END` lines |

CI needs no secrets at all. CD runs only when `banking_dbt/` or `cd.yml` changes on `main` (or by hand from
the Actions tab).

---

## 1. Start the stack

```bash
docker compose up -d --build
docker compose ps -a
```

Wait until `postgres`, `kafka`, `connect` and `minio` show **healthy**, `consumer` is **running**, and
`airflow-init` shows **exited (0)**.
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

---

## 5. First dbt build

Run it inside the Airflow container, so it uses the same dbt version as the scheduled builds:

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
| Kafka → MinIO | `docker compose logs -f consumer` (or terminal 1) | uploads + committed offsets |
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
2. `docker stop minio` → `docker compose logs -f consumer` shows `ERROR ... Flush failed` with a traceback,
   then `Kafka offset NOT committed. Rewound ... retrying`.
3. `docker start minio` → uploads resume from the rewound offsets.
4. Stop the generator, wait ~4 min, and compare counts (step 9): they match.

### Idempotency test: re-loading does not duplicate

```bash
docker exec airflow-scheduler airflow variables delete minio_to_snowflake_watermarks
docker exec airflow-scheduler airflow dags trigger minio_to_snowflake_banking
```

The `load_snowflake` log shows `LOAD_SKIPPED ... File was loaded before`, and RAW row counts don't change.

---

## 11. Power BI report

The report runs in the Power BI service in a browser (works on macOS), in **Import** mode on
`BANKING.ANALYTICS`. Screenshots: [`docs/images/powerbi/`](images/powerbi/).

**11a. Snowflake access (once).**

1. In Snowsight, as `ACCOUNTADMIN`, run [`snowflake/powerbi_setup.sql`](../snowflake/powerbi_setup.sql)
   ("Run All"). It creates the read-only role `PBI_READER`, the auto-suspending warehouse `PBI_WH`, and the
   service user `PBI_SVC`.
2. Create a key for `PBI_SVC` and run the printed `ALTER USER` in Snowsight:

   ```bash
   make snowflake-keys KEY_NAME=snowflake_pbi_key SF_USER=PBI_SVC
   ```

   Power BI signs in with this key because scheduled refresh can't answer an MFA prompt.

**11b. Connect (app.powerbi.com).** You need a work or school account (Gmail / Outlook.com are not accepted)
with Pro or a trial.

1. Workspace → **+ New item → Semantic model → Get data → Snowflake**.
2. **Server:** `<SNOWFLAKE_ACCOUNT>.snowflakecomputing.com` (value from `docker/dags/.env`) · **Warehouse:**
   `PBI_WH` · **Advanced options → Role:** `PBI_READER`.
3. **Credentials:** no gateway · **Authentication kind:** Key pair · **Username:** `PBI_SVC` · **Private key:**
   the full contents of `keys/snowflake_pbi_key.p8` including the `BEGIN` / `END` lines
   (`pbcopy < keys/snowflake_pbi_key.p8` copies it) · passphrase empty. If the form shows **Browse files**
   instead of a text box, upload `keys/snowflake_pbi_key.p8` (in the Mac file picker, ⌘⇧G and type the
   project's `keys` folder path).
4. Under `BANKING → ANALYTICS` select `FACT_TRANSACTIONS`, `DIM_CUSTOMERS`, `DIM_ACCOUNTS`, `DIM_DATE`,
   `DIM_CUSTOMERS_HISTORY`, `FCT_CDC_EVENTS`, `MON_PIPELINE_FRESHNESS` → **Create** → **Create a semantic model
   only**.

**11c. Model** (open the semantic model, switch **Viewing → Editing**; edits save immediately, and
**Version history** restores older versions).

1. **Model options:** turn off *Auto date/time* and *Autodetect new relationships*; delete any relationships
   Power BI created.
2. **Manage relationships → New**, all *many to one*, cross-filter *single*:

   | # | From (many) | To (one) | Active |
   |---|---|---|---|
   | 1 | `FACT_TRANSACTIONS[TRANSACTION_DATE]` | `DIM_DATE[DATE]` | yes |
   | 2 | `FACT_TRANSACTIONS[ACCOUNT_ID]` | `DIM_ACCOUNTS[ACCOUNT_ID]` | yes |
   | 3 | `DIM_ACCOUNTS[CUSTOMER_ID]` | `DIM_CUSTOMERS[CUSTOMER_ID]` | yes |
   | 4 | `DIM_CUSTOMERS_HISTORY[CUSTOMER_ID]` | `DIM_CUSTOMERS[CUSTOMER_ID]` | yes |
   | 5 | `FACT_TRANSACTIONS[RELATED_ACCOUNT_ID]` | `DIM_ACCOUNTS[ACCOUNT_ID]` | **no** (used by `Transfer In Value`) |

   Customer filters flow customer → accounts → transactions along one path. `FCT_CDC_EVENTS` and
   `MON_PIPELINE_FRESHNESS` stay unconnected.
3. Right-click `DIM_DATE` → **Mark as date table** → column `DATE`.
4. **Properties:** *Summarize by: None* on ID columns; *Sort by column* `MONTH_NAME` → `MONTH_NUM`,
   `YEAR_MONTH` → `YEAR_MONTH_NUM`, `DAY_NAME` → `DAY_OF_WEEK_NUM`, `AMOUNT_BAND` → `AMOUNT_BAND_ORDER`,
   `BALANCE_BAND` → `BALANCE_BAND_ORDER`; currency format on `AMOUNT` and `BALANCE`.

**11d. Measures.** Workspace → semantic model **⋯ → Write DAX queries** → paste
[`powerbi/measures.dax`](../powerbi/measures.dax) → **Update model with changes** (adds all 49 measures) →
**Run** (the two result grids are a self-test). Then set formats in the model editor: currency for values
and balances, percentage for `%` measures, whole number for counts.

**11e. Check the numbers.** Run [`snowflake/powerbi_expected_values.sql`](../snowflake/powerbi_expected_values.sql)
in Snowsight. `Transaction Count`, `Transaction Value`, `Failure Rate %`, `Customer Count`, `Account Count`
and `Total Balance` must equal the first result grid from 11d. If not, refresh the model first; if they still
differ, a relationship is wrong.

**11f. Report pages.** Semantic model → **New report**.

| Page | Visuals |
|---|---|
| Overview | KPI cards, daily value + count trend, value by transaction type, top 10 customers, date slicer |
| Transactions | daily value by type, failure share by type, transactions by UTC hour, amount distribution, transfers between account types, largest transactions |
| Customers | KPI cards, value vs activity scatter, segments by `ACCOUNT_MIX`, new customers over time, customer list |
| Accounts | KPI cards, current balance by account type, balance distribution, account ledger with transfers in/out |
| Pipeline Health | freshness and latency cards, CDC events per load day, events by source table, RAW events vs mart rows, data-quality cards (all 0) |
| Customer Detail | hidden drill-through on `CUSTOMER_LABEL`: accounts, transactions, SCD2 email history |

Sync the date slicer across the first four pages (**View → Sync slicers**); Pipeline Health has none.
Balances are current values, so the date slicer doesn't change them.

**11g. Scheduled refresh.** Semantic model **⋯ → Settings → Data source credentials** (key pair, as in 11b)
→ **Refresh → Configure a refresh schedule** (Pro allows 8 per day).

**11h. End-to-end check.** Run test A (with amount `9999.99`) and test B from step 10, wait ~4 min, then
semantic model **⋯ → Refresh now**: the deposit tops *Largest transactions*, customer #1's Customer Detail
page shows the new email version, and Pipeline Health shows `Fresh`.

The dashboard is as fresh as its last refresh: the pipeline is near-real-time (about 1 min to Snowflake), the
report is not.

---

## 12. Full resync (if Snowflake is missing rows that Postgres has)

This forces Debezium to re-snapshot every table; dbt de-duplicates the resulting events.

```bash
cd kafka-debezium && CONNECTOR_NAME=banking-cdc-connector-v2 ../.venv/bin/python register_connector.py --replace && cd ..
```

Keep using the new name (`CONNECTOR_NAME=banking-cdc-connector-v2`) for later runs of that script.

---

## 13. Stop

1. `Ctrl+C` the generator. The consumer container flushes and commits on `docker compose stop` (SIGTERM);
   a host-run consumer does the same on `Ctrl+C`.
2. `docker compose stop` keeps containers, Kafka topics and connector offsets.
   Avoid `docker compose down` unless you want a clean Kafka; the next start will then re-snapshot.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `kafka` keeps restarting with `NodeExists` | Previous broker session still in ZooKeeper; it clears within ~20 s. If it persists: `docker compose restart zookeeper kafka` |
| Connector task `FAILED` | `curl localhost:8083/connectors/banking-cdc-connector/status` for the trace; then re-run step 3 |
| `fact_transactions was built by an older version of this model` | Run the `--full-refresh` build from step 5 |
| `load_snowflake` fails with `SignatureDoesNotMatch` | MinIO keys in `docker/dags/.env` don't match the MinIO root user |
| `Could not connect to Snowflake backend` | Network/DNS from Docker; the task retries once. Re-trigger the DAG |
| `Snowflake private key not found at ...` | `keys/` is missing or not mounted: run `make snowflake-keys`, then recreate the Airflow containers (step 0b) |
| `JWT token is invalid` / `Failed to authenticate` | The public key on the user doesn't match the private key: re-run the printed `ALTER USER ... SET RSA_PUBLIC_KEY` |
| `Insufficient privileges` / `does not exist or not authorized` | Existing objects are owned by another role: run the ownership transfer (step 0b.3) |
| Power BI: Snowflake sign-in fails | Server must end in `.snowflakecomputing.com`; user `PBI_SVC`, role `PBI_READER`, warehouse `PBI_WH`; the pasted key needs its `BEGIN` / `END` lines; `describe user PBI_SVC` must show `RSA_PUBLIC_KEY_FP` |
| Power BI: totals too high | An extra relationship exists (e.g. `FACT_TRANSACTIONS[CUSTOMER_ID]` → `DIM_CUSTOMERS`): keep exactly the 5 in 11c |
| Power BI: `Transfer In Value` blank | Relationship 5 is missing or active |
| Power BI: no **Write DAX queries** / can't switch to Editing | Workspace settings → enable *Users can edit data models in the Power BI service* |
| Power BI: scheduled refresh fails | Semantic model settings → Data source credentials → re-enter the key; check that `PBI_WH` exists |
| `load_snowflake` fails with `COPY failed for N file(s)` | The log names each file and Snowflake's `first_error`. Fix or remove the file in MinIO; the watermark only moves once every file loads |
| `consumer` never starts | It waits for Kafka and MinIO to be healthy: `docker compose ps` and `docker compose logs minio kafka` |
| dbt test failure in `dbt_build` | Open the task log. Downstream models are skipped, so bad data never reaches the marts |
