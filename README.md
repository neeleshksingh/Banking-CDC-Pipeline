# 🏦 Real-Time Banking Data Engineering Pipeline

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-EF3B2D?logo=apache&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?logo=git&logoColor=white)
![CI/CD](https://img.shields.io/badge/CI%2FCD-000000?logo=githubactions&logoColor=white)

---

## 📌 Project Overview

This project demonstrates an **end-to-end CDC data pipeline** for a **banking domain**.
We simulate **customer, account, and transaction activity** in Postgres, capture every change in real time with
Debezium, land it in a data lake, load it into Snowflake in micro-batches, and transform it with dbt into a tested
dimensional model with **SCD Type-2 history**. A Power BI dashboard on top of the marts is in progress.

👉 End to end, a change in Postgres reaches the analytics tables in about **2–4 minutes**. That's
**near-real-time**: capture is streaming, and the warehouse load and transformations run as micro-batches.

---

## 🏗️ Architecture

<img width="5647" height="3107" alt="Architecture" src="https://github.com/user-attachments/assets/7521ea8a-451e-46ff-9db0-71dd6ddf8181" />

```text
Postgres ─WAL─▶ Debezium ─▶ Kafka ─▶ Python consumer ─▶ MinIO (Parquet)
                                                          │ Airflow, every 1 min
                                                          ▼
                                                 Snowflake RAW (bronze)
                                                          │ Airflow Dataset trigger → dbt build
                                                          ▼
                                   STAGING (silver) ─▶ ANALYTICS (gold: snapshots, dims, facts, monitoring)
```

**Pipeline Flow:**

1. **Data Generator** → Simulates customers, accounts and balance-aware transactions in Postgres (Faker).
2. **Debezium** → Captures every insert/update/delete from the Postgres WAL into Kafka topics.
3. **Python consumer** → Writes CDC events from Kafka to MinIO (S3-compatible) as date-partitioned Parquet, committing Kafka offsets only after a successful upload.
4. **Airflow** → Loads new MinIO files into Snowflake RAW every minute, then triggers dbt when new data lands.
5. **Snowflake** → Cloud data warehouse: `RAW` (bronze) → `STAGING` (silver) → `ANALYTICS` (gold).
6. **dbt** → Applies CDC, builds SCD Type-2 snapshots, dimensions, facts and monitoring models, and runs data-quality tests.
7. **CI/CD with GitHub Actions** → Lint and dbt compile on every push/PR; `dbt build` on merge to `main`.

### ⏱️ Latency by stage

| Stage | Mechanism | Latency |
| --- | --- | --- |
| Change capture | Postgres WAL → Debezium → Kafka | seconds (streaming) |
| Landing | Kafka → consumer → MinIO | ~10 s batches |
| Warehouse load | Airflow → `PUT` + `COPY INTO` Snowflake | ≤ 1 min (micro-batch) |
| Transformation | Airflow Dataset → `dbt build` | ~1–2 min |

---

## ⚡ Tech Stack

- **Snowflake** → Cloud Data Warehouse
- **DBT** → Transformations, testing, snapshots (SCD Type-2)
- **Apache Airflow** → Orchestration & DAG scheduling
- **Apache Kafka + Debezium** → Real-time change data capture (CDC)
- **MinIO** → S3-compatible object storage (data lake landing zone)
- **Postgres** → Source OLTP system
- **Python (Faker)** → Data simulation and the Kafka → MinIO consumer
- **Docker & docker-compose** → Containerized setup
- **Git & GitHub Actions** → CI/CD workflows

---

## ✅ Key Features

- **PostgreSQL OLTP**: source database with ACID guarantees, constraints and indexes (customers, accounts, transactions)
- **Realistic simulation**: balances update with every transaction; overdrawing debits are recorded as `FAILED`; customers occasionally change email
- **Change Data Capture (CDC)** via Debezium reading the Postgres WAL (inserts, updates, deletes)
- **At-least-once, no-loss landing**: the consumer rewinds and retries a failed upload instead of skipping events
- **Idempotent warehouse loads**: re-processing files never duplicates rows (Snowflake load history)
- **Correct CDC apply**: latest version per record chosen by Postgres LSN; deletes removed downstream
- **SCD Type-2 snapshots** for customer and account history
- **RAW → STAGING → Fact/Dimension** models in dbt, plus a date dimension
- **47 dbt data-quality tests**: unique and not-null keys, relationships, accepted values, custom business rules
- **Pipeline monitoring models**: CDC operation counts, ingestion latency, freshness per table
- **CI/CD**: lint + dbt compile in CI; `dbt build` (models, snapshots, tests) in CD

---

## 🗄️ Data Model (Snowflake `BANKING.ANALYTICS`)

| Object | Grain / purpose |
| --- | --- |
| `FACT_TRANSACTIONS` | One row per transaction (a transfer = one row: `account_id` sends, `related_account_id` receives) |
| `DIM_CUSTOMERS` | One row per current customer |
| `DIM_ACCOUNTS` | One row per current account, with latest balance |
| `DIM_DATE` | Calendar dimension (one row per day) |
| `DIM_CUSTOMERS_HISTORY`, `DIM_ACCOUNTS_HISTORY` | SCD Type-2 versions (`valid_from`, `valid_to`, `is_current`) |
| `FCT_ACCOUNT_BALANCE_CHANGES` | One row per observed balance change (from CDC events) |
| `FCT_CDC_EVENTS`, `MON_PIPELINE_FRESHNESS` | Pipeline monitoring: events by operation, ingestion latency, freshness |

`BANKING.RAW` holds the append-only CDC event log; `BANKING.STAGING` holds the current state per record.

---

## 📂 Repository Structure

```text
Real-Time-Banking-Data-Engineering-Pipeline/
├── .github/workflows/            # CI/CD pipelines (ci.yml, cd.yml)
├── banking_dbt/                  # dbt project
│   ├── macros/                   # schema naming, RAW table setup
│   ├── models/
│   │   ├── staging/              # CDC-applied current state (views)
│   │   ├── marts/                # facts & dimensions (incl. dim_date, SCD2 history)
│   │   ├── monitoring/           # CDC events & pipeline freshness
│   │   └── sources.yml
│   ├── snapshots/                # SCD2 snapshots
│   ├── tests/                    # custom data-quality tests
│   └── dbt_project.yml
├── consumer/
│   └── kafka_to_minio.py         # Kafka → MinIO Parquet writer
├── data-generator/
│   └── faker_generator.py        # Faker-based banking simulator
├── docker/
│   └── dags/                     # minio_to_snowflake_dag.py, dbt_banking_build.py
├── docs/
│   └── RUNBOOK.md                # step-by-step run & test guide
├── kafka-debezium/
│   └── generator_and_post_connector.py   # registers the Debezium connector
├── postgres/
│   └── schema.sql                # OLTP DDL (constraints, indexes, CDC settings)
├── snowflake/                    # one-time cleanup & verification SQL
├── .gitignore
├── docker-compose.yml            # containerized infrastructure
├── dockerfile-airflow.dockerfile
├── requirements.txt
└── README.md
```

---

## ⚙️ Step-by-Step Implementation

### **1. Data Simulation**

- Generated synthetic banking data (**customers, accounts, transactions**) using **Faker**.
- Inserted data into **PostgreSQL (OLTP)** so the system behaves like a real transactional database (**ACID, constraints**).
- Each transaction updates account balances in the same database transaction; debits that would overdraw are recorded as `FAILED`.
- Controlled via CLI flags (`--iterations`, `--customers`, `--transactions`, `--backfill-days` for historical data).

---

### **2. Debezium CDC + Kafka → MinIO**

- Set up **Kafka Connect & Debezium** to capture changes from the **Postgres WAL** into Kafka topics.
- A **Python consumer** batches events, adds CDC metadata (operation, LSN, Kafka offset) and writes **Parquet files to MinIO**, committing offsets only after a successful upload.

---

### **3. Airflow Orchestration**

- `minio_to_snowflake_banking` (every minute): loads only **new** MinIO files into **Snowflake RAW**; skips without touching Snowflake when nothing is new.
- `dbt_banking_build` (triggered by an Airflow **Dataset** when new RAW data lands): runs `dbt build` for staging, snapshots, marts and tests.

---

### **4. Snowflake Warehouse**

- Organized into **RAW (bronze) → STAGING (silver) → ANALYTICS (gold)**.
- **RAW** holds the append-only CDC event log with load-audit columns (`_FILE_NAME`, `_LOADED_AT`).

---

### **5. dbt Transformations**

- **Staging models** → apply CDC: latest version per record (by Postgres LSN), deletes removed.
- **Dimension & fact models** → star schema, plus a date dimension.
- **Snapshots** → SCD Type-2 history of accounts & customers.
- **Tests** → 47 data-quality checks run on every build.

---

### **6. CI/CD with GitHub Actions**

- **ci.yml** → Ruff lint + `dbt compile` on pushes and pull requests.
- **cd.yml** → `dbt build` (models + snapshots + tests) on merge to `main`.

---

## ▶️ How to Run

See **[docs/RUNBOOK.md](docs/RUNBOOK.md)** for step-by-step setup, verification queries and hands-on CDC tests
(insert, update, SCD2, delete, failure recovery, idempotent reload).

---

## 📊 Final Deliverables

- **Automated CDC pipeline** from Postgres → Snowflake (near-real-time, ~2–4 min end to end)
- **dbt models** (facts, dimensions, SCD2 snapshots, monitoring) with **47 data-quality tests**
- **Orchestrated DAGs in Airflow** (dataset-driven)
- **Synthetic banking dataset** for demos, including historical backfill
- **CI/CD workflows** for linting, compilation and tested deployments
- **Power BI dashboard** (in progress)
