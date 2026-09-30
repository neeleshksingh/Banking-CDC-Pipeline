# Real-Time Banking Data Engineering Pipeline

CDC pipeline from an OLTP banking database to a Snowflake dimensional model, orchestrated by Airflow and
modelled with dbt.

```text
Postgres ─▶ Debezium ─▶ Kafka ─▶ Python consumer ─▶ MinIO (Parquet, date-partitioned)
   ─▶ Airflow (1-min micro-batch COPY) ─▶ Snowflake RAW ─▶ dbt (staging ▸ SCD2 snapshots ▸ marts ▸ tests)
```

| Layer | Tech | Latency |
|---|---|---|
| Change capture | Postgres logical WAL + Debezium (pgoutput) | seconds (streaming) |
| Landing | Kafka → consumer → MinIO Parquet, commit-after-write | ~10 s batches |
| Warehouse load | Airflow → `PUT` + `COPY INTO` (idempotent via load history) | ≤ 1 min (micro-batch) |
| Transform | dbt `build`, triggered by an Airflow Dataset when new RAW data lands | ~1–2 min |

End to end: about 2–4 minutes from a Postgres commit to the marts. This is *near-real-time*, not streaming analytics.

## Snowflake layout

| Schema | Content |
|---|---|
| `BANKING.RAW` | Append-only CDC event log (`V` variant + `_FILE_NAME`, `_LOADED_AT`) |
| `BANKING.STAGING` | Current state per entity: latest version by Postgres LSN, deletes applied |
| `BANKING.ANALYTICS` | `DIM_CUSTOMERS`, `DIM_ACCOUNTS`, `DIM_DATE`, `FACT_TRANSACTIONS` (one row per transaction), SCD2 `DIM_*_HISTORY`, `FCT_ACCOUNT_BALANCE_CHANGES`, monitoring (`FCT_CDC_EVENTS`, `MON_PIPELINE_FRESHNESS`) |

## Run it

See **[docs/RUNBOOK.md](docs/RUNBOOK.md)** for step-by-step setup, verification queries and hands-on CDC
tests (insert / update / SCD2 / delete / failure recovery / idempotent reload).

## Repository

| Path | Purpose |
|---|---|
| `postgres/schema.sql` | Source schema (idempotent; constraints, indexes, `updated_at`, `REPLICA IDENTITY FULL`) |
| `data-generator/` | Balance-aware transaction generator (live mode and `--backfill-days`) |
| `kafka-debezium/` | Debezium connector registration (create or update) |
| `consumer/` | Kafka → MinIO Parquet writer with CDC metadata, batching and rewind-on-failure |
| `docker/dags/` | Airflow DAGs: `minio_to_snowflake_banking`, `dbt_banking_build` |
| `banking_dbt/` | dbt project: staging, snapshots, marts, monitoring, tests |
| `snowflake/` | One-time cleanup and verification SQL |
