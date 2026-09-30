# banking_dbt

dbt project that turns the raw CDC event log in `BANKING.RAW` into a tested dimensional model.

## Layers

| Folder | Schema | Materialization | What it does |
| --- | --- | --- | --- |
| `models/staging` | `STAGING` | view | Casts types and applies CDC: keeps the latest version of each record (ordered by Postgres LSN, then Kafka offset) and drops deleted records |
| `snapshots` | `ANALYTICS` | snapshot (check strategy) | SCD Type-2 history for customers (name, email) and accounts (owner, type, currency) |
| `models/marts` | `ANALYTICS` | table / incremental | `dim_customers`, `dim_accounts`, `dim_date`, `fact_transactions` (incremental merge), SCD2 history dims, balance changes |
| `models/monitoring` | `ANALYTICS` | table | `fct_cdc_events` and `mon_pipeline_freshness` for pipeline health |
| `tests` | – | – | Custom tests: positive amounts, transfer counterparty rules, one current SCD2 version per key |

`macros/generate_schema_name.sql` makes models land in exactly `STAGING` / `ANALYTICS` regardless of the
profile's default schema. `macros/ensure_raw_tables.sql` (an `on-run-start` hook) creates the RAW tables and
their audit columns if they are missing.

## Running

In production this runs from Airflow (`dbt_banking_build` DAG) after each load. Locally:

```bash
set -a; source ../docker/dags/.env; set +a      # SNOWFLAKE_* variables
dbt build --profiles-dir .dbt                   # models + snapshots + tests
dbt source freshness --profiles-dir .dbt
```

`--full-refresh` is only needed if `fact_transactions` was built by an older version of the model.
