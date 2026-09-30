-- ============================================================
-- Least-privilege role + key-pair service users for the pipeline.
-- Run manually in Snowsight as ACCOUNTADMIN ("Run All"). Idempotent:
-- safe to re-run. Replaces the password login with ACCOUNTADMIN.
--
--   BANKING_PIPELINE_ROLE / BANKING_PIPELINE_USER  -> Airflow (COPY + dbt)
--   BANKING_CI_ROLE       / BANKING_CI_USER        -> GitHub Actions CD (dbt build)
--
-- After running it:
--   1. `make snowflake-keys` (and `make snowflake-keys KEY_NAME=snowflake_ci_key SF_USER=BANKING_CI_USER`)
--      prints the ALTER USER ... SET RSA_PUBLIC_KEY statements to run (section 6).
--   2. Transfer ownership of the existing objects (section 7), once.
-- ============================================================

use role ACCOUNTADMIN;

-- COMPUTE_WH = SNOWFLAKE_WAREHOUSE in docker/dags/.env; change it everywhere below if yours differs.

-- ------------------------------------------------------------
-- 1. Pipeline role: Airflow load + dbt build
-- ------------------------------------------------------------
create role if not exists BANKING_PIPELINE_ROLE
    comment = 'Airflow COPY into RAW and dbt build into STAGING / ANALYTICS';
grant role BANKING_PIPELINE_ROLE to role SYSADMIN;  -- keep it in the admin hierarchy

grant usage on warehouse COMPUTE_WH to role BANKING_PIPELINE_ROLE;
grant usage on database BANKING to role BANKING_PIPELINE_ROLE;

-- dbt's on-run-start hook (macros/ensure_raw_tables.sql) runs
-- `create schema if not exists BANKING.RAW`, and dbt creates STAGING /
-- ANALYTICS on a fresh database. Scoped to BANKING only.
grant create schema on database BANKING to role BANKING_PIPELINE_ROLE;

create schema if not exists BANKING.RAW;
create schema if not exists BANKING.STAGING;
create schema if not exists BANKING.ANALYTICS;

-- RAW: landing tables. PUT / COPY use the table stage (@%table), which only
-- needs OWNERSHIP of the table; ALTER TABLE ... ADD COLUMN needs it too.
grant usage, create table on schema BANKING.RAW to role BANKING_PIPELINE_ROLE;
grant select, insert on all tables    in schema BANKING.RAW to role BANKING_PIPELINE_ROLE;
grant select, insert on future tables in schema BANKING.RAW to role BANKING_PIPELINE_ROLE;

-- STAGING: dbt views. ANALYTICS: dbt tables, incremental merges (temp
-- views/tables) and SCD2 snapshots. `create or replace` needs ownership of
-- the existing object, which the pipeline role gets via section 7 / 3.
grant usage, create view, create table on schema BANKING.STAGING   to role BANKING_PIPELINE_ROLE;
grant usage, create view, create table on schema BANKING.ANALYTICS to role BANKING_PIPELINE_ROLE;

-- ------------------------------------------------------------
-- 2. CI role: GitHub Actions `dbt build` (CD on main)
-- ------------------------------------------------------------
-- Inherits the pipeline role, so it can replace objects Airflow built.
-- A separate role + user keeps CI access auditable and revocable on its own.
create role if not exists BANKING_CI_ROLE
    comment = 'GitHub Actions CD: dbt build';
grant role BANKING_PIPELINE_ROLE to role BANKING_CI_ROLE;
grant role BANKING_CI_ROLE to role SYSADMIN;

-- ------------------------------------------------------------
-- 3. Keep ownership with the pipeline role, whoever creates the object
-- ------------------------------------------------------------
-- Without this, a CD run (session role BANKING_CI_ROLE) would own the
-- objects it (re)creates, and Airflow's next `create or replace` would fail.
grant ownership on future tables in schema BANKING.RAW       to role BANKING_PIPELINE_ROLE;
grant ownership on future tables in schema BANKING.STAGING   to role BANKING_PIPELINE_ROLE;
grant ownership on future views  in schema BANKING.STAGING   to role BANKING_PIPELINE_ROLE;
grant ownership on future tables in schema BANKING.ANALYTICS to role BANKING_PIPELINE_ROLE;
grant ownership on future views  in schema BANKING.ANALYTICS to role BANKING_PIPELINE_ROLE;

-- ------------------------------------------------------------
-- 4. Service users (key-pair only: TYPE = SERVICE cannot use a password)
-- ------------------------------------------------------------
create user if not exists BANKING_PIPELINE_USER
    type = service
    default_role = BANKING_PIPELINE_ROLE
    default_warehouse = COMPUTE_WH
    default_namespace = 'BANKING.RAW'
    comment = 'Airflow: minio_to_snowflake_banking + dbt_banking_build';
grant role BANKING_PIPELINE_ROLE to user BANKING_PIPELINE_USER;

create user if not exists BANKING_CI_USER
    type = service
    default_role = BANKING_CI_ROLE
    default_warehouse = COMPUTE_WH
    default_namespace = 'BANKING.ANALYTICS'
    comment = 'GitHub Actions CD (dbt build)';
grant role BANKING_CI_ROLE to user BANKING_CI_USER;

-- ------------------------------------------------------------
-- 5. Power BI keeps working
-- ------------------------------------------------------------
-- PBI_READER's future SELECT grants on ANALYTICS (powerbi_setup.sql) also
-- apply to tables the pipeline role creates; nothing to change here.

-- ------------------------------------------------------------
-- 6. Attach the public keys (paste the output of `make snowflake-keys`)
-- ------------------------------------------------------------
-- alter user BANKING_PIPELINE_USER set RSA_PUBLIC_KEY = '<paste>';
-- alter user BANKING_CI_USER       set RSA_PUBLIC_KEY = '<paste>';
-- Check: RSA_PUBLIC_KEY_FP must be set.
-- describe user BANKING_PIPELINE_USER;

-- ------------------------------------------------------------
-- 7. ONE-TIME: hand existing objects to the pipeline role (NOT run by default)
-- ------------------------------------------------------------
-- Objects created so far are owned by ACCOUNTADMIN (the old login). The
-- pipeline role must own them to PUT into @%table, ALTER the RAW tables and
-- `create or replace` the dbt models / snapshots. COPY CURRENT GRANTS keeps
-- existing grants (e.g. PBI_READER SELECT). Review, then uncomment and run:
--
-- grant ownership on all tables in schema BANKING.RAW       to role BANKING_PIPELINE_ROLE copy current grants;
-- grant ownership on all tables in schema BANKING.STAGING   to role BANKING_PIPELINE_ROLE copy current grants;
-- grant ownership on all views  in schema BANKING.STAGING   to role BANKING_PIPELINE_ROLE copy current grants;
-- grant ownership on all tables in schema BANKING.ANALYTICS to role BANKING_PIPELINE_ROLE copy current grants;
-- grant ownership on all views  in schema BANKING.ANALYTICS to role BANKING_PIPELINE_ROLE copy current grants;
--
-- Verify (owner column must be BANKING_PIPELINE_ROLE):
-- show tables in schema BANKING.RAW;
-- show objects in schema BANKING.ANALYTICS;

-- ------------------------------------------------------------
-- 8. Smoke test as the new role
-- ------------------------------------------------------------
use role BANKING_PIPELINE_ROLE;
use warehouse COMPUTE_WH;
show grants to role BANKING_PIPELINE_ROLE;
use role ACCOUNTADMIN;
