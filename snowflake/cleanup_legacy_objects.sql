-- ============================================================
-- One-time cleanup after upgrading the dbt project layout.
--
-- Earlier versions built staging views and marts into BANKING.RAW (and a CD
-- run built another copy into BANKING.ANALYTICS with VARCHAR ids). All of
-- these are derived objects and are fully rebuilt by `dbt build` from RAW.
--
-- NOT touched: RAW landing tables (CUSTOMERS, ACCOUNTS, TRANSACTIONS) and the
-- SCD2 snapshot tables (ANALYTICS.*_SNAPSHOT), which hold history.
-- ============================================================

use database BANKING;

-- Old marts/staging that lived in the RAW (landing) schema
drop view  if exists RAW.STG_CUSTOMERS;
drop view  if exists RAW.STG_ACCOUNTS;
drop view  if exists RAW.STG_TRANSACTIONS;
drop table if exists RAW.DIM_CUSTOMERS;
drop table if exists RAW.DIM_ACCOUNTS;
drop table if exists RAW.FACT_TRANSACTIONS;

-- Old copies in ANALYTICS (staging now lives in BANKING.STAGING)
drop view  if exists ANALYTICS.STG_CUSTOMERS;
drop view  if exists ANALYTICS.STG_ACCOUNTS;
drop view  if exists ANALYTICS.STG_TRANSACTIONS;

-- Old-layout fact (VARCHAR ids, old columns); dbt rebuilds it with the new schema
drop table if exists ANALYTICS.FACT_TRANSACTIONS;
