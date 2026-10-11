-- ============================================================
-- Power BI access: read-only role, small auto-suspending warehouse and a
-- key-pair service user for scheduled refresh.
-- Run once in Snowsight as ACCOUNTADMIN ("Run All"). Idempotent.
--
-- Then attach the key (section 5):
--   make snowflake-keys KEY_NAME=snowflake_pbi_key SF_USER=PBI_SVC
-- and run the printed ALTER USER statement.
-- ============================================================

use role ACCOUNTADMIN;

-- 1. Read-only role for reporting
create role if not exists PBI_READER;

-- 2. Dedicated XS warehouse: suspends after 60 s idle, so refreshes cost seconds of credits
create warehouse if not exists PBI_WH
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;

grant usage on warehouse PBI_WH to role PBI_READER;

-- 3. Read access to the gold layer only (not RAW / STAGING)
grant usage on database BANKING to role PBI_READER;
grant usage on schema BANKING.ANALYTICS to role PBI_READER;
grant select on all tables in schema BANKING.ANALYTICS to role PBI_READER;
grant select on all views  in schema BANKING.ANALYTICS to role PBI_READER;
-- dbt re-creates tables on every build: future grants keep access working
grant select on future tables in schema BANKING.ANALYTICS to role PBI_READER;
grant select on future views  in schema BANKING.ANALYTICS to role PBI_READER;

-- 4. Who can use the role
-- Power BI's scheduled refresh can't answer an MFA prompt, so it signs in as a
-- SERVICE user with a key pair (no password possible for this user type).
create user if not exists PBI_SVC
    type = service
    default_role = PBI_READER
    default_warehouse = PBI_WH
    comment = 'Power BI semantic model refresh (read-only)';
grant role PBI_READER to user PBI_SVC;

-- Your own login too, to explore the tables in Snowsight as PBI_READER
set my_user = current_user();
grant role PBI_READER to user identifier($my_user);

-- 5. Attach the public key (paste the output of `make snowflake-keys ... SF_USER=PBI_SVC`)
-- alter user PBI_SVC set RSA_PUBLIC_KEY = '<paste>';
-- Check: RSA_PUBLIC_KEY_FP must be set.
-- describe user PBI_SVC;

-- 6. Verify: must return a number, not an error
use role PBI_READER;
use warehouse PBI_WH;
select count(*) as fact_rows from BANKING.ANALYTICS.FACT_TRANSACTIONS;
use role ACCOUNTADMIN;
