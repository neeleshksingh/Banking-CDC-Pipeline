-- ============================================================
-- Power BI access: read-only role + small auto-suspending warehouse.
-- Run once in Snowsight as ACCOUNTADMIN ("Run All").
--
-- Power BI then logs in with YOUR Snowflake user but uses the
-- PBI_READER role, which can only SELECT from BANKING.ANALYTICS.
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

-- 4. Give the role to the user you are logged in as
set my_user = current_user();
grant role PBI_READER to user identifier($my_user);

-- 5. Verify: must return a number, not an error
use role PBI_READER;
use warehouse PBI_WH;
select count(*) as fact_rows from BANKING.ANALYTICS.FACT_TRANSACTIONS;
use role ACCOUNTADMIN;
