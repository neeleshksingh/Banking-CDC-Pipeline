-- ============================================================
-- Pipeline verification queries (read-only). Run in Snowsight.
-- Compare the counts with Postgres (see docs/RUNBOOK.md, step 9).
-- ============================================================

use database BANKING;

-- 1. Row counts per layer (RAW = events, STAGING/ANALYTICS = current state)
select 'raw events'       as layer, 'customers' as entity, count(*) as row_count from RAW.CUSTOMERS
union all select 'raw events',       'accounts',     count(*) from RAW.ACCOUNTS
union all select 'raw events',       'transactions', count(*) from RAW.TRANSACTIONS
union all select 'staging (current)', 'customers',    count(*) from STAGING.STG_CUSTOMERS
union all select 'staging (current)', 'accounts',     count(*) from STAGING.STG_ACCOUNTS
union all select 'staging (current)', 'transactions', count_if(not is_deleted) from STAGING.STG_TRANSACTIONS
union all select 'marts',             'customers',    count(*) from ANALYTICS.DIM_CUSTOMERS
union all select 'marts',             'accounts',     count(*) from ANALYTICS.DIM_ACCOUNTS
union all select 'marts',             'transactions', count(*) from ANALYTICS.FACT_TRANSACTIONS
order by entity, layer;

-- 2. Fact grain: must return 0 duplicates and 0 NULL types
select
    count(*)                                   as fact_rows,
    count(*) - count(distinct transaction_id)  as duplicate_ids,
    count_if(transaction_type is null)         as null_types,
    count_if(customer_id is null)              as missing_customer
from ANALYTICS.FACT_TRANSACTIONS;

-- 3. Business mix
select transaction_type, status, count(*) as txns, sum(amount) as total_amount
from ANALYTICS.FACT_TRANSACTIONS
group by 1, 2
order by 1, 2;

-- 4. Pipeline health (one row per source table)
select * from ANALYTICS.MON_PIPELINE_FRESHNESS order by source_table;

-- 5. CDC operations landed per hour
select date_trunc('hour', raw_loaded_at_utc) as hour_utc, source_table, cdc_operation, count(*) as events
from ANALYTICS.FCT_CDC_EVENTS
group by 1, 2, 3
order by 1 desc, 2, 3
limit 50;

-- 6. SCD2: customers with more than one version (after an email update)
select customer_id, count(*) as versions, max(valid_from) as latest_change
from ANALYTICS.DIM_CUSTOMERS_HISTORY
group by customer_id
having count(*) > 1
order by latest_change desc;

-- 7. Balance history for the most active account
select *
from ANALYTICS.FCT_ACCOUNT_BALANCE_CHANGES
where account_id = (
    select account_id from ANALYTICS.FACT_TRANSACTIONS group by 1 order by count(*) desc limit 1
)
order by balance_as_of;
