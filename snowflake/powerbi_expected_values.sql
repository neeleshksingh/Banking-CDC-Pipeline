-- ============================================================
-- Expected values for the Power BI measures (no slicers applied).
-- Run in Snowsight right after a Power BI refresh: the report's KPI
-- cards must show the same numbers. If they don't, a relationship or
-- measure is wrong (see docs/POWERBI.md -> "Check your numbers").
-- ============================================================

use database BANKING;

-- 1. KPI cards
select
    count(*)                                                        as "Transaction Count",
    count_if(status = 'COMPLETED')                                  as "Completed Transactions",
    round(count_if(status = 'FAILED') / nullif(count(*), 0), 4)     as "Failure Rate %",
    sum(iff(status = 'COMPLETED', amount, 0))                       as "Transaction Value",
    round(sum(iff(status = 'COMPLETED', amount, 0))
          / nullif(count_if(status = 'COMPLETED'), 0), 2)           as "Avg Transaction Value",
    sum(iff(status = 'COMPLETED' and transaction_type = 'DEPOSIT', amount, 0))
      - sum(iff(status = 'COMPLETED' and transaction_type = 'WITHDRAWAL', amount, 0))
                                                                    as "Net External Flow",
    count(distinct account_id)                                      as "Active Accounts",
    count(distinct customer_id)                                     as "Active Customers",
    (select count(*) from ANALYTICS.DIM_CUSTOMERS)                  as "Customer Count",
    (select count(*) from ANALYTICS.DIM_ACCOUNTS)                   as "Account Count",
    (select sum(balance) from ANALYTICS.DIM_ACCOUNTS)               as "Total Balance",
    max(transaction_date)                                           as "Last Data Date"
from ANALYTICS.FACT_TRANSACTIONS;

-- 2. By transaction type (matches the type bar chart)
select
    transaction_type,
    count(*)                                  as "Transaction Count",
    sum(iff(status = 'COMPLETED', amount, 0)) as "Transaction Value"
from ANALYTICS.FACT_TRANSACTIONS
group by transaction_type
order by transaction_type;

-- 3. Transfers in vs out for the top 5 accounts (matches the Account page table)
with outgoing as (
    select account_id, sum(amount) as transfer_out
    from ANALYTICS.FACT_TRANSACTIONS
    where transaction_type = 'TRANSFER' and status = 'COMPLETED'
    group by account_id
),
incoming as (
    select related_account_id as account_id, sum(amount) as transfer_in
    from ANALYTICS.FACT_TRANSACTIONS
    where transaction_type = 'TRANSFER' and status = 'COMPLETED'
    group by related_account_id
)
select a.account_id, coalesce(o.transfer_out, 0) as "Transfer Out Value", coalesce(i.transfer_in, 0) as "Transfer In Value"
from ANALYTICS.DIM_ACCOUNTS a
left join outgoing o on a.account_id = o.account_id
left join incoming i on a.account_id = i.account_id
order by coalesce(o.transfer_out, 0) + coalesce(i.transfer_in, 0) desc
limit 5;
