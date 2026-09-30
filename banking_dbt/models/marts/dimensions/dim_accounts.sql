{{ config(materialized='table') }}

-- One row per current (non-deleted) account with its latest balance.
-- Type 1; SCD2 history of descriptive attributes is in dim_accounts_history,
-- balance movements are in fct_account_balance_changes.

select
    account_id,
    customer_id,
    account_type,
    currency,
    balance,
    case
        when balance < 500   then '0 - 500'
        when balance < 1000  then '500 - 1K'
        when balance < 2500  then '1K - 2.5K'
        when balance < 5000  then '2.5K - 5K'
        else '5K+'
    end                                  as balance_band,
    case
        when balance < 500   then 1
        when balance < 1000  then 2
        when balance < 2500  then 3
        when balance < 5000  then 4
        else 5
    end                                  as balance_band_order,
    created_at,
    created_at::date                     as created_date,
    updated_at                           as balance_updated_at
from {{ ref('stg_accounts') }}
