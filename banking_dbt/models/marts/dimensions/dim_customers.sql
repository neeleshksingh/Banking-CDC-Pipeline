{{ config(materialized='table') }}

-- One row per current (non-deleted) customer. Type 1 (latest values);
-- full SCD2 history is in dim_customers_history.

with customers as (
    select * from {{ ref('stg_customers') }}
),

accounts as (
    select
        customer_id,
        count(*)                                 as account_count,
        count_if(account_type = 'SAVINGS')       as savings_accounts,
        count_if(account_type = 'CHECKING')      as checking_accounts
    from {{ ref('stg_accounts') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name           as customer_name,
    c.email,
    c.created_at,
    c.created_at::date                           as created_date,
    c.updated_at,
    coalesce(a.account_count, 0)                 as account_count,
    case
        when a.savings_accounts > 0 and a.checking_accounts > 0 then 'MIXED'
        when a.savings_accounts > 0 then 'SAVINGS_ONLY'
        when a.checking_accounts > 0 then 'CHECKING_ONLY'
        else 'NO_ACCOUNTS'
    end                                          as account_mix
from customers c
left join accounts a
    on c.customer_id = a.customer_id
