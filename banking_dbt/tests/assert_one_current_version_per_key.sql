-- SCD2 integrity: each business key has at most one open (current) version.
select 'customer' as entity, customer_id as business_key, count(*) as current_versions
from {{ ref('dim_customers_history') }}
where is_current
group by customer_id
having count(*) > 1

union all

select 'account', account_id, count(*)
from {{ ref('dim_accounts_history') }}
where is_current
group by account_id
having count(*) > 1
