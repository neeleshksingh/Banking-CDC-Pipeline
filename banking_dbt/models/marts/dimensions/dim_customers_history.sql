{{ config(materialized='table') }}

-- SCD Type 2: one row per customer per version (from customers_snapshot).

select
    dbt_scd_id                               as customer_version_key,
    customer_id::number                      as customer_id,
    first_name,
    last_name,
    email,
    created_at,
    dbt_valid_from                           as valid_from,
    dbt_valid_to                             as valid_to,
    dbt_valid_to is null                     as is_current
from {{ ref('customers_snapshot') }}
