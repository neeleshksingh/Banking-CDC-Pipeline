{{ config(materialized='table') }}

-- SCD Type 2: one row per account per version (from accounts_snapshot).
-- balance is intentionally excluded: the snapshot only versions descriptive
-- attributes, so a stored balance would be stale.

select
    dbt_scd_id                               as account_version_key,
    account_id::number                       as account_id,
    customer_id::number                      as customer_id,
    account_type,
    currency,
    created_at,
    dbt_valid_from                           as valid_from,
    dbt_valid_to                             as valid_to,
    dbt_valid_to is null                     as is_current
from {{ ref('accounts_snapshot') }}
