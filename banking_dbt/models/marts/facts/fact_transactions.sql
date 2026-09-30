{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    merge_exclude_columns=['dbt_inserted_at'],
    on_schema_change='append_new_columns',
    post_hook="delete from {{ this }} where is_deleted"
) }}

-- Grain: one row per banking transaction (transaction_id), as originated by
-- account_id. A TRANSFER is one row: account_id = sender,
-- related_account_id = receiver.

{% if is_incremental() and execute %}
    {% set existing_cols = adapter.get_columns_in_relation(this) | map(attribute='name') | map('upper') | list %}
    {% if 'RAW_LOADED_AT' not in existing_cols %}
        {{ exceptions.raise_compiler_error(
            this ~ " was built by an older version of this model (no RAW_LOADED_AT column). "
            ~ "Rebuild it once with: dbt build --full-refresh --select fact_transactions+"
        ) }}
    {% endif %}
{% endif %}

with txns as (
    select *
    from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    -- New RAW data (2h look-back for late files), plus rows whose account
    -- arrived after the transaction (late-arriving dimension) so they get enriched.
    where raw_loaded_at > (
            select dateadd(hour, -2, coalesce(max(raw_loaded_at), '1900-01-01'::timestamp_ltz))
            from {{ this }}
        )
       or transaction_id in (
            select transaction_id
            from {{ this }}
            where customer_id is null
               or (related_account_id is not null and counterparty_customer_id is null)
        )
    {% endif %}
),

accounts as (
    select account_id, customer_id, account_type
    from {{ ref('stg_accounts') }}
)

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    a.account_type,
    t.related_account_id,
    r.customer_id                                   as counterparty_customer_id,
    r.account_type                                  as counterparty_account_type,
    case
        when t.related_account_id is null then null
        else a.customer_id = r.customer_id
    end                                             as is_own_account_transfer,
    t.transaction_type,
    t.status,
    t.amount,
    case
        when t.amount < 50   then '< 50'
        when t.amount < 100  then '50 - 100'
        when t.amount < 250  then '100 - 250'
        when t.amount < 500  then '250 - 500'
        when t.amount < 1000 then '500 - 1K'
        else '1K+'
    end                                             as amount_band,
    case
        when t.amount < 50   then 1
        when t.amount < 100  then 2
        when t.amount < 250  then 3
        when t.amount < 500  then 4
        when t.amount < 1000 then 5
        else 6
    end                                             as amount_band_order,
    t.transaction_time,                             -- UTC
    t.transaction_time::date                        as transaction_date,
    hour(t.transaction_time)                        as transaction_hour,
    t.is_deleted,
    t.raw_loaded_at,
    current_timestamp()                             as dbt_inserted_at,
    current_timestamp()                             as dbt_updated_at
from txns t
left join accounts a
    on t.account_id = a.account_id
left join accounts r
    on t.related_account_id = r.account_id
