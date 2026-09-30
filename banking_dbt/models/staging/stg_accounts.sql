{{ config(materialized='view') }}

-- Current state of each account (including its latest balance), derived from
-- the RAW CDC event log. Same versioning rules as stg_customers.

with events as (
    select
        v:id::number                               as account_id,
        v:customer_id::number                      as customer_id,
        v:account_type::string                     as account_type,
        v:balance::number(18, 2)                   as balance,
        v:currency::string                         as currency,
        v:created_at::timestamp_ntz                as created_at,
        v:updated_at::timestamp_ntz                as updated_at,
        coalesce(v:_cdc_operation::string, 'c')    as cdc_operation,
        v:_source_lsn::number                      as source_lsn,
        v:_kafka_offset::number                    as kafka_offset,
        _loaded_at                                 as raw_loaded_at
    from {{ source('raw', 'accounts') }}
),

latest as (
    select *
    from events
    qualify row_number() over (
        partition by account_id
        order by
            source_lsn desc nulls last,
            kafka_offset desc nulls last,
            updated_at desc nulls last,
            raw_loaded_at desc nulls last
    ) = 1
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    updated_at,
    raw_loaded_at
from latest
where cdc_operation <> 'd'
