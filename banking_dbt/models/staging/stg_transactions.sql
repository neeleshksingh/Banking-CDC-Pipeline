{{ config(materialized='view') }}

-- Latest version of each transaction, one row per transaction_id.
-- Deleted transactions are kept with is_deleted = true so the incremental
-- fact can remove rows it loaded earlier.

with events as (
    select
        v:id::number                               as transaction_id,
        v:account_id::number                       as account_id,
        v:related_account_id::number               as related_account_id,
        v:transaction_type::string                 as transaction_type,
        v:amount::number(18, 2)                    as amount,
        v:status::string                           as status,
        v:created_at::timestamp_ntz                as transaction_time,
        v:updated_at::timestamp_ntz                as updated_at,
        coalesce(v:_cdc_operation::string, 'c')    as cdc_operation,
        v:_source_lsn::number                      as source_lsn,
        v:_kafka_offset::number                    as kafka_offset,
        _loaded_at                                 as raw_loaded_at
    from {{ source('raw', 'transactions') }}
),

latest as (
    select *
    from events
    qualify row_number() over (
        partition by transaction_id
        order by
            source_lsn desc nulls last,
            kafka_offset desc nulls last,
            updated_at desc nulls last,
            raw_loaded_at desc nulls last
    ) = 1
)

select
    transaction_id,
    account_id,
    related_account_id,
    transaction_type,
    amount,
    status,
    transaction_time,
    updated_at,
    cdc_operation = 'd'    as is_deleted,
    raw_loaded_at
from latest
