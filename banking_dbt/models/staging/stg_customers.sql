{{ config(materialized='view') }}

-- Current state of each customer, derived from the RAW CDC event log.
-- Latest version = highest Postgres LSN; Kafka offset / updated_at / load time
-- break ties and order legacy rows loaded before CDC metadata existed.
-- Customers whose latest event is a DELETE are excluded (the snapshot then
-- invalidates them via hard_deletes='invalidate').

with events as (
    select
        v:id::number                               as customer_id,
        v:first_name::string                       as first_name,
        v:last_name::string                        as last_name,
        v:email::string                            as email,
        v:created_at::timestamp_ntz                as created_at,
        v:updated_at::timestamp_ntz                as updated_at,
        coalesce(v:_cdc_operation::string, 'c')    as cdc_operation,
        v:_source_lsn::number                      as source_lsn,
        v:_kafka_offset::number                    as kafka_offset,
        _loaded_at                                 as raw_loaded_at
    from {{ source('raw', 'customers') }}
),

latest as (
    select *
    from events
    qualify row_number() over (
        partition by customer_id
        order by
            source_lsn desc nulls last,
            kafka_offset desc nulls last,
            updated_at desc nulls last,
            raw_loaded_at desc nulls last
    ) = 1
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at,
    raw_loaded_at
from latest
where cdc_operation <> 'd'
