{{ config(materialized='table') }}

-- Grain: one row per observed account balance version (CDC create / update /
-- snapshot-read event). Built straight from the RAW event log, so it keeps the
-- balance history that the SCD2 snapshot intentionally does not track.

with events as (
    select
        v:id::number                                              as account_id,
        v:customer_id::number                                     as customer_id,
        v:balance::number(18, 2)                                  as balance,
        coalesce(
            v:updated_at::timestamp_ntz,
            to_timestamp_ntz(v:_source_ts_ms::number, 3),
            v:created_at::timestamp_ntz
        )                                                         as balance_as_of,   -- UTC
        coalesce(v:_cdc_operation::string, 'c')                   as cdc_operation,
        v:_source_lsn::number                                     as source_lsn,
        v:_kafka_offset::number                                   as kafka_offset,
        _loaded_at                                                as raw_loaded_at
    from {{ source('raw', 'accounts') }}
),

-- The same event can land in RAW more than once (at-least-once delivery)
deduped as (
    select *
    from events
    where cdc_operation <> 'd'
    qualify row_number() over (
        partition by account_id, coalesce(source_lsn, -1), balance, balance_as_of
        order by raw_loaded_at desc nulls last
    ) = 1
)

select
    account_id,
    customer_id,
    balance,
    balance - lag(balance) over (
        partition by account_id
        order by balance_as_of, source_lsn nulls first, kafka_offset nulls first
    )                                                             as balance_change,
    balance_as_of,
    balance_as_of::date                                           as balance_date,
    cdc_operation,
    source_lsn
from deduped
