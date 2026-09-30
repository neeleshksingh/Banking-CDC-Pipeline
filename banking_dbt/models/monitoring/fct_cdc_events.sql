{{ config(materialized='table') }}

-- Grain: one row per CDC event landed in RAW (all tables).
-- Rows loaded before the consumer added CDC metadata show cdc_operation = 'legacy'.

{% for table in ['customers', 'accounts', 'transactions'] %}
select
    '{{ table }}'                                                  as source_table,
    v:id::number                                                   as record_id,
    coalesce(v:_cdc_operation::string, 'legacy')                   as cdc_operation,
    to_timestamp_ntz(v:_source_ts_ms::number, 3)                   as source_committed_at_utc,
    to_timestamp_ntz(v:_cdc_ts_ms::number, 3)                      as captured_at_utc,
    convert_timezone('UTC', _loaded_at)::timestamp_ntz             as raw_loaded_at_utc,
    datediff(
        'second',
        to_timestamp_ntz(v:_source_ts_ms::number, 3),
        convert_timezone('UTC', _loaded_at)::timestamp_ntz
    )                                                              as ingest_latency_seconds,
    v:_kafka_partition::number                                     as kafka_partition,
    v:_kafka_offset::number                                        as kafka_offset,
    _file_name                                                     as file_name
from {{ source('raw', table) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
