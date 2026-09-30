{{ config(materialized='table') }}

-- Grain: one row per source table. Pipeline health as of the last dbt build.
-- All timestamps are UTC.

with events as (
    select
        source_table,
        count(*)                                     as raw_events,
        count_if(cdc_operation = 'r')                as snapshot_reads,
        count_if(cdc_operation = 'c')                as inserts,
        count_if(cdc_operation = 'u')                as updates,
        count_if(cdc_operation = 'd')                as deletes,
        count_if(cdc_operation = 'legacy')           as legacy_rows,
        max(source_committed_at_utc)                 as last_source_commit_utc,
        max(raw_loaded_at_utc)                       as last_raw_load_utc,
        median(ingest_latency_seconds)               as median_ingest_latency_seconds,
        max(ingest_latency_seconds)                  as max_ingest_latency_seconds
    from {{ ref('fct_cdc_events') }}
    group by source_table
),

marts as (
    select 'customers' as source_table, count(*) as mart_rows from {{ ref('dim_customers') }}
    union all
    select 'accounts', count(*) from {{ ref('dim_accounts') }}
    union all
    select 'transactions', count(*) from {{ ref('fact_transactions') }}
)

select
    e.*,
    m.mart_rows,
    (select max(transaction_time) from {{ ref('fact_transactions') }})   as latest_transaction_time_utc,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz           as built_at_utc
from events e
left join marts m
    on e.source_table = m.source_table
