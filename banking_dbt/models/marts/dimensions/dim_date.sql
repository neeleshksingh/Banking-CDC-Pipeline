{{ config(materialized='table') }}

-- Calendar dimension: one row per day, contiguous full years from the first
-- year with transactions through the end of next year (Power BI date table).

with bounds as (
    select
        date_trunc('year', coalesce(min(transaction_time)::date, current_date()))       as start_date,
        dateadd(day, -1, dateadd(year, 2, date_trunc('year', current_date())))          as end_date
    from {{ ref('stg_transactions') }}
),

spine as (
    -- row_number() instead of seq4() alone: seq4() is not guaranteed gap-free
    select
        dateadd(day, row_number() over (order by seq4()) - 1, b.start_date)  as date_day,
        b.end_date
    from table(generator(rowcount => 7320)), bounds b
)

select
    date_day                                         as date,
    to_number(to_char(date_day, 'YYYYMMDD'))         as date_key,
    year(date_day)                                   as year,
    quarter(date_day)                                as quarter_num,
    'Q' || quarter(date_day)                         as quarter_label,
    month(date_day)                                  as month_num,
    to_char(date_day, 'MMMM')                        as month_name,
    to_char(date_day, 'MON')                         as month_short,
    to_char(date_day, 'YYYY-MM')                     as year_month,
    year(date_day) * 100 + month(date_day)           as year_month_num,
    date_trunc('week', date_day)                     as week_start_date,
    weekiso(date_day)                                as iso_week,
    day(date_day)                                    as day_of_month,
    dayofweekiso(date_day)                           as day_of_week_num,
    dayname(date_day)                                as day_name,
    dayofweekiso(date_day) in (6, 7)                 as is_weekend
from spine
where date_day <= end_date
