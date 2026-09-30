{#
    Idempotently create the RAW landing tables and their load-audit columns.
    Mirrors the DDL in docker/dags/minio_to_snowflake_dag.py so dbt can run
    even before the ingestion DAG has loaded anything with the new layout.
#}
{% macro ensure_raw_tables() %}
    {% if execute %}
        {% do run_query("create schema if not exists BANKING.RAW") %}
        {% for table in ['CUSTOMERS', 'ACCOUNTS', 'TRANSACTIONS'] %}
            {% do run_query("create table if not exists BANKING.RAW." ~ table ~ " (v variant)") %}
            {% do run_query("alter table BANKING.RAW." ~ table ~ " add column if not exists _file_name varchar") %}
            {% do run_query("alter table BANKING.RAW." ~ table ~ " add column if not exists _loaded_at timestamp_ltz") %}
        {% endfor %}
    {% endif %}
    select 1
{% endmacro %}
