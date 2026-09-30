{#
    Use a model's custom schema exactly as written (STAGING, ANALYTICS)
    instead of dbt's default "<target_schema>_<custom_schema>".
    This keeps local runs, Airflow and CI/CD writing to the same schemas
    regardless of the schema set in each profile.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
