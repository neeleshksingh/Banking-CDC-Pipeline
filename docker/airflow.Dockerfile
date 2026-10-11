FROM apache/airflow:2.9.3

USER airflow

# Keep in sync with requirements.txt and the CI / CD workflows
RUN pip install --no-cache-dir dbt-core==1.12.5 dbt-snowflake==1.12.1
