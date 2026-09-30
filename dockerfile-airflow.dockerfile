# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to airflow user first
USER airflow

# Install dbt packages (pinned to the versions verified with this project)
RUN pip install --no-cache-dir dbt-core==1.12.5 dbt-snowflake==1.12.1
