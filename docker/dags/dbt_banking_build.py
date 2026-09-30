from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

# Same URI as the outlet in minio_to_snowflake_dag.py: this DAG runs
# whenever new data has been loaded into Snowflake RAW.
RAW_DATASET = Dataset("snowflake://banking/raw")

# Keep Airflow's dbt artifacts out of the host-mounted project folder so they
# don't clash with local dbt runs (different dbt versions / partial parse).
DBT = (
    "cd /opt/airflow/banking_dbt && dbt {cmd} "
    "--profiles-dir /home/airflow/.dbt "
    "--target-path /tmp/dbt_target "
    "--log-path /tmp/dbt_logs"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_banking_build",
    default_args=default_args,
    description="dbt build: staging -> SCD2 snapshots -> marts -> tests",
    schedule=[RAW_DATASET],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "snapshots"],
) as dag:

    # Runs models, snapshots and tests in dependency order;
    # a failing test skips everything downstream of it.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT.format(cmd="build"),
    )

    # Freshness is informational: it must not block the build.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=DBT.format(cmd="source freshness"),
        retries=0,
    )

    dbt_build >> dbt_source_freshness
