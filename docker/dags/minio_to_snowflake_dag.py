import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import snowflake.connector
from airflow import DAG
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Load environment variables
DAG_DIR = Path(__file__).resolve().parent
ENV_FILE = DAG_DIR / ".env"

load_dotenv(ENV_FILE, override=True)

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]

# Emitted when new RAW data lands; triggers the dbt DAG.
RAW_DATASET = Dataset("snowflake://banking/raw")

# Per-table watermark of MinIO LastModified, plus the keys seen inside the
# look-back window (uploads can finish slightly out of order).
WATERMARK_VAR = "minio_to_snowflake_watermarks"
LOOKBACK = timedelta(minutes=10)
COPY_FILES_PER_STATEMENT = 1000  # Snowflake limit for FILES = (...)


# -------- Python Callables --------
def discover_and_download(**context):
    """Download only MinIO objects not loaded yet. Skips the run if none."""
    state = Variable.get(WATERMARK_VAR, default_var={}, deserialize_json=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    paginator = s3.get_paginator("list_objects_v2")

    run_dir = Path(LOCAL_DIR) / context["run_id"].replace(":", "_").replace("+", "_")
    files, new_state, total = {}, {}, 0

    for table in TABLES:
        table_state = state.get(table, {})
        watermark = (
            datetime.fromisoformat(table_state["watermark"]) if table_state.get("watermark") else None
        )
        seen_recent = set(table_state.get("recent_keys", []))
        since = watermark - LOOKBACK if watermark else None

        new_objects, window = [], []
        for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{table}/"):
            for obj in page.get("Contents", []):
                key, modified = obj["Key"], obj["LastModified"]
                if not key.endswith(".parquet"):
                    continue
                if since and modified < since:
                    continue
                window.append((key, modified))
                if key not in seen_recent:
                    new_objects.append(key)

        files[table] = []
        for key in new_objects:
            # Keep the plain file name: the stage file name must stay identical to
            # earlier loads so Snowflake's load history skips already-loaded files.
            local_file = run_dir / table / os.path.basename(key)
            local_file.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(BUCKET, key, str(local_file))
            files[table].append(str(local_file))
        total += len(files[table])
        print(f"{table}: {len(files[table])} new file(s)")

        # Next state: newest LastModified seen, and all keys within the look-back window of it
        max_modified = max([m for _, m in window], default=watermark)
        new_state[table] = {
            "watermark": max_modified.isoformat() if max_modified else None,
            "recent_keys": sorted(k for k, m in window if max_modified and m >= max_modified - LOOKBACK),
        }

    if total == 0:
        # Nothing new: no Snowflake connection, so the warehouse can stay suspended.
        raise AirflowSkipException("No new files in MinIO.")

    return {"run_dir": str(run_dir), "files": files, "state": new_state}


def load_to_snowflake(**kwargs):
    payload = kwargs["ti"].xcom_pull(task_ids="discover_minio")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    try:
        for table, files in payload["files"].items():
            # RAW = append-only CDC event log + load audit columns
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (v VARIANT)")
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS _file_name VARCHAR")
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS _loaded_at TIMESTAMP_LTZ")

            if not files:
                print(f"No files for {table}, skipping.")
                continue

            staged = []
            for f in files:
                # PUT skips files that already exist in the stage.
                for row in cur.execute(f"PUT 'file://{f}' @%{table}").fetchall():
                    staged.append(row[1])  # target file name in the stage
            print(f"Uploaded {len(staged)} file(s) -> @%{table}")

            # Only the files from this run; Snowflake load metadata also
            # skips any file this table has already loaded (idempotent).
            for i in range(0, len(staged), COPY_FILES_PER_STATEMENT):
                chunk = ", ".join(f"'{name}'" for name in staged[i:i + COPY_FILES_PER_STATEMENT])
                results = cur.execute(f"""
                    COPY INTO {table} (v, _file_name, _loaded_at)
                    FROM (
                        SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
                        FROM @%{table}
                    )
                    FILES = ({chunk})
                    FILE_FORMAT = (TYPE = PARQUET)
                    ON_ERROR = 'SKIP_FILE'
                """).fetchall()
                for r in results:
                    print(f"COPY {table}: {r}")
                    # LOAD_SKIPPED = already loaded earlier (expected, idempotent)
                    if len(r) > 1 and r[1] in ("LOAD_FAILED", "PARTIALLY_LOADED"):
                        print(f"⚠️ {table}: file skipped due to errors -> {r}")

            print(f"Data loaded into {table}")
    finally:
        cur.close()
        conn.close()

    # Advance the watermark only after every table loaded successfully.
    Variable.set(WATERMARK_VAR, payload["state"], serialize_json=True)
    shutil.rmtree(payload["run_dir"], ignore_errors=True)


# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load new MinIO parquet files into Snowflake RAW tables",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "snowflake"],
) as dag:

    task1 = PythonOperator(
        task_id="discover_minio",
        python_callable=discover_and_download,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        outlets=[RAW_DATASET],
    )

    task1 >> task2
