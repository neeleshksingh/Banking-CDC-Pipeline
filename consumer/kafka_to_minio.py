import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

# ============================================================
# Load consumer/.env
# ============================================================

ENV_FILE = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_FILE)

print(f"✅ Loaded environment: {ENV_FILE}")


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]

# Keep this at 1 for our first end-to-end test.
# Later change to 50.
BATCH_SIZE = 1


# ============================================================
# Kafka Consumer
# ============================================================

consumer = KafkaConsumer(
    *TOPICS,

    bootstrap_servers=KAFKA_BOOTSTRAP,

    auto_offset_reset="earliest",

    # IMPORTANT:
    # Kafka offsets are committed manually AFTER MinIO succeeds.
    enable_auto_commit=False,

    group_id=KAFKA_GROUP,

    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    ),
)


# ============================================================
# MinIO Client
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

bucket_names = [
    bucket["Name"]
    for bucket in s3.list_buckets()["Buckets"]
]

if MINIO_BUCKET not in bucket_names:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"✅ Created bucket: {MINIO_BUCKET}")


# ============================================================
# Write records to MinIO
# ============================================================

def write_to_minio(table_name, records):

    if not records:
        return

    df = pd.DataFrame(records)

    date_str = datetime.now().astimezone().strftime("%Y-%m-%d")
    timestamp = datetime.now().astimezone().strftime("%H%M%S%f")

    file_name = f"{table_name}_{timestamp}.parquet"

    s3_key = (
        f"{table_name}/"
        f"date={date_str}/"
        f"{file_name}"
    )

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        delete=False
    ) as tmp:

        temp_file = tmp.name

    try:

        df.to_parquet(
            temp_file,
            engine="fastparquet",
            index=False
        )

        s3.upload_file(
            temp_file,
            MINIO_BUCKET,
            s3_key
        )

        print(
            f"✅ MinIO upload successful | "
            f"{len(records)} record(s) | "
            f"s3://{MINIO_BUCKET}/{s3_key}"
        )

    finally:

        if os.path.exists(temp_file):
            os.remove(temp_file)


# ============================================================
# Start
# ============================================================

print("============================================")
print("Kafka → MinIO CDC Consumer")
print("============================================")
print(f"Kafka     : {KAFKA_BOOTSTRAP}")
print(f"Group     : {KAFKA_GROUP}")
print(f"MinIO     : {MINIO_ENDPOINT}")
print(f"Bucket    : {MINIO_BUCKET}")
print(f"Batch     : {BATCH_SIZE}")
print("============================================")
print("✅ Listening for CDC events...")


# ============================================================
# Main loop
# ============================================================

for message in consumer:

    topic = message.topic
    event = message.value

    operation = event.get("op")

    before = event.get("before")
    after = event.get("after")


    # --------------------------------------------------------
    # CREATE / SNAPSHOT / UPDATE
    # --------------------------------------------------------

    if operation in ("c", "r", "u"):

        record = after

        if not record:
            print(
                f"⚠️ {operation.upper()} event has no 'after'"
            )
            continue

        record["_cdc_operation"] = operation

        print(
            f"📥 {operation.upper()} | "
            f"{topic} | "
            f"ID={record.get('id')}"
        )


    # --------------------------------------------------------
    # DELETE
    # --------------------------------------------------------

    elif operation == "d":

        record = before

        if not record:
            print(
                "⚠️ DELETE event has no 'before'"
            )
            continue

        record["_cdc_operation"] = "d"

        print(
            f"🗑️ DELETE | "
            f"{topic} | "
            f"ID={record.get('id')}"
        )


    # --------------------------------------------------------
    # Unknown operation
    # --------------------------------------------------------

    else:

        print(
            f"⚠️ Unknown Debezium operation: {operation}"
        )

        continue


    # --------------------------------------------------------
    # Write to MinIO
    # --------------------------------------------------------

    table_name = topic.split(".")[-1]

    try:

        write_to_minio(
            table_name,
            [record]
        )

        # ----------------------------------------------------
        # IMPORTANT:
        # Commit ONLY after MinIO upload succeeds.
        # ----------------------------------------------------

        partition = TopicPartition(
            message.topic,
            message.partition
        )

        offset = OffsetAndMetadata(
            message.offset + 1,
            None
        )

        consumer.commit({
            partition: offset
        })

        print(
            f"✅ Kafka offset committed | "
            f"{topic} | "
            f"partition={message.partition} | "
            f"offset={message.offset + 1}"
        )

    except Exception as e:  # noqa: BLE001

        print(
            f"❌ MinIO write failed: {e}"
        )

        print(
            "⚠️ Kafka offset NOT committed. "
            "This message will be retried."
        )