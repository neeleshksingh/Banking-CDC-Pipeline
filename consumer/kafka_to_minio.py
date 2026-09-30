import json
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
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

# A file is written per table when either limit is reached.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "10"))
RETRY_BACKOFF_SECONDS = 5


# ============================================================
# Kafka Consumer
# ============================================================

def deserialize(raw):
    # Tombstones (value=None) are disabled in the connector, but be defensive.
    return json.loads(raw.decode("utf-8")) if raw else None


consumer = KafkaConsumer(
    *TOPICS,

    bootstrap_servers=KAFKA_BOOTSTRAP,

    auto_offset_reset="earliest",

    # IMPORTANT:
    # Kafka offsets are committed manually AFTER MinIO succeeds.
    enable_auto_commit=False,

    group_id=KAFKA_GROUP,

    value_deserializer=deserialize,
)


def offset_meta(offset):
    # kafka-python >= 2.1 added a leader_epoch field to OffsetAndMetadata.
    if len(OffsetAndMetadata._fields) == 3:
        return OffsetAndMetadata(offset, None, -1)
    return OffsetAndMetadata(offset, None)


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
# Event -> flat record
# ============================================================

def to_record(message, event):
    """Flatten a Debezium envelope into one row with CDC metadata.

    Returns None for events that carry no row image.
    """
    event = event.get("payload", event)  # tolerate schemas.enable=true
    operation = event.get("op")

    if operation in ("c", "r", "u"):
        row = event.get("after")
    elif operation == "d":
        row = event.get("before")
    else:
        print(f"⚠️ Unknown Debezium operation: {operation}")
        return None

    if not row:
        print(f"⚠️ {str(operation).upper()} event has no row image | {message.topic} | offset={message.offset}")
        return None

    source = event.get("source") or {}

    # All values are written as strings (NULLs stay NULL): Parquet columns get a
    # stable type across batches, and dbt casts them to proper types in staging.
    record = {k: (None if v is None else str(v)) for k, v in row.items()}
    record.update({
        "_cdc_operation": operation,
        "_cdc_ts_ms": str(event.get("ts_ms")) if event.get("ts_ms") is not None else None,
        "_source_ts_ms": str(source.get("ts_ms")) if source.get("ts_ms") is not None else None,
        "_source_lsn": str(source.get("lsn")) if source.get("lsn") is not None else None,
        "_source_tx_id": str(source.get("txId")) if source.get("txId") is not None else None,
        "_kafka_topic": message.topic,
        "_kafka_partition": str(message.partition),
        "_kafka_offset": str(message.offset),
    })
    return record


def event_date(record):
    ts_ms = record.get("_source_ts_ms") or record.get("_cdc_ts_ms")
    ts = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc) if ts_ms else datetime.now(timezone.utc)
    return ts.strftime("%Y-%m-%d")


# ============================================================
# Write records to MinIO
# ============================================================

def write_to_minio(table_name, records):

    if not records:
        return

    # Partition by event date (UTC); a batch may straddle midnight.
    by_date = {}
    for record in records:
        by_date.setdefault(event_date(record), []).append(record)

    for date_str, rows in by_date.items():

        df = pd.DataFrame(rows, dtype=object)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        file_name = f"{table_name}_{stamp}_{uuid.uuid4().hex[:6]}.parquet"

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
                f"{len(rows)} record(s) | "
                f"s3://{MINIO_BUCKET}/{s3_key}"
            )

        finally:

            if os.path.exists(temp_file):
                os.remove(temp_file)


# ============================================================
# Buffering + commit-after-write
# ============================================================

# table -> {"records": [...], "first_offsets": {tp: offset}, "last_offsets": {tp: offset}, "since": t}
buffers = {}


def buffer_for(table_name):
    if table_name not in buffers:
        buffers[table_name] = {"records": [], "first_offsets": {}, "last_offsets": {}, "since": None}
    return buffers[table_name]


def flush(table_name):
    buf = buffers.get(table_name)
    if not buf or not buf["last_offsets"]:
        return

    try:

        write_to_minio(table_name, buf["records"])

        # ----------------------------------------------------
        # IMPORTANT:
        # Commit ONLY after MinIO upload succeeds.
        # ----------------------------------------------------
        consumer.commit({
            tp: offset_meta(offset + 1)
            for tp, offset in buf["last_offsets"].items()
        })

        for tp, offset in buf["last_offsets"].items():
            print(
                f"✅ Kafka offset committed | "
                f"{tp.topic} | "
                f"partition={tp.partition} | "
                f"offset={offset + 1}"
            )

    except Exception as e:  # noqa: BLE001

        print(f"❌ Flush failed for {table_name}: {e}")

        # Rewind so the same events are re-read; nothing is skipped.
        for tp, offset in buf["first_offsets"].items():
            consumer.seek(tp, offset)

        print(
            "⚠️ Kafka offset NOT committed. "
            f"Rewound {table_name} and retrying in {RETRY_BACKOFF_SECONDS}s."
        )
        time.sleep(RETRY_BACKOFF_SECONDS)

    finally:

        buffers.pop(table_name, None)


def flush_due():
    now = time.monotonic()
    for table_name in list(buffers):
        buf = buffers[table_name]
        if len(buf["records"]) >= BATCH_SIZE or (buf["since"] is not None and now - buf["since"] >= FLUSH_SECONDS):
            flush(table_name)


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
print(f"Batch     : {BATCH_SIZE} records / {FLUSH_SECONDS}s")
print("============================================")
print("✅ Listening for CDC events...")


# ============================================================
# Main loop
# ============================================================

try:
    while True:

        polled = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)

        for tp, messages in polled.items():
            table_name = tp.topic.split(".")[-1]

            for message in messages:
                buf = buffer_for(table_name)
                tp_key = TopicPartition(message.topic, message.partition)

                # Track offsets even for skipped events so they get committed too.
                buf["first_offsets"].setdefault(tp_key, message.offset)
                buf["last_offsets"][tp_key] = message.offset
                if buf["since"] is None:
                    buf["since"] = time.monotonic()

                if message.value is None:
                    continue

                record = to_record(message, message.value)
                if record is None:
                    continue

                buf["records"].append(record)
                print(
                    f"📥 {record['_cdc_operation'].upper()} | "
                    f"{message.topic} | "
                    f"ID={record.get('id')}"
                )

        flush_due()

except KeyboardInterrupt:
    print("\nInterrupted by user. Flushing buffered events...")
    for table_name in list(buffers):
        flush(table_name)

finally:
    consumer.close()
