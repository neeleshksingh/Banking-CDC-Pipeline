import json
import logging
import os
import signal
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

log = logging.getLogger("kafka_to_minio")

# ============================================================
# Configuration (read in main(); nothing connects at import time)
# ============================================================

ENV_FILE = Path(__file__).resolve().parent / ".env"

TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]

# A file is written per table when either limit is reached.
DEFAULT_BATCH_SIZE = 500
DEFAULT_FLUSH_SECONDS = 10.0
RETRY_BACKOFF_SECONDS = 5


# ============================================================
# Kafka helpers
# ============================================================

def deserialize(raw):
    # Tombstones (value=None) are disabled in the connector, but be defensive.
    return json.loads(raw.decode("utf-8")) if raw else None


def offset_meta(offset):
    # kafka-python >= 2.1 added a leader_epoch field to OffsetAndMetadata.
    if len(OffsetAndMetadata._fields) == 3:
        return OffsetAndMetadata(offset, None, -1)
    return OffsetAndMetadata(offset, None)


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
        log.warning("Unknown Debezium operation: %s | %s | offset=%s", operation, message.topic, message.offset)
        return None

    if not row:
        log.warning("%s event has no row image | %s | offset=%s", str(operation).upper(), message.topic, message.offset)
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

def write_to_minio(s3, bucket, table_name, records):

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
                bucket,
                s3_key
            )

            log.info("MinIO upload successful | %d record(s) | s3://%s/%s", len(rows), bucket, s3_key)

        finally:

            if os.path.exists(temp_file):
                os.remove(temp_file)


# ============================================================
# Buffering + commit-after-write
# ============================================================

class MinioLander:
    """Buffers CDC events per table and lands them in MinIO.

    The Kafka consumer and S3 client are passed in, so tests can use fakes.
    """

    def __init__(self, consumer, s3, bucket, batch_size=DEFAULT_BATCH_SIZE,
                 flush_seconds=DEFAULT_FLUSH_SECONDS, retry_backoff=RETRY_BACKOFF_SECONDS,
                 sleep=time.sleep, clock=time.monotonic):
        self.consumer = consumer
        self.s3 = s3
        self.bucket = bucket
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.retry_backoff = retry_backoff
        self.sleep = sleep
        self.clock = clock
        # table -> {"records": [...], "first_offsets": {tp: offset}, "last_offsets": {tp: offset}, "since": t}
        self.buffers = {}
        self.stopping = False

    def buffer_for(self, table_name):
        if table_name not in self.buffers:
            self.buffers[table_name] = {"records": [], "first_offsets": {}, "last_offsets": {}, "since": None}
        return self.buffers[table_name]

    def add(self, message):
        table_name = message.topic.split(".")[-1]
        buf = self.buffer_for(table_name)
        tp_key = TopicPartition(message.topic, message.partition)

        # Track offsets even for skipped events so they get committed too.
        buf["first_offsets"].setdefault(tp_key, message.offset)
        buf["last_offsets"][tp_key] = message.offset
        if buf["since"] is None:
            buf["since"] = self.clock()

        if message.value is None:
            return

        record = to_record(message, message.value)
        if record is None:
            return

        buf["records"].append(record)
        log.debug("%s | %s | ID=%s", record["_cdc_operation"].upper(), message.topic, record.get("id"))

    def flush(self, table_name):
        buf = self.buffers.get(table_name)
        if not buf or not buf["last_offsets"]:
            return

        try:

            write_to_minio(self.s3, self.bucket, table_name, buf["records"])

            # ----------------------------------------------------
            # IMPORTANT:
            # Commit ONLY after MinIO upload succeeds.
            # ----------------------------------------------------
            self.consumer.commit({
                tp: offset_meta(offset + 1)
                for tp, offset in buf["last_offsets"].items()
            })

            for tp, offset in buf["last_offsets"].items():
                log.info("Kafka offset committed | %s | partition=%s | offset=%d", tp.topic, tp.partition, offset + 1)

        except Exception:

            log.exception("Flush failed for %s", table_name)  # ERROR level, with traceback

            # Rewind so the same events are re-read; nothing is skipped.
            for tp, offset in buf["first_offsets"].items():
                self.consumer.seek(tp, offset)

            log.warning(
                "Kafka offset NOT committed. Rewound %s and retrying in %ss.", table_name, self.retry_backoff
            )
            self.sleep(self.retry_backoff)

        finally:

            self.buffers.pop(table_name, None)

    def flush_due(self):
        now = self.clock()
        for table_name in list(self.buffers):
            buf = self.buffers[table_name]
            if len(buf["records"]) >= self.batch_size or (
                buf["since"] is not None and now - buf["since"] >= self.flush_seconds
            ):
                self.flush(table_name)

    def flush_all(self):
        for table_name in list(self.buffers):
            self.flush(table_name)

    def poll_once(self):
        polled = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
        for messages in polled.values():
            for message in messages:
                self.add(message)
        self.flush_due()

    def stop(self, *_):
        self.stopping = True

    def run(self):
        """Poll until stop() is called, then flush whatever is still buffered."""
        try:
            while not self.stopping:
                self.poll_once()
        except KeyboardInterrupt:
            log.info("Interrupted by user.")
        log.info("Shutting down. Flushing buffered events...")
        self.flush_all()


# ============================================================
# Wiring
# ============================================================

def make_consumer(bootstrap, group_id):
    return KafkaConsumer(
        *TOPICS,

        bootstrap_servers=bootstrap,

        auto_offset_reset="earliest",

        # IMPORTANT:
        # Kafka offsets are committed manually AFTER MinIO succeeds.
        enable_auto_commit=False,

        group_id=group_id,

        value_deserializer=deserialize,
    )


def make_s3(endpoint, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def ensure_bucket(s3, bucket):
    if bucket not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=bucket)
        log.info("Created bucket: %s", bucket)


def main():
    # Variables already set in the environment (e.g. by docker-compose) win over consumer/.env.
    load_dotenv(ENV_FILE)
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    kafka_group = os.getenv("KAFKA_GROUP")
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    bucket = os.getenv("MINIO_BUCKET")
    batch_size = int(os.getenv("BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
    flush_seconds = float(os.getenv("FLUSH_SECONDS", str(DEFAULT_FLUSH_SECONDS)))

    log.info(
        "Kafka -> MinIO CDC consumer | kafka=%s group=%s minio=%s bucket=%s batch=%d records / %ss",
        kafka_bootstrap, kafka_group, minio_endpoint, bucket, batch_size, flush_seconds,
    )

    s3 = make_s3(minio_endpoint, os.getenv("MINIO_ACCESS_KEY"), os.getenv("MINIO_SECRET_KEY"))
    ensure_bucket(s3, bucket)

    consumer = make_consumer(kafka_bootstrap, kafka_group)
    lander = MinioLander(consumer, s3, bucket, batch_size=batch_size, flush_seconds=flush_seconds)

    # `docker stop` sends SIGTERM; Ctrl+C sends SIGINT. Both stop the loop
    # after the current poll, then buffered events are flushed and committed.
    signal.signal(signal.SIGTERM, lander.stop)
    signal.signal(signal.SIGINT, lander.stop)

    log.info("Listening for CDC events...")
    try:
        lander.run()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
