import logging
from collections import namedtuple
from datetime import datetime, timezone

import kafka_to_minio as km
import pandas as pd
import pytest
from kafka import TopicPartition

Message = namedtuple("Message", "topic partition offset value")

CUSTOMERS = "banking_server.public.customers"


def ms(*args):
    return int(datetime(*args, tzinfo=timezone.utc).timestamp() * 1000)


def envelope(op, before=None, after=None, ts_ms=1_700_000_000_000, source=None):
    return {
        "op": op,
        "before": before,
        "after": after,
        "ts_ms": ts_ms,
        "source": source if source is not None else {"ts_ms": ts_ms - 5, "lsn": 123, "txId": 9},
    }


def msg(offset, event, topic=CUSTOMERS, partition=0):
    return Message(topic, partition, offset, event)


# ------------------------------------------------------------
# to_record
# ------------------------------------------------------------

@pytest.mark.parametrize("op", ["c", "r", "u"])
def test_to_record_uses_after_image_for_create_read_update(op):
    event = envelope(op, before={"id": 1, "email": "old"}, after={"id": 1, "email": "new"})
    record = km.to_record(msg(7, event), event)
    assert record["email"] == "new"
    assert record["_cdc_operation"] == op


def test_to_record_uses_before_image_for_delete():
    event = envelope("d", before={"id": 1, "email": "gone"}, after=None)
    record = km.to_record(msg(7, event), event)
    assert record["email"] == "gone"
    assert record["_cdc_operation"] == "d"


def test_to_record_without_row_image_returns_none_and_warns(caplog):
    event = envelope("u", after=None)
    with caplog.at_level(logging.WARNING, logger="kafka_to_minio"):
        assert km.to_record(msg(7, event), event) is None
    assert "no row image" in caplog.text


def test_to_record_unknown_operation_returns_none(caplog):
    event = envelope("t", after={"id": 1})
    with caplog.at_level(logging.WARNING, logger="kafka_to_minio"):
        assert km.to_record(msg(7, event), event) is None
    assert "Unknown Debezium operation" in caplog.text


def test_to_record_unwraps_schemas_enable_true_envelope():
    inner = envelope("c", after={"id": 5})
    wrapped = {"schema": {"type": "struct"}, "payload": inner}
    record = km.to_record(msg(1, wrapped), wrapped)
    assert record["id"] == "5"
    assert record["_cdc_operation"] == "c"


def test_to_record_stringifies_values_and_keeps_nulls():
    event = envelope("c", after={"id": 5, "balance": "100.50", "active": True, "ratio": 0.5, "note": None})
    record = km.to_record(msg(1, event), event)
    assert record["id"] == "5"
    assert record["balance"] == "100.50"
    assert record["active"] == "True"
    assert record["ratio"] == "0.5"
    assert record["note"] is None


def test_to_record_adds_cdc_metadata():
    event = envelope("c", after={"id": 5}, ts_ms=1000, source={"ts_ms": 900, "lsn": 42, "txId": 7})
    record = km.to_record(msg(11, event, partition=2), event)
    assert record["_cdc_ts_ms"] == "1000"
    assert record["_source_ts_ms"] == "900"
    assert record["_source_lsn"] == "42"
    assert record["_source_tx_id"] == "7"
    assert record["_kafka_topic"] == CUSTOMERS
    assert record["_kafka_partition"] == "2"
    assert record["_kafka_offset"] == "11"


def test_to_record_missing_source_metadata_is_null():
    event = {"op": "c", "after": {"id": 1}}
    record = km.to_record(msg(1, event), event)
    assert record["_cdc_ts_ms"] is None
    assert record["_source_ts_ms"] is None
    assert record["_source_lsn"] is None


# ------------------------------------------------------------
# event_date
# ------------------------------------------------------------

def test_event_date_prefers_source_ts():
    record = {"_source_ts_ms": str(ms(2025, 3, 1, 12)), "_cdc_ts_ms": str(ms(2025, 3, 2, 12))}
    assert km.event_date(record) == "2025-03-01"


def test_event_date_falls_back_to_cdc_ts():
    record = {"_source_ts_ms": None, "_cdc_ts_ms": str(ms(2025, 3, 2, 12))}
    assert km.event_date(record) == "2025-03-02"


def test_event_date_falls_back_to_now(monkeypatch):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2030, 7, 4, 1, 2, 3, tzinfo=tz)

    monkeypatch.setattr(km, "datetime", FrozenDatetime)
    assert km.event_date({"_source_ts_ms": None, "_cdc_ts_ms": None}) == "2030-07-04"


# ------------------------------------------------------------
# write_to_minio
# ------------------------------------------------------------

class FakeS3:
    def __init__(self, calls=None, fail=False):
        self.calls = calls if calls is not None else []
        self.fail = fail
        self.uploads = {}  # key -> DataFrame read back before the temp file is deleted

    def upload_file(self, filename, bucket, key):
        self.calls.append(("upload", bucket, key))
        if self.fail:
            raise ConnectionError("MinIO is down")
        self.uploads[key] = pd.read_parquet(filename, engine="fastparquet")

    def list_buckets(self):
        return {"Buckets": [{"Name": "other"}]}

    def create_bucket(self, Bucket):
        self.calls.append(("create_bucket", Bucket))


def test_write_to_minio_splits_batch_that_straddles_midnight():
    s3 = FakeS3()
    records = [
        {"id": "1", "_source_ts_ms": str(ms(2025, 1, 1, 23, 59, 59)), "_cdc_ts_ms": None},
        {"id": "2", "_source_ts_ms": str(ms(2025, 1, 2, 0, 0, 1)), "_cdc_ts_ms": None},
        {"id": "3", "_source_ts_ms": str(ms(2025, 1, 2, 0, 0, 2)), "_cdc_ts_ms": None},
    ]
    km.write_to_minio(s3, "raw", "customers", records)

    assert len(s3.uploads) == 2
    by_date = {key.split("/")[1]: df for key, df in s3.uploads.items()}
    assert set(by_date) == {"date=2025-01-01", "date=2025-01-02"}
    assert by_date["date=2025-01-01"]["id"].tolist() == ["1"]
    assert by_date["date=2025-01-02"]["id"].tolist() == ["2", "3"]
    assert all(key.startswith("customers/") and key.endswith(".parquet") for key in s3.uploads)
    assert all(call[1] == "raw" for call in s3.calls)


def test_write_to_minio_without_records_uploads_nothing():
    s3 = FakeS3()
    km.write_to_minio(s3, "raw", "customers", [])
    assert s3.calls == []


def test_ensure_bucket_creates_missing_bucket():
    s3 = FakeS3()
    km.ensure_bucket(s3, "raw")
    km.ensure_bucket(s3, "other")
    assert s3.calls == [("create_bucket", "raw")]


# ------------------------------------------------------------
# offset_meta
# ------------------------------------------------------------

def test_offset_meta_with_two_field_signature(monkeypatch):
    old = namedtuple("OffsetAndMetadata", "offset metadata")
    monkeypatch.setattr(km, "OffsetAndMetadata", old)
    assert km.offset_meta(42) == old(42, None)


def test_offset_meta_with_three_field_signature(monkeypatch):
    new = namedtuple("OffsetAndMetadata", "offset metadata leader_epoch")
    monkeypatch.setattr(km, "OffsetAndMetadata", new)
    assert km.offset_meta(42) == new(42, None, -1)


# ------------------------------------------------------------
# MinioLander: buffering, flush, commit-after-write
# ------------------------------------------------------------

class FakeConsumer:
    def __init__(self, calls, batches=()):
        self.calls = calls
        self.batches = list(batches)

    def commit(self, offsets):
        self.calls.append(("commit", offsets))

    def seek(self, tp, offset):
        self.calls.append(("seek", tp, offset))

    def poll(self, timeout_ms, max_records):
        return self.batches.pop(0) if self.batches else {}


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def make_lander(fail=False, batches=(), batch_size=500, flush_seconds=10):
    calls = []
    sleeps = []
    clock = FakeClock()
    lander = km.MinioLander(
        FakeConsumer(calls, batches), FakeS3(calls, fail=fail), "raw",
        batch_size=batch_size, flush_seconds=flush_seconds,
        sleep=sleeps.append, clock=clock,
    )
    return lander, calls, sleeps, clock


def create(offset, customer_id):
    return msg(offset, envelope("c", after={"id": customer_id}))


def test_flush_uploads_then_commits_last_offset_plus_one():
    lander, calls, sleeps, _ = make_lander()
    lander.add(create(10, 1))
    lander.add(create(11, 2))

    lander.flush("customers")

    kinds = [c[0] for c in calls]
    assert kinds == ["upload", "commit"]  # commit only after the upload
    tp = TopicPartition(CUSTOMERS, 0)
    assert calls[1][1] == {tp: km.offset_meta(12)}
    assert lander.buffers == {}
    assert sleeps == []


def test_flush_failure_does_not_commit_and_seeks_back():
    lander, calls, sleeps, _ = make_lander(fail=True)
    lander.add(create(10, 1))
    lander.add(create(11, 2))

    lander.flush("customers")

    assert not any(c[0] == "commit" for c in calls)
    assert ("seek", TopicPartition(CUSTOMERS, 0), 10) in calls
    assert lander.buffers == {}  # cleared: the events are re-read from Kafka
    assert sleeps == [km.RETRY_BACKOFF_SECONDS]


def test_flush_failure_is_logged_as_error_with_traceback(caplog):
    lander, _, _, _ = make_lander(fail=True)
    lander.add(create(10, 1))
    with caplog.at_level(logging.ERROR, logger="kafka_to_minio"):
        lander.flush("customers")
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and errors[0].exc_info is not None


def test_skipped_events_still_advance_committed_offsets():
    lander, calls, _, _ = make_lander()
    lander.add(msg(5, None))  # tombstone
    lander.add(msg(6, envelope("u", after=None)))  # no row image

    lander.flush("customers")

    assert not any(c[0] == "upload" for c in calls)
    assert calls == [("commit", {TopicPartition(CUSTOMERS, 0): km.offset_meta(7)})]


def test_flush_of_unknown_table_is_a_no_op():
    lander, calls, _, _ = make_lander()
    lander.flush("accounts")
    assert calls == []


def test_flush_due_by_batch_size_and_by_age():
    lander, calls, _, clock = make_lander(batch_size=2, flush_seconds=10)
    lander.add(create(1, 1))
    lander.flush_due()
    assert calls == []  # 1 record, 0 s old

    lander.add(create(2, 2))
    lander.flush_due()
    assert [c[0] for c in calls] == ["upload", "commit"]  # batch size reached

    calls.clear()
    lander.add(create(3, 3))
    clock.now += 10
    lander.flush_due()
    assert [c[0] for c in calls] == ["upload", "commit"]  # flush interval reached


def test_run_polls_until_stopped_then_flushes_remaining_buffers():
    tp = TopicPartition(CUSTOMERS, 0)
    lander, calls, _, _ = make_lander(batches=[{tp: [create(1, 1)]}])

    def poll_then_stop(timeout_ms, max_records):
        batch = FakeConsumer.poll(lander.consumer, timeout_ms, max_records)
        if not batch:
            lander.stop()  # what the SIGTERM / SIGINT handler does
        return batch

    lander.consumer.poll = poll_then_stop
    lander.run()

    assert [c[0] for c in calls] == ["upload", "commit"]
    assert lander.buffers == {}
