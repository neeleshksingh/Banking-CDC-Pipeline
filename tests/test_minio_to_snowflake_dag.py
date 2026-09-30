from datetime import datetime, timedelta, timezone
from unittest import mock

import minio_to_snowflake_dag as dag
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

T0 = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)


def at(minutes):
    return T0 + timedelta(minutes=minutes)


# ------------------------------------------------------------
# plan_table: watermark / discovery (pure)
# ------------------------------------------------------------

def test_first_run_without_state_takes_every_parquet_file():
    objects = [("t/a.parquet", at(0)), ("t/b.parquet", at(5)), ("t/_SUCCESS", at(6))]

    new_keys, state = dag.plan_table(objects, {})

    assert new_keys == ["t/a.parquet", "t/b.parquet"]
    assert state == {"watermark": at(5).isoformat(), "recent_keys": ["t/a.parquet", "t/b.parquet"]}


def test_only_keys_not_seen_before_are_returned():
    state = {"watermark": at(0).isoformat(), "recent_keys": ["t/a.parquet"]}
    objects = [("t/a.parquet", at(0)), ("t/b.parquet", at(1))]

    new_keys, next_state = dag.plan_table(objects, state)

    assert new_keys == ["t/b.parquet"]
    assert next_state["watermark"] == at(1).isoformat()


def test_seen_keys_inside_lookback_are_not_reloaded():
    state = {"watermark": at(0).isoformat(), "recent_keys": ["t/a.parquet", "t/b.parquet"]}
    objects = [("t/a.parquet", at(-8)), ("t/b.parquet", at(0))]

    new_keys, next_state = dag.plan_table(objects, state)

    assert new_keys == []
    assert next_state == state


def test_keys_older_than_lookback_are_ignored_and_dropped_from_state():
    state = {"watermark": at(0).isoformat(), "recent_keys": ["t/b.parquet"]}
    objects = [("t/old.parquet", at(-30)), ("t/b.parquet", at(0)), ("t/c.parquet", at(20))]

    new_keys, next_state = dag.plan_table(objects, state)

    assert new_keys == ["t/c.parquet"]
    # b is now more than LOOKBACK behind the new watermark
    assert next_state == {"watermark": at(20).isoformat(), "recent_keys": ["t/c.parquet"]}


def test_out_of_order_last_modified_within_window_is_picked_up():
    # b finished uploading after the last run but has an older LastModified than a.
    state = {"watermark": at(0).isoformat(), "recent_keys": ["t/a.parquet"]}
    objects = [("t/a.parquet", at(0)), ("t/b.parquet", at(-5))]

    new_keys, next_state = dag.plan_table(objects, state)

    assert new_keys == ["t/b.parquet"]
    assert next_state == {"watermark": at(0).isoformat(), "recent_keys": ["t/a.parquet", "t/b.parquet"]}


def test_empty_listing_returns_nothing_and_keeps_watermark():
    state = {"watermark": at(0).isoformat(), "recent_keys": ["t/a.parquet"]}

    new_keys, next_state = dag.plan_table([], state)

    assert new_keys == []
    assert next_state["watermark"] == at(0).isoformat()


# ------------------------------------------------------------
# discover_and_download (boto3 + Variable faked)
# ------------------------------------------------------------

class FakePaginator:
    def __init__(self, listing):
        self.listing = listing

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [o for o in self.listing if o["Key"].startswith(Prefix)]}


class FakeS3:
    def __init__(self, listing):
        self.listing = listing
        self.downloads = []

    def get_paginator(self, name):
        return FakePaginator(self.listing)

    def download_file(self, bucket, key, filename):
        self.downloads.append(key)
        with open(filename, "w") as f:
            f.write("parquet")


@pytest.fixture
def minio(monkeypatch, tmp_path):
    def install(listing, state=None):
        s3 = FakeS3(listing)
        monkeypatch.setattr(dag.boto3, "client", lambda *a, **k: s3)
        monkeypatch.setattr(dag.Variable, "get", lambda *a, **k: state or {})
        monkeypatch.setattr(dag, "LOCAL_DIR", str(tmp_path))
        return s3
    return install


def test_empty_listing_skips_the_run(minio):
    minio([])
    with pytest.raises(dag.AirflowSkipException):
        dag.discover_and_download(run_id="manual__1")


def test_discover_downloads_new_files_and_returns_next_state(minio):
    s3 = minio([
        {"Key": "customers/date=2025-06-01/c1.parquet", "LastModified": at(0)},
        {"Key": "transactions/date=2025-06-01/t1.parquet", "LastModified": at(1)},
    ])

    payload = dag.discover_and_download(run_id="scheduled__2025-06-01T12:00:00+00:00")

    assert sorted(s3.downloads) == ["customers/date=2025-06-01/c1.parquet", "transactions/date=2025-06-01/t1.parquet"]
    assert [p.split("/")[-1] for p in payload["files"]["customers"]] == ["c1.parquet"]
    assert payload["files"]["accounts"] == []
    assert payload["state"]["transactions"]["watermark"] == at(1).isoformat()
    assert ":" not in payload["run_dir"].split("/")[-1]


# ------------------------------------------------------------
# load_to_snowflake: PUT / COPY result handling
# ------------------------------------------------------------

PUT_COLUMNS = ["source", "target", "source_size", "target_size", "source_compression",
               "target_compression", "status", "message"]
COPY_COLUMNS = ["file", "status", "rows_parsed", "rows_loaded", "error_limit", "errors_seen",
                "first_error", "first_error_line", "first_error_character", "first_error_column_name"]


def put_row(name, status="UPLOADED"):
    return (f"/tmp/{name}", name, 10, 10, "PARQUET", "PARQUET", status, "")


def copy_row(name, status, first_error=None):
    return (name, status, 1, 1 if status == "LOADED" else 0, 1, 0 if first_error is None else 1,
            first_error, None, None, None)


class FakeCursor:
    """Answers PUT and COPY from scripted results; other statements return nothing."""

    def __init__(self, put_status=None, copy_results=None):
        self.put_status = put_status or {}
        self.copy_results = copy_results or {}
        self.description = []
        self._rows = []
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)
        stripped = sql.strip()
        if stripped.startswith("PUT"):
            name = stripped.split("'")[1].split("/")[-1]
            self.description = [(c,) for c in PUT_COLUMNS]
            self._rows = [put_row(name, self.put_status.get(name, "UPLOADED"))]
        elif stripped.startswith("COPY"):
            table = stripped.split()[2]
            rows = self.copy_results.get(table, [])
            self.description = [(c,) for c in (COPY_COLUMNS if rows and len(rows[0]) > 1 else ["status"])]
            self._rows = rows
        else:
            self.description, self._rows = [("status",)], [("ok",)]
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        pass


@pytest.fixture
def snowflake(monkeypatch, tmp_path):
    def install(cursor, files):
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        monkeypatch.setattr(dag.snowflake.connector, "connect", mock.MagicMock(return_value=conn))
        monkeypatch.setattr(dag, "load_private_key", lambda *a, **k: b"der")
        variable_set = mock.MagicMock()
        monkeypatch.setattr(dag.Variable, "set", variable_set)
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        payload = {"run_dir": str(run_dir), "files": files, "state": {"customers": {"watermark": "w"}}}
        ti = mock.MagicMock()
        ti.xcom_pull.return_value = payload
        return ti, variable_set, run_dir
    return install


FILES = {"customers": ["/tmp/run/customers/c1.parquet", "/tmp/run/customers/c2.parquet"], "accounts": []}


def test_loaded_and_load_skipped_advance_the_watermark(snowflake):
    cursor = FakeCursor(copy_results={"customers": [copy_row("c1.parquet", "LOADED"),
                                                    copy_row("c2.parquet", "LOAD_SKIPPED")]})
    ti, variable_set, run_dir = snowflake(cursor, FILES)

    dag.load_to_snowflake(ti=ti)

    variable_set.assert_called_once_with(dag.WATERMARK_VAR, {"customers": {"watermark": "w"}}, serialize_json=True)
    assert not run_dir.exists()
    copy_sql = next(s for s in cursor.statements if s.strip().startswith("COPY"))
    assert "FILES = ('c1.parquet', 'c2.parquet')" in copy_sql
    assert "ON_ERROR = 'SKIP_FILE'" in copy_sql


def test_all_files_already_loaded_summary_row_is_ok(snowflake):
    cursor = FakeCursor(copy_results={"customers": [("Copy executed with 0 files processed.",)]})
    ti, variable_set, _ = snowflake(cursor, FILES)

    dag.load_to_snowflake(ti=ti)

    variable_set.assert_called_once()


@pytest.mark.parametrize("bad_status", ["LOAD_FAILED", "PARTIALLY_LOADED"])
def test_failed_copy_raises_and_does_not_advance_the_watermark(snowflake, bad_status):
    cursor = FakeCursor(copy_results={"customers": [
        copy_row("c1.parquet", "LOADED"),
        copy_row("c2.parquet", bad_status, first_error="Invalid parquet file"),
    ]})
    ti, variable_set, run_dir = snowflake(cursor, FILES)

    with pytest.raises(RuntimeError) as err:
        dag.load_to_snowflake(ti=ti)

    assert "c2.parquet" in str(err.value)
    assert bad_status in str(err.value)
    assert "Invalid parquet file" in str(err.value)
    variable_set.assert_not_called()
    assert run_dir.exists()  # kept for the Airflow retry


def test_failed_put_raises_and_does_not_advance_the_watermark(snowflake):
    cursor = FakeCursor(put_status={"c2.parquet": "ERROR"})
    ti, variable_set, _ = snowflake(cursor, FILES)

    with pytest.raises(RuntimeError, match="c2.parquet"):
        dag.load_to_snowflake(ti=ti)

    variable_set.assert_not_called()
    assert not any(s.strip().startswith("COPY") for s in cursor.statements)


def test_connects_with_private_key_and_role(snowflake, monkeypatch):
    monkeypatch.setattr(dag, "SNOWFLAKE_ROLE", "BANKING_PIPELINE_ROLE")
    cursor = FakeCursor(copy_results={"customers": [copy_row("c1.parquet", "LOADED"),
                                                    copy_row("c2.parquet", "LOADED")]})
    ti, _, _ = snowflake(cursor, FILES)

    dag.load_to_snowflake(ti=ti)

    kwargs = dag.snowflake.connector.connect.call_args.kwargs
    assert kwargs["private_key"] == b"der"
    assert kwargs["role"] == "BANKING_PIPELINE_ROLE"
    assert "password" not in kwargs


def test_copy_failures_names_each_failed_file():
    results = [
        {"file": "a.parquet", "status": "LOADED"},
        {"file": "b.parquet", "status": "LOAD_FAILED", "first_error": "boom"},
    ]
    assert dag.copy_failures("accounts", results) == ["accounts: b.parquet -> LOAD_FAILED: boom"]


def test_rows_as_dicts_maps_by_column_name():
    cursor = mock.MagicMock(description=[("FILE",), ("STATUS",)])
    assert dag.rows_as_dicts(cursor, [("a", "LOADED")]) == [{"file": "a", "status": "LOADED"}]


# ------------------------------------------------------------
# load_private_key
# ------------------------------------------------------------

def write_key(path, passphrase=None):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption = (serialization.BestAvailableEncryption(passphrase.encode())
                  if passphrase else serialization.NoEncryption())
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption))
    return key


def der(key):
    return key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def test_load_private_key_returns_der_bytes(tmp_path):
    key = write_key(tmp_path / "k.p8")
    assert dag.load_private_key(str(tmp_path / "k.p8")) == der(key)


def test_load_private_key_with_passphrase(tmp_path):
    key = write_key(tmp_path / "k.p8", passphrase="s3cret")
    assert dag.load_private_key(str(tmp_path / "k.p8"), "s3cret") == der(key)


def test_load_private_key_missing_file_has_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="Snowflake private key not found"):
        dag.load_private_key(str(tmp_path / "missing.p8"))


def test_load_private_key_requires_a_path():
    with pytest.raises(RuntimeError, match="SNOWFLAKE_PRIVATE_KEY_PATH"):
        dag.load_private_key(None)
