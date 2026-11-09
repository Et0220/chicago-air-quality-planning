"""S06 transfer, failure recovery, selection, and real SQL state integration."""

import base64
import hashlib
import io
import os
import uuid
import zipfile
from contextlib import closing, contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock

import pytest
import requests

from ingestion import db
from ingestion import epa_download as download
from ingestion.epa_manifest import ROOT, ManifestEntry, load_settings
from ingestion.epa_state import State

ENTRY = ManifestEntry(
    "hourly_88502_2026.zip",
    "pm25_non_frm_fem",
    2026,
    "https://aqs.epa.gov/aqsweb/airdata/hourly_88502_2026.zip",
    335702,
    2734080,
    "2026-06-25",
    "listed_nonempty",
)


@pytest.fixture
def archive():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as handle:
        handle.writestr("hourly_88502_2026.csv", "State Code,Value\n17,10\n")
    return buffer.getvalue()


def response(content, *, length=None):
    result = MagicMock()
    result.__enter__.return_value = result
    result.headers = {
        "ETag": '"version-1"',
        "Last-Modified": "Mon, 13 Jul 2026 15:47:57 GMT",
        "Content-Length": str(len(content) if length is None else length),
    }
    result.iter_content.return_value = [content[:3], content[3:]]
    return result


class MemoryBlob:
    """Only models GCS publication boundaries, checksums, and generation guards."""

    def __init__(self):
        self.name = "raw/epa/2026/hourly_88502_2026.zip"
        self.metadata = {}
        self.generation = None
        self.metageneration = 1
        self.size = 0
        self.md5_hash = None
        self.time_created = datetime.now(UTC)
        self.published = 0
        self.aborted = 0

    @contextmanager
    def open(self, mode, **kwargs):
        assert mode == "wb" and kwargs["checksum"] == "crc32c"
        assert kwargs["if_generation_match"] == (
            int(self.generation) if self.generation else 0
        )
        buffer = io.BytesIO()
        try:
            yield buffer
        except Exception:
            self.aborted += 1
            raise
        else:
            data = buffer.getvalue()
            self.size = len(data)
            self.md5_hash = base64.b64encode(hashlib.md5(data).digest()).decode()
            self.generation = str(int(self.generation or 0) + 1)
            self.published += 1

    def reload(self):
        pass

    def patch(self, **kwargs):
        assert kwargs["if_generation_match"] == int(self.generation)
        assert kwargs["if_metageneration_match"] == self.metageneration


def setup_transfer(archive):
    blob = MemoryBlob()
    bucket = Mock(name="bucket")
    bucket.name = "test-bucket"
    bucket.blob.return_value = blob
    bucket.get_blob.side_effect = lambda name: blob if blob.published else None
    session = Mock()
    session.head.side_effect = lambda *args, **kwargs: response(archive)
    session.get.side_effect = lambda *args, **kwargs: response(archive)
    state = Mock()
    state.lock.side_effect = lambda entry: nullcontext()
    state.register.return_value = (1, "pending", None, None)
    return blob, bucket, session, state


def test_stream_verifies_checksum_and_rerun_is_noop(archive):
    blob, bucket, session, state = setup_transfer(archive)
    run_id = uuid.uuid4()
    assert (
        download.download_one(ENTRY, state, bucket, session, run_id, "raw/epa")
        == "downloaded"
    )
    artifact = state.downloaded.call_args.args[1]
    assert artifact.md5 == hashlib.md5(archive).hexdigest()
    assert artifact.size == len(archive)
    assert blob.metadata["download_md5_hex"] == artifact.md5
    state.register.return_value = (1, "downloaded", artifact.uri, artifact.generation)
    assert (
        download.download_one(ENTRY, state, bucket, session, run_id, "raw/epa")
        == "unchanged"
    )
    assert blob.published == session.get.call_count == 1


def test_upload_success_then_db_failure_reconciles_without_redownload(archive):
    blob, bucket, session, state = setup_transfer(archive)
    state.downloaded.side_effect = [RuntimeError("database disconnected"), None]
    with pytest.raises(RuntimeError, match="database disconnected"):
        download.download_one(ENTRY, state, bucket, session, uuid.uuid4(), "raw/epa")
    state.failed.assert_called_once()
    state.register.return_value = (1, "failed", None, None)
    assert (
        download.download_one(ENTRY, state, bucket, session, uuid.uuid4(), "raw/epa")
        == "reconciled"
    )
    assert blob.published == session.get.call_count == 1


@pytest.mark.parametrize("status", ["listed_empty", "missing"])
def test_empty_or_missing_entries_never_contact_source_or_gcs(status):
    state = Mock()
    state.lock.return_value = nullcontext()
    state.register.return_value = (1, "skipped", None, None)
    bucket, session = Mock(), Mock()
    result = download.download_one(
        replace(ENTRY, status=status, listed_rows=0),
        state,
        bucket,
        session,
        uuid.uuid4(),
        "raw/epa",
    )
    assert result.startswith("skipped_")
    assert not session.mock_calls and not bucket.mock_calls
    state.downloaded.assert_not_called()


@pytest.mark.parametrize(
    "bad", [b"<html>upstream error</html>", b"PK\x03\x04truncated"]
)
def test_invalid_zip_never_publishes(bad):
    blob, bucket, session, state = setup_transfer(bad)
    with pytest.raises(ValueError):
        download.download_one(
            ENTRY, state, bucket, session, uuid.uuid4(), "raw/epa", sleep=lambda _: None
        )
    assert blob.published == 0 and blob.aborted == download.ATTEMPTS
    state.downloaded.assert_not_called()
    state.failed.assert_called_once()


def test_interrupted_stream_aborts_and_retries_with_backoff(archive):
    blob, bucket, session, state = setup_transfer(archive)
    broken = response(archive)

    def chunks(**kwargs):
        yield archive[:40]
        raise requests.exceptions.ChunkedEncodingError("connection interrupted")

    broken.iter_content.side_effect = chunks
    session.get.side_effect = [broken, response(archive)]
    pauses = []
    assert (
        download.download_one(
            ENTRY, state, bucket, session, uuid.uuid4(), "raw/epa", sleep=pauses.append
        )
        == "downloaded"
    )
    assert pauses == [1] and blob.aborted == 1 and blob.published == 1


def test_content_length_mismatch_aborts(archive):
    blob, bucket, session, _ = setup_transfer(archive)
    session.get.side_effect = [response(archive, length=len(archive) + 5)]
    with pytest.raises(ValueError, match="Incomplete source"):
        download.stream_zip(
            session,
            bucket,
            ENTRY,
            blob.name,
            None,
            download.source_headers(response(archive)),
            uuid.uuid4(),
        )
    assert blob.aborted == 1 and blob.published == 0


def test_missing_remote_file_is_a_failure_not_catalog_empty(archive):
    _, bucket, session, state = setup_transfer(archive)
    missing = response(b"")
    missing.status_code = 404
    missing.raise_for_status.side_effect = requests.HTTPError(response=missing)
    session.head.side_effect = [missing]
    with pytest.raises(requests.HTTPError):
        download.download_one(ENTRY, state, bucket, session, uuid.uuid4(), "raw/epa")
    assert session.head.call_count == 1
    state.failed.assert_called_once()
    state.downloaded.assert_not_called()


def test_changed_validator_or_missing_integrity_metadata_forces_refresh(archive):
    blob, bucket, session, state = setup_transfer(archive)
    download.download_one(ENTRY, state, bucket, session, uuid.uuid4(), "raw/epa")
    headers = download.source_headers(response(archive))
    assert download.reusable(blob, ENTRY, headers)
    assert not download.reusable(blob, ENTRY, {**headers, "source_etag": '"new"'})
    assert not download.reusable(
        blob, replace(ENTRY, last_modified="2026-07-01"), headers
    )
    blob.metadata.pop("download_md5_hex")
    assert not download.reusable(blob, ENTRY, headers)


def test_manifest_selection_and_bad_filters(tmp_path):
    _, _, groups = load_settings(ROOT / "config/settings.yaml")
    path = ROOT / "output/epa/manifest.csv"
    entries = download.load_manifest(path, groups, {2026}, {"lead", "ozone"})
    assert {entry.status for entry in entries} == {"listed_nonempty", "listed_empty"}
    assert download.parse_years("1990-1992,2026") == {1990, 1991, 1992, 2026}
    with pytest.raises(ValueError, match="absent"):
        download.load_manifest(path, groups, {2026, 2099})
    duplicate = tmp_path / "duplicate.csv"
    lines = path.read_text().splitlines()
    duplicate.write_text("\n".join([lines[0], lines[1], lines[1]]) + "\n")
    with pytest.raises(ValueError, match="Duplicate"):
        download.load_manifest(duplicate, groups)
    assert download.main(["--dry-run", "--pollutants", "unknown"]) == 1


def test_verification_detects_sql_generation_drift_and_untracked_objects(archive):
    blob, bucket, session, state = setup_transfer(archive)
    download.download_one(ENTRY, state, bucket, session, uuid.uuid4(), "raw/epa")
    artifact = state.downloaded.call_args.args[1]
    row = [
        ENTRY.pollutant,
        ENTRY.year,
        ENTRY.status,
        "downloaded",
        artifact.uri,
        artifact.generation,
        artifact.md5,
        artifact.size,
    ]
    version = [
        ENTRY.pollutant,
        ENTRY.year,
        artifact.generation,
        artifact.uri,
        artifact.md5,
        artifact.size,
    ]
    client = Mock()
    client.list_blobs.return_value = [blob]
    conn = Mock()
    conn.cursor.return_value.fetchall.side_effect = [[row], [version]]
    report = download.verify_state([ENTRY], conn, client, bucket.name, "raw/epa")
    assert report["mismatches"] == []
    assert report["matched_nonempty"] == 1 and report["actual_zip_bytes"] == len(
        archive
    )
    row[5] = "wrong-generation"
    untracked = MemoryBlob()
    untracked.name = "raw/epa/2026/untracked.zip"
    client.list_blobs.return_value = [blob, untracked]
    conn.cursor.return_value.fetchall.side_effect = [[row], [version]]
    report = download.verify_state([ENTRY], conn, client, bucket.name, "raw/epa")
    assert len(report["mismatches"]) == 2


@pytest.mark.skipif(
    os.environ.get("AIR_QUALITY_EPA_SQL_TEST") != "1",
    reason="Opt in to a disposable schema on the configured PostgreSQL",
)
def test_real_sql_versions_locks_and_progress():
    schema = "s06_test_" + uuid.uuid4().hex
    with db.connect() as conn:
        state = State(conn)
        try:
            state.execute(f'CREATE SCHEMA "{schema}"')
            state.execute(f'SET search_path TO "{schema}"')
            db.migrate(conn)
            run_id = uuid.uuid4()
            state.start_run(run_id, 2, {})
            with state.lock(ENTRY):
                with db.connect() as second:
                    with pytest.raises(RuntimeError, match="Another worker"):
                        with State(second).lock(ENTRY):
                            pytest.fail("The same file must not have two owners")
                file_id = state.register(ENTRY, run_id)[0]
                artifact = download.Artifact(
                    "gs://test/raw.zip", "100", "a" * 32, 1000, datetime.now(UTC)
                )
                state.downloaded(file_id, artifact, run_id)
                state.downloaded(file_id, artifact, run_id)
                assert (
                    state.execute("SELECT count(*) FROM ingest_file_version", one=True)[
                        0
                    ]
                    == 1
                )
                state.execute(
                    "UPDATE ingest_file SET stage_state='staged', staged_rows=10, "
                    "process_state='processed', processed_rows=9"
                )
                state.register(ENTRY, run_id)
                state.downloaded(file_id, artifact, run_id)
                assert state.execute(
                    "SELECT stage_state, process_state FROM ingest_file", one=True
                ) == ["staged", "processed"]
                state.downloaded(file_id, replace(artifact, generation="101"), run_id)
                assert state.execute(
                    "SELECT stage_state, staged_rows, process_state, processed_rows "
                    "FROM ingest_file",
                    one=True,
                ) == ["pending", None, "pending", None]
                assert (
                    state.execute("SELECT count(*) FROM ingest_file_version", one=True)[
                        0
                    ]
                    == 2
                )
                state.failed(file_id, run_id)
                state.downloaded(file_id, replace(artifact, generation="101"), run_id)
                state.register(
                    replace(ENTRY, status="listed_empty", listed_rows=0), run_id
                )
                assert (
                    state.execute("SELECT download_state FROM ingest_file", one=True)[0]
                    == "skipped"
                )
                state.finish_run(run_id, {"downloaded": 1}, [])
        finally:
            conn.rollback()
            with closing(conn.cursor()) as cursor:
                cursor.execute("SET search_path TO public")
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()
