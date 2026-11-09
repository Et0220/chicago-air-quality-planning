"""Offline tests for S10 ZIP validation and staging lineage."""

import base64
import csv
import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion.epa_stage import StageEntry, extract_csv, reusable, stage_name, verify

HEADER = [
    "State Code",
    "County Code",
    "Site Num",
    "Parameter Code",
    "POC",
    "Date GMT",
    "Time GMT",
    "Sample Measurement",
    "Units of Measure",
]


def _entry(**changes):
    values = dict(
        ingest_file_id=1,
        pollutant="pm25_frm_fem",
        year=2024,
        file_name="hourly_88101_2024.zip",
        listed_rows=2,
        raw_uri="gs://bucket/raw/epa/2024/hourly_88101_2024.zip",
        raw_generation="1234",
        raw_md5="0" * 32,
        raw_size_bytes=100,
        stage_state="staged",
        staged_rows=2,
    )
    values.update(changes)
    return StageEntry(**values)


def _zip(path: Path, name: str, content: bytes):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, content)


def test_extract_counts_csv_records_and_preserves_bytes(tmp_path):
    text = io.StringIO(newline="")
    writer = csv.writer(text)
    writer.writerow(HEADER)
    writer.writerow(
        ["17", "031", "0001", "88101", "1", "2024-01-01", "00:00", "1", "ug/m3"]
    )
    writer.writerow(
        ["17", "031", "0001", "88101", "1", "2024-01-01", "01:00", "2", "quoted\nunit"]
    )
    content = text.getvalue().encode()
    path = tmp_path / "hourly_88101_2024.zip"
    _zip(path, "hourly_88101_2024.csv", content)
    output = io.BytesIO()
    rows, size, _, header = extract_csv(path, "hourly_88101_2024.csv", output)
    assert rows == 2  # A quoted newline does not add a record.
    assert size == len(content)
    assert output.getvalue() == content
    assert header == HEADER


def test_extract_rejects_unexpected_member(tmp_path):
    path = tmp_path / "hourly_88101_2024.zip"
    _zip(path, "different.csv", b"bad")
    with pytest.raises(ValueError, match="Unexpected ZIP members"):
        extract_csv(path, "hourly_88101_2024.csv", io.BytesIO())


def test_stage_name_and_reuse_require_matching_source_generation():
    entry = _entry()
    name = stage_name(entry, "staging/epa")
    assert name == "staging/epa/88101/2024/hourly_88101_2024__g1234.csv"
    blob = SimpleNamespace(
        name=name,
        size=200,
        generation=5678,
        md5_hash=base64.b64encode(bytes.fromhex("a" * 32)).decode(),
        metadata={
            "source_uri": entry.raw_uri,
            "source_generation": entry.raw_generation,
            "csv_rows": "2",
            "csv_md5_hex": "a" * 32,
        },
    )
    assert reusable(blob, entry)
    assert not reusable(blob, _entry(raw_generation="9999"))


def test_verify_lists_catalog_row_mismatch():
    entry = _entry(listed_rows=3)
    name = stage_name(entry, "staging/epa")
    blob = SimpleNamespace(
        name=name,
        size=200,
        generation=5678,
        md5_hash=base64.b64encode(bytes.fromhex("a" * 32)).decode(),
        metadata={
            "source_uri": entry.raw_uri,
            "source_generation": entry.raw_generation,
            "csv_rows": "2",
            "csv_md5_hex": "a" * 32,
        },
    )
    client = SimpleNamespace(list_blobs=lambda *a, **k: [blob])
    report = verify([entry], client, "bucket", "staging/epa")
    assert report["matched"] == 1
    assert len(report["mismatches"]) == 1
    assert "3 vs 2" in report["mismatches"][0]
