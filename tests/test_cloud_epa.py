"""Guard full-year coverage and immutable source validation before spending."""

import base64
from types import SimpleNamespace

import pytest

from spark.cloud_epa import check_stage, gcloud, release_failed, selected_sources


def test_scope_requires_full_reconciled_year():
    row = {"year": 2026, "pollutant": "ozone", "listed_rows": 10, "staged_rows": 10}
    catalog = [
        {
            "year": "2026",
            "pollutant": "ozone",
            "listed_rows": "10",
            "status": "listed_nonempty",
        },
        {
            "year": "2026",
            "pollutant": "lead",
            "listed_rows": "0",
            "status": "listed_empty",
        },
    ]
    assert selected_sources({"objects": [row]}, catalog, 2026) == [row]
    for rows in [[], [row, row], [{**row, "staged_rows": 9}]]:
        with pytest.raises(ValueError):
            selected_sources({"objects": rows}, catalog, 2026)


def test_stage_generation_checksum_and_raw_lineage():
    blob = SimpleNamespace(
        reload=lambda: None,
        generation=10,
        size=123,
        md5_hash=base64.b64encode(b"0123456789abcdef").decode(),
        metadata={
            "csv_rows": "5",
            "source_generation": "8",
            "source_uri": "gs://b/raw",
        },
    )
    entry = {
        "stage_generation": "10",
        "csv_bytes": 123,
        "csv_md5": b"0123456789abcdef".hex(),
        "staged_rows": 5,
        "raw_generation": "8",
        "raw_uri": "gs://b/raw",
        "stage_uri": "gs://b/stage",
    }
    check_stage(blob, entry)
    for key, value in [
        ("stage_generation", "11"),
        ("csv_bytes", 124),
        ("staged_rows", 6),
        ("raw_generation", "9"),
    ]:
        with pytest.raises(ValueError):
            check_stage(blob, {**entry, key: value})


def test_gcloud_flags_do_not_leak_into_spark_arguments(monkeypatch):
    def run(command, **kwargs):
        assert command == ["gcloud", "--format=json", "dataproc", "--", "--manifest=a"]
        assert kwargs["check"]
        return SimpleNamespace(stdout='{"state":"PENDING"}')

    monkeypatch.setattr("spark.cloud_epa.subprocess.run", run)
    assert gcloud("dataproc", "--", "--manifest=a")["state"] == "PENDING"


def test_release_refuses_active_batch_before_touching_lock(monkeypatch, tmp_path):
    import json

    folder = tmp_path / "output/spark/aq-s12-test"
    folder.mkdir(parents=True)
    (folder / "submission.json").write_text(json.dumps({"project": "p", "region": "r"}))
    monkeypatch.setattr("spark.cloud_epa.ROOT", tmp_path)
    monkeypatch.setattr("spark.cloud_epa.gcloud", lambda *args: {"state": "RUNNING"})
    # No client is needed: touching storage for a live batch would fail here.
    with pytest.raises(ValueError, match="Only FAILED/CANCELLED"):
        release_failed(SimpleNamespace(batch="aq-s12-test"), None)
