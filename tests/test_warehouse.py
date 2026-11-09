"""Manifest isolation, rollback, and documented SCD2 history boundaries."""

import copy
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from warehouse.manifest import reconcile, validate_commit, verify_snapshot
from warehouse.scd import at_time, event, versions
from warehouse.sql import copy_statements, render

ROOT = Path(__file__).resolve().parents[1]
COMMIT = json.loads(
    (ROOT / "output/spark/aq-s12-2026-20261109221230/commit.json").read_text()
)
BUCKET = "final-team-514205-air-quality"


@pytest.mark.parametrize(
    "problem", ["duplicate", "old_batch", "wrong_year", "wildcard", "bytes", "counts"]
)
def test_manifest_rejects_unsafe_or_inconsistent_scope(problem):
    value = copy.deepcopy(COMMIT)
    if problem == "duplicate":
        value["files"].append(value["files"][0])
    elif problem == "old_batch":
        value["files"][0]["uri"] = value["files"][0]["uri"].replace(
            "20261109221230", "20261109220005"
        )
    elif problem == "wrong_year":
        value["files"][0]["uri"] = value["files"][0]["uri"].replace(
            "year=2026", "year=2025"
        )
    elif problem == "wildcard":
        value["files"][0]["uri"] = value["files"][0]["uri"].replace(
            ".parquet", "*.parquet"
        )
    elif problem == "bytes":
        value["output_bytes"] += 1
    else:
        value["pollutants"][0]["rows"] += 1
    with pytest.raises(ValueError):
        validate_commit(value, BUCKET, "curated/epa_hourly", 2026)


def test_copy_reads_only_the_50_current_files():
    files, expected = validate_commit(COMMIT, BUCKET, "curated/epa_hourly", 2026)
    (statement,) = copy_statements("EPA_CURATED_STAGE", files)
    assert sum(expected.values()) == 6988009
    assert len(files) == 50
    for item in files:
        assert statement.count("'" + item["relative_path"] + "'") == 1
    assert "20261109220005" not in statement
    assert "PATTERN" not in statement and "PURGE = FALSE" in statement
    assert "ON_ERROR = ABORT_STATEMENT" in statement


def test_generation_change_cannot_pass_with_identical_content():
    item = COMMIT["files"][0]
    client = Mock()
    client.bucket.return_value.blob.side_effect = [
        SimpleNamespace(reload=lambda: None, generation=1),
        SimpleNamespace(
            reload=lambda: None,
            generation=int(item["generation"]) + 1,
            size=item["bytes"],
            md5_hash=item["md5"],
        ),
    ]
    with pytest.raises(ValueError, match="changed"):
        verify_snapshot(
            client,
            {"commit_uri": "gs://b/commit", "commit_generation": "1", "files": [item]},
        )
    client.list_blobs.assert_not_called()


def test_reconciliation_catches_missing_and_extra_pollutants():
    for actual in ({"88101": 9}, {"88101": 10, "LEAD": 1}):
        with pytest.raises(ValueError, match="reconciliation"):
            reconcile({"88101": 10}, actual)


def test_scd_moves_method_closure_replay_and_unknown_history():
    attrs = {"latitude": 41.0, "method_code": "736", "status": "active"}

    def make(day, values):
        return event(
            "monitor",
            "17-031-0001-88101-01",
            f"2026-01-{day:02}T00:00:00Z",
            values,
            "gs://source",
            "1",
            "documented change",
        )

    events = [
        make(1, attrs),
        make(2, attrs),
        make(3, {**attrs, "latitude": 42.0}),
        make(4, {**attrs, "latitude": 42.0, "method_code": "738"}),
        make(5, {**attrs, "latitude": 42.0, "method_code": "738", "status": "closed"}),
    ]
    history = versions(events)
    assert len(history) == 4
    assert history == versions(events + events)
    key = events[0]["natural_key"]
    assert at_time(history, "monitor", key, "2025-12-31T23:59:59Z") is None
    assert (
        at_time(history, "monitor", key, "2026-01-02T12:00:00Z")["attributes"][
            "latitude"
        ]
        == 41
    )
    assert (
        at_time(history, "monitor", key, "2026-01-03T00:00:00Z")["attributes"][
            "latitude"
        ]
        == 42
    )
    assert (
        at_time(history, "monitor", key, "2026-01-04T00:00:00Z")["attributes"][
            "method_code"
        ]
        == "738"
    )
    assert history[-1]["is_current"] and history[-1]["attributes"]["status"] == "closed"
    with pytest.raises(ValueError, match="Conflicting"):
        versions(events + [make(1, {**attrs, "latitude": 43.0})])


def test_failed_promotion_rolls_back_without_commit(monkeypatch, tmp_path):
    import warehouse.load as loader

    cur = Mock()
    cur.execute.return_value.fetchall.return_value = []
    conn = Mock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(loader, "connect", lambda config: conn)
    monkeypatch.setattr(loader, "year_lock", lambda *args: nullcontext())
    monkeypatch.setattr(
        loader,
        "prepare",
        lambda *args: ({"commit_sha256": "a"}, {"sha256": "b"}, tmp_path),
    )
    monkeypatch.setattr(loader, "stage_epa", lambda *args: [])
    monkeypatch.setattr(loader, "stage_dimensions", lambda *args: None)

    def fail(*args):
        raise ValueError("row mismatch")

    monkeypatch.setattr(loader, "promote", fail)
    with pytest.raises(ValueError, match="row mismatch"):
        loader.load(None, {"bucket": "b", "prefix": "curated"}, 2026)
    statements = [call.args[0] for call in cur.execute.call_args_list]
    assert "BEGIN" in statements and statements[-1] == "ROLLBACK"
    assert "COMMIT" not in statements


def test_rendered_schema_keeps_python_package_and_sql_contract_separate():
    # The local snowflake/ folder must not shadow the installed connector package.
    assert not (ROOT / "snowflake/__init__.py").exists()
    sql = render(
        ROOT / "snowflake/ddl/002_model.sql",
        {
            "database": "AIR_QUALITY",
            "schema": "ANALYTICS",
            "warehouse": "AQ_XS",
            "bucket": BUCKET,
        },
    )
    assert "pollutant VARCHAR" in sql
    assert "TIMESTAMP_NTZ(6)" in sql
    assert "{{" not in sql
