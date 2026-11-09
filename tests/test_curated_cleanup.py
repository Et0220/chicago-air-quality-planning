"""Never retire currently committed, changed, or out-of-scope Parquet."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from spark.curated_cleanup import candidates, cleanup, verify_live

BATCH = "aq-s12-2026-20261109220005"
FILE = {
    "uri": f"gs://b/curated/year=2026/pollutant=14129/{BATCH}-part.parquet",
    "generation": "10",
    "bytes": 123,
    "md5": "checksum",
}


@pytest.mark.parametrize(
    "problem", ["current", "shared", "year", "prefix", "duplicate"]
)
def test_cleanup_rejects_protected_or_out_of_scope_files(problem):
    current = {"batch": "new", "files": []}
    previous = {"batch": BATCH, "files": [FILE]}
    if problem == "current":
        current["batch"] = BATCH
    elif problem == "shared":
        current["files"] = [FILE]
    elif problem in {"year", "prefix"}:
        old, new = ("2026", "2025") if problem == "year" else ("curated", "raw")
        previous["files"] = [{**FILE, "uri": FILE["uri"].replace(old, new)}]
    else:
        previous["files"] = [FILE, FILE]
    with pytest.raises(ValueError):
        candidates(current, previous, "b", "curated", 2026, BATCH)


def test_cleanup_rejects_replaced_generation_even_with_same_content():
    blob = SimpleNamespace(
        reload=lambda: None, generation=11, size=123, md5_hash="checksum"
    )
    with pytest.raises(ValueError, match="differs"):
        verify_live(blob, FILE)


def storage_case(monkeypatch, tmp_path):
    monkeypatch.setattr("spark.curated_cleanup.ROOT", tmp_path)
    current_file = {**FILE, "uri": FILE["uri"].replace(BATCH, "current")}
    second_old = {**FILE, "uri": FILE["uri"].replace("part.parquet", "part2.parquet")}
    current = {"batch": "current", "files": [current_file]}
    previous = {"batch": BATCH, "files": [FILE, second_old]}
    blobs = {}

    def blob(name, generation=None):
        # Deletes must address the live object, not permanently erase a version.
        assert generation is None
        if name not in blobs:
            blobs[name] = Mock(generation=10, size=123, md5_hash="checksum")
            blobs[name].name = name
            blobs[name].exists.return_value = True
        return blobs[name]

    commit = blob("curated/_commits/2026.json")
    commit.download_as_text.return_value = json.dumps(current)
    blob(
        f"curated/_runs/{BATCH}/metrics.json"
    ).download_as_text.return_value = json.dumps(previous)
    bucket = Mock(versioning_enabled=True, blob=blob)
    bucket.get_blob.return_value = commit
    current_blob = blob(current_file["uri"].split("gs://b/")[1])
    client = Mock()
    client.bucket.return_value = bucket
    client.list_blobs.return_value = [current_blob]
    args = SimpleNamespace(
        bucket="b", prefix="curated", year=2026, batch=BATCH, apply=False
    )
    return args, client, blob, second_old, current_blob


def test_cleanup_dry_run_then_apply_only_superseded_generations(monkeypatch, tmp_path):
    args, client, blob, second, current = storage_case(monkeypatch, tmp_path)
    assert cleanup(args, client)["status"] == "planned"
    old = [blob(f["uri"].split("gs://b/")[1]) for f in (FILE, second)]
    for item in [*old, current]:
        item.delete.assert_not_called()
    blob("curated/_locks/2026.json").upload_from_string.assert_not_called()
    args.apply = True
    assert cleanup(args, client)["status"] == "retired"
    for item in old:
        item.delete.assert_called_once_with(if_generation_match=10)
    current.delete.assert_not_called()
    blob("curated/_locks/2026.json").delete.assert_called_once_with(
        if_generation_match=10
    )


def test_cleanup_preflights_all_files_before_deleting_any(monkeypatch, tmp_path):
    args, client, blob, second, current = storage_case(monkeypatch, tmp_path)
    args.apply = True
    blob(second["uri"].split("gs://b/")[1]).generation = 11
    with pytest.raises(ValueError, match="differs"):
        cleanup(args, client)
    blob(FILE["uri"].split("gs://b/")[1]).delete.assert_not_called()
    current.delete.assert_not_called()
    blob("curated/_locks/2026.json").delete.assert_called_once_with(
        if_generation_match=10
    )
