"""Acceptance gates use expected scheduled slots, not only observed good rows."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from analysis.collection_gate import evaluate, load_logs


@pytest.fixture
def campaign(tmp_path):
    strategy = yaml.safe_load(Path("config/collection_strategy.yaml").read_text())
    strategy["purpleair"]["sensor_ids"] = [1]
    start = datetime(2026, 11, 18, tzinfo=UTC)
    records = []
    for i in range(432):
        requested = start + timedelta(minutes=5 + i * 10)
        path = tmp_path / f"poll-{i}.json"
        path.write_text(
            json.dumps(
                {
                    "time_stamp": requested.timestamp(),
                    "fields": [
                        "sensor_index",
                        "last_seen",
                        "location_type",
                        "humidity",
                        "pm2.5_cf_1_a",
                        "pm2.5_cf_1_b",
                    ],
                    "data": [[1, requested.timestamp() - 60, 0, 50, 10, 11]],
                }
            )
        )
        records.append(
            {
                "event": "purpleair_poll",
                "poll_id": str(i),
                "requested_at_utc": requested.isoformat(),
                "outcome": "success",
                "raw_uri": str(path),
                "details": {"sensor_ids": [1], "run_kind": "scheduled"},
            }
        )
    return strategy, records, tmp_path, start


def run(campaign, records=None, hours=72, as_of=None):
    strategy, original, raw, start = campaign
    return evaluate(
        strategy,
        original if records is None else records,
        raw,
        start,
        hours,
        as_of or start + timedelta(hours=72, minutes=5),
    )


def test_complete_window_uses_expected_denominators_and_dedupes_logs(campaign):
    report = run(campaign, campaign[1] * 2)
    assert report["status"] == "pass"
    assert report["expected_calls"] == report["successful_call_slots"] == 432
    assert report["minimum_successful_calls"] == 411
    assert report["minimum_accepted_hours_per_sensor"] == 65
    assert report["sensors"][0]["accepted_hours"] == 72


def test_exploratory_coverage_is_reported_separately(campaign):
    campaign[0]["study_scope"]["exploratory_sensor_ids"] = [1]
    report = run(campaign)
    assert report["sensors"][0]["distance_stratum"] == "exploratory_5to10km"
    assert report["distance_strata"]["primary_le5km"]["accepted_sensor_hours"] == 0
    assert (
        report["distance_strata"]["exploratory_5to10km"]["accepted_sensor_hours"] == 72
    )


def test_95_percent_call_boundary(campaign):
    assert run(campaign, campaign[1][21:])["status"] == "pass"
    assert run(campaign, campaign[1][22:])["status"] == "review_required"


def test_90_percent_hour_boundary(campaign):
    records = campaign[1]
    seven_bad_hours = [
        r for i, r in enumerate(records) if not (i // 6 < 7 and i % 6 < 2)
    ]
    eight_bad_hours = [
        r for i, r in enumerate(records) if not (i // 6 < 8 and i % 6 < 2)
    ]
    assert run(campaign, seven_bad_hours)["status"] == "pass"
    result = run(campaign, eight_bad_hours)
    assert result["sensors"][0]["accepted_hours"] == 64
    assert result["status"] == "review_required"


def test_incomplete_window_never_passes(campaign):
    result = run(campaign, as_of=campaign[3] + timedelta(hours=71))
    assert result["status"] == "pending"


def test_manual_preflight_does_not_fill_missing_scheduled_slot(campaign):
    records = deepcopy(campaign[1])
    for r in records[:22]:
        r["details"]["run_kind"] = "preflight"
    assert run(campaign, records)["successful_call_slots"] == 410


def test_hours_use_report_time_and_distinct_ten_minute_bins(campaign):
    _, records, _, start = campaign
    # Six distinct readings squeezed into three bins cannot form a sampled hour.
    for i in range(6):
        path = Path(records[i]["raw_uri"])
        payload = json.loads(path.read_text())
        timestamp = (start + timedelta(minutes=3 + i * 5)).timestamp()
        payload["data"][0][1] = timestamp
        payload["time_stamp"] = timestamp + 60
        path.write_text(json.dumps(payload))
    report = run(campaign, hours=1)
    assert report["sensors"][0]["accepted_hours"] == 0
    # A valid report in the preceding UTC hour must not count in this window.
    path = Path(records[0]["raw_uri"])
    payload = json.loads(path.read_text())
    payload["data"][0][1] = (start - timedelta(seconds=30)).timestamp()
    payload["time_stamp"] = start.timestamp()
    path.write_text(json.dumps(payload))
    assert run(campaign, hours=1)["sensors"][0]["usable_polls"] == 5


def test_stale_data_and_duplicate_report_times_do_not_create_coverage(campaign):
    _, records, _, _ = campaign
    first = json.loads(Path(records[0]["raw_uri"]).read_text())
    for i in range(1, 6):
        path = Path(records[i]["raw_uri"])
        payload = json.loads(path.read_text())
        payload["data"][0][1] = first["data"][0][1]
        path.write_text(json.dumps(payload))
    sensor = run(campaign, hours=1)["sensors"][0]
    assert sensor["accepted_hours"] == 0 and sensor["usable_polls"] == 1
    assert sensor["health_alert"] and sensor["consecutive_unusable_polls"] == 5


def test_roster_changes_and_missing_raw_evidence_block_acceptance(campaign):
    records = deepcopy(campaign[1])
    records[0]["details"]["sensor_ids"] = [2]
    records[1]["raw_uri"] = "missing.json"
    report = run(campaign, records)
    assert report["status"] == "review_required"
    assert {e["reason"] for e in report["evidence_errors"]} == {
        "roster_changed_or_missing",
        "raw_evidence_missing",
    }


def test_cloud_logging_and_local_jsonl_formats(tmp_path):
    record = {"event": "purpleair_poll", "poll_id": "1"}
    path = tmp_path / "logs.json"
    path.write_text(
        json.dumps([{"jsonPayload": record}, {"textPayload": "old format"}])
    )
    assert load_logs(path) == [record]
    path.write_text(json.dumps(record) + "\n")
    assert load_logs(path) == [record]
