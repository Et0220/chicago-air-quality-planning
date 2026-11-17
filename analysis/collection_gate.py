"""Evaluate a completed fixed-cohort window from raw files and poll JSON logs.

This is an offline acceptance check, not a scheduler or automatic replacement
service. No API points are spent and no production state is changed.
"""

import argparse
import json
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from ingestion.purpleair_poll import parse_payload, readings_from


def utc(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamps must include a timezone")
    return result.astimezone(UTC)


def load_logs(path):
    text = path.read_text().strip()
    records = (
        json.loads(text)
        if text.startswith("[")
        else [json.loads(line) for line in text.splitlines() if line.strip()]
    )
    return [
        entry.get("jsonPayload", entry)
        for entry in records
        if entry.get("jsonPayload", entry).get("event") == "purpleair_poll"
    ]


def evaluate(strategy, records, raw_dir, start, hours, as_of):
    if hours <= 0 or start != start.replace(minute=0, second=0, microsecond=0):
        raise ValueError("Use a positive duration starting on a full UTC hour")
    pa, qc, replacement = (
        strategy["purpleair"],
        strategy["quality"],
        strategy["replacement"],
    )
    if pa["interval_seconds"] != 600 or pa["cron"] != "5,15,25,35,45,55 * * * *":
        raise ValueError(
            "This gate expects the reviewed ten-minute :05/:15/... schedule"
        )
    sensor_ids = pa["sensor_ids"]
    exploratory = set(strategy["study_scope"]["exploratory_sensor_ids"])
    end = start + timedelta(hours=hours)
    slots = [start + timedelta(minutes=5 + 10 * i) for i in range(hours * 6)]
    available_slots = [s for s in slots if s + timedelta(minutes=10) <= as_of]
    successful, per_sensor_slots = set(), defaultdict(set)
    samples = defaultdict(set)
    errors, seen_polls = [], set()
    files = {}
    for path in raw_dir.rglob("*.json"):
        if path.name in files:
            raise ValueError(f"Ambiguous raw filename: {path.name}")
        files[path.name] = path
    for record in sorted(records, key=lambda r: r["requested_at_utc"]):
        if record.get("details", {}).get("run_kind") != "scheduled":
            continue
        requested = utc(record["requested_at_utc"])
        if not slots[0] <= requested < end or requested > as_of:
            continue
        poll_id = record["poll_id"]
        if poll_id in seen_polls:
            continue
        seen_polls.add(poll_id)
        slot = slots[int((requested - slots[0]).total_seconds()) // 600]
        if record.get("outcome") != "success":
            continue
        if set(record.get("details", {}).get("sensor_ids") or []) != set(sensor_ids):
            errors.append({"poll_id": poll_id, "reason": "roster_changed_or_missing"})
            continue
        path = files.get(Path(record.get("raw_uri") or "").name)
        if path is None:
            errors.append({"poll_id": poll_id, "reason": "raw_evidence_missing"})
            continue
        try:
            readings, unusable = readings_from(
                parse_payload(path.read_bytes()), min(qc["maximum_age_seconds"], 180)
            )
            if unusable or any(r.sensor_id not in sensor_ids for r in readings):
                raise ValueError("unusable_or_unexpected_raw_rows")
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            errors.append({"poll_id": poll_id, "reason": str(exc)})
            continue
        successful.add(slot)
        for reading in readings:
            if reading.quality_flags or not start <= reading.report_ts < end:
                continue
            per_sensor_slots[reading.sensor_id].add(slot)
            hour = reading.report_ts.replace(minute=0, second=0, microsecond=0)
            samples[(reading.sensor_id, hour)].add(reading.report_ts)
    per_sensor = []
    for sensor in sensor_ids:
        valid_hours = []
        for i in range(hours):
            hour = start + timedelta(hours=i)
            reports = sorted(samples[(sensor, hour)])
            bins = {r.minute // 10 for r in reports}
            span = (reports[-1] - reports[0]).total_seconds() / 60 if reports else 0
            if (
                len(reports) >= qc["minimum_distinct_samples_per_hour"]
                and len(bins) >= qc["minimum_distinct_10minute_bins"]
                and span >= qc["minimum_sample_span_minutes"]
            ):
                valid_hours.append(hour.isoformat())
        consecutive = 0
        for slot in reversed(available_slots):
            if slot in per_sensor_slots[sensor]:
                break
            consecutive += 1
        usable = len(per_sensor_slots[sensor])
        passes = usable >= math.ceil(
            len(slots) * replacement["daily_availability_minimum"]
        ) and len(valid_hours) >= math.ceil(
            hours * replacement["daily_valid_hour_fraction_minimum"]
        )
        per_sensor.append(
            {
                "sensor_id": sensor,
                "distance_stratum": (
                    "exploratory_5to10km" if sensor in exploratory else "primary_le5km"
                ),
                "usable_polls": usable,
                "expected_polls": len(slots),
                "usable_poll_fraction": round(usable / len(slots), 5),
                "accepted_hours": len(valid_hours),
                "expected_hours": hours,
                "accepted_hour_fraction": round(len(valid_hours) / hours, 5),
                "consecutive_unusable_polls": consecutive,
                "health_alert": consecutive
                >= replacement["consecutive_unusable_polls_to_alert"],
                "passes": passes,
                "accepted_hour_keys": valid_hours,
            }
        )
    minimum = math.ceil(len(slots) * pa["minimum_call_success_fraction"])
    complete = as_of >= end + timedelta(minutes=5)
    passed = (
        complete
        and not errors
        and len(successful) >= minimum
        and all(s["passes"] for s in per_sensor)
    )
    return {
        "status": "pass" if passed else "review_required" if complete else "pending",
        "window_start_utc": start.isoformat(),
        "window_end_utc": end.isoformat(),
        "evaluated_at_utc": as_of.isoformat(),
        "expected_calls": len(slots),
        "successful_call_slots": len(successful),
        "minimum_successful_calls": minimum,
        "call_success_fraction": round(len(successful) / len(slots), 5),
        "minimum_accepted_hours_per_sensor": math.ceil(
            hours * replacement["daily_valid_hour_fraction_minimum"]
        ),
        "evidence_errors": errors,
        "sensors": per_sensor,
        "distance_strata": {
            name: {
                "sensor_ids": [
                    s["sensor_id"] for s in per_sensor if s["distance_stratum"] == name
                ],
                "accepted_sensor_hours": sum(
                    s["accepted_hours"]
                    for s in per_sensor
                    if s["distance_stratum"] == name
                ),
                "expected_sensor_hours": hours
                * sum(s["distance_stratum"] == name for s in per_sensor),
                "note": (
                    "Coverage only; sensor-hours sharing a reference are not "
                    "independent spatial replicates."
                ),
            }
            for name in ("primary_le5km", "exploratory_5to10km")
        },
        "replacement_policy": (
            "No automatic changes. Use the saved replacement map, recheck the backup, "
            "and record effective time. A changed roster needs separately "
            "evaluated windows."
        ),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strategy", type=Path, default=Path("config/collection_strategy.yaml")
    )
    parser.add_argument("--poll-log", type=Path, required=True)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument(
        "--start-utc", type=utc, required=True, help="first fully sampled UTC hour"
    )
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--as-of-utc", type=utc, default=None)
    parser.add_argument(
        "--out", type=Path, default=Path("output/collection_review/campaign_gate.json")
    )
    args = parser.parse_args(argv)
    report = evaluate(
        yaml.safe_load(args.strategy.read_text()),
        load_logs(args.poll_log),
        args.raw_dir,
        args.start_utc,
        args.hours,
        args.as_of_utc or datetime.now(UTC),
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "sensors"}, indent=2))
    return (
        0 if report["status"] == "pass" else 2 if report["status"] == "pending" else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
